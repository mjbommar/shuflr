//! Record-level sampling transforms that wrap any `Read` and re-expose a
//! `Read` with filtered contents. Two modes:
//!
//! - **Head limit** (`limit = Some(N)`): pass records through until `N`
//!   have been emitted, then report EOF.
//! - **Bernoulli filter** (`sample_rate = Some(p)`): each input record is
//!   kept with probability `p`, independently, using a seeded ChaCha20Rng.
//!
//! The two combine: `sample_rate=p, limit=N` → Bernoulli sampling with an
//! early termination after `N` accepted records, which is what the user
//! wants for "1% of records, capped at 1M" style workflows on a huge
//! compressed corpus.
//!
//! Reservoir sampling (exactly K uniform-random) isn't in this module
//! because it needs to hold K records in memory and emit them only after
//! the full scan — use [`crate::pipeline::reservoir`] for that.
//!
//! Records are identified by `\n` separators (JSONL framing, 002 §2).
//! A final partial record without a trailing `\n` is passed through as-is.

use std::io::{self, BufRead, BufReader, Read};

use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha20Rng;

/// Wraps a reader and filters its records on the fly.
pub struct SamplingReader<R: Read> {
    inner: BufReader<R>,
    rng: Option<ChaCha20Rng>,
    sample_rate: Option<f64>,
    limit: Option<u64>,
    kept: u64,
    /// Current accepted record's bytes, awaiting emit.
    line_buf: Vec<u8>,
    /// Position in `line_buf` of the next byte to emit.
    emit_cursor: usize,
    /// Once true, subsequent reads report EOF.
    done: bool,
}

impl<R: Read> SamplingReader<R> {
    pub fn new(inner: R, sample_rate: Option<f64>, limit: Option<u64>, seed: u64) -> Self {
        let rng = sample_rate.map(|_| ChaCha20Rng::seed_from_u64(seed));
        Self {
            inner: BufReader::with_capacity(2 * 1024 * 1024, inner),
            rng,
            sample_rate,
            limit,
            kept: 0,
            line_buf: Vec::with_capacity(8 * 1024),
            emit_cursor: 0,
            done: false,
        }
    }

    /// Number of records accepted so far.
    pub fn records_kept(&self) -> u64 {
        self.kept
    }
}

impl<R: Read> Read for SamplingReader<R> {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        loop {
            // 1. Serve from the accepted-record buffer.
            if self.emit_cursor < self.line_buf.len() {
                let remaining = self.line_buf.len() - self.emit_cursor;
                let n = remaining.min(out.len());
                out[..n].copy_from_slice(&self.line_buf[self.emit_cursor..self.emit_cursor + n]);
                self.emit_cursor += n;
                return Ok(n);
            }

            if self.done {
                return Ok(0);
            }

            // 2. Pull the next record from the underlying reader.
            self.line_buf.clear();
            self.emit_cursor = 0;
            let n = self.inner.read_until(b'\n', &mut self.line_buf)?;
            if n == 0 {
                self.done = true;
                return Ok(0);
            }

            // 3. Bernoulli filter.
            let keep = match (self.sample_rate, &mut self.rng) {
                (Some(rate), Some(rng)) => rng.gen_bool(rate.clamp(0.0, 1.0)),
                _ => true,
            };
            if !keep {
                // Critical: wipe the buffer so the next iter doesn't serve
                // these bytes as if they were an accepted record.
                self.line_buf.clear();
                self.emit_cursor = 0;
                continue;
            }

            // 4. Count toward limit (applied after Bernoulli so "--limit N
            //    --sample-rate 0.01" means "stop after N kept records").
            self.kept += 1;
            if let Some(cap) = self.limit
                && self.kept > cap
            {
                // We counted one too many; drop this record and report EOF.
                self.line_buf.clear();
                self.done = true;
                self.kept -= 1;
                return Ok(0);
            }
            // 5. Loop: the next iteration serves from the now-populated line_buf.
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    fn read_all<R: Read>(mut r: R) -> Vec<u8> {
        let mut buf = Vec::new();
        r.read_to_end(&mut buf).unwrap();
        buf
    }

    #[test]
    fn passthrough_when_no_filters() {
        let input = b"a\nb\nc\nd\ne\n";
        let sr = SamplingReader::new(&input[..], None, None, 0);
        let out = read_all(sr);
        assert_eq!(out, input);
    }

    #[test]
    fn head_limit_caps_records() {
        let input = b"one\ntwo\nthree\nfour\nfive\n";
        let sr = SamplingReader::new(&input[..], None, Some(3), 0);
        let out = read_all(sr);
        assert_eq!(out, b"one\ntwo\nthree\n");
    }

    #[test]
    fn head_limit_stops_at_exact_count() {
        let input = b"one\ntwo\n";
        let sr = SamplingReader::new(&input[..], None, Some(10), 0);
        let out = read_all(sr);
        assert_eq!(out, b"one\ntwo\n");
    }

    #[test]
    fn bernoulli_rate_0_keeps_nothing() {
        let input = b"a\nb\nc\nd\ne\n";
        let sr = SamplingReader::new(&input[..], Some(0.0), None, 0);
        let out = read_all(sr);
        assert!(out.is_empty());
    }

    #[test]
    fn bernoulli_rate_1_keeps_everything() {
        let input = b"a\nb\nc\nd\ne\n";
        let sr = SamplingReader::new(&input[..], Some(1.0), None, 0);
        let out = read_all(sr);
        assert_eq!(out, input);
    }

    #[test]
    fn bernoulli_deterministic_with_same_seed() {
        let input: Vec<u8> = (0..500)
            .map(|i| format!("r{i:04}\n"))
            .collect::<String>()
            .into_bytes();
        let a = read_all(SamplingReader::new(&input[..], Some(0.3), None, 42));
        let b = read_all(SamplingReader::new(&input[..], Some(0.3), None, 42));
        let c = read_all(SamplingReader::new(&input[..], Some(0.3), None, 43));
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn bernoulli_output_is_subset_of_input() {
        let input: Vec<u8> = (0..200)
            .map(|i| format!("r{i:03}\n"))
            .collect::<String>()
            .into_bytes();
        let out = read_all(SamplingReader::new(&input[..], Some(0.2), None, 1));
        let in_set: HashSet<&[u8]> = input.split_inclusive(|b| *b == b'\n').collect();
        for rec in out.split_inclusive(|b| *b == b'\n') {
            assert!(in_set.contains(rec));
        }
    }

    #[test]
    fn bernoulli_rate_roughly_matches_over_many_samples() {
        // Expect ~30% inclusion over a 10k-record input at rate 0.3.
        let input: Vec<u8> = (0..10_000)
            .map(|i| format!("r{i:05}\n"))
            .collect::<String>()
            .into_bytes();
        let out = read_all(SamplingReader::new(&input[..], Some(0.3), None, 2));
        let kept = out.split_inclusive(|b| *b == b'\n').count();
        // Binomial(10000, 0.3) has stddev ~46; 3σ ≈ 140. Use a generous band
        // so the test isn't flaky across seeds.
        assert!(
            (2700..3300).contains(&kept),
            "expected ~3000 kept, got {kept}"
        );
    }

    #[test]
    fn bernoulli_with_limit_stops_early() {
        // Rate 1.0 + limit 5 = first 5 records.
        let input = b"a\nb\nc\nd\ne\nf\ng\nh\n";
        let sr = SamplingReader::new(&input[..], Some(1.0), Some(5), 0);
        let out = read_all(sr);
        assert_eq!(out, b"a\nb\nc\nd\ne\n");
    }

    #[test]
    fn bernoulli_rate_half_with_limit_hits_limit() {
        // Rate 0.5 on 100 records with limit 10: output should be <=10 records
        // and, with high probability, == 10 (since 50 accepted on average >> 10).
        let input: Vec<u8> = (0..100)
            .map(|i| format!("r{i:03}\n"))
            .collect::<String>()
            .into_bytes();
        let sr = SamplingReader::new(&input[..], Some(0.5), Some(10), 1);
        let out = read_all(sr);
        assert_eq!(out.split_inclusive(|b| *b == b'\n').count(), 10);
    }

    #[test]
    fn passes_trailing_partial_line_through() {
        let input = b"one\ntwo"; // no trailing newline
        let sr = SamplingReader::new(&input[..], None, None, 0);
        let out = read_all(sr);
        assert_eq!(out, input);
    }
}
