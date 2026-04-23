//! Record-level sampling transforms that wrap any `Read` and re-expose a
//! `Read` with filtered contents. Three orthogonal modes, composable:
//!
//! - **Head limit** (`limit = Some(N)`): pass records through until `N`
//!   have been emitted, then report EOF.
//! - **Bernoulli filter** (`sample_rate = Some(p)`): each input record is
//!   kept with probability `p`, independently, using a seeded ChaCha20Rng.
//! - **Entropy gate** (`min_entropy_nats` / `max_entropy_nats`): drop
//!   records whose byte-distribution Shannon entropy falls outside the
//!   given range. Useful for kicking out boilerplate (entropy below
//!   ~2 bits), random/binary blobs (>7 bits), and anything else the
//!   user has an empirical threshold for.
//!
//! All three combine: `sample_rate=p, limit=N, min_entropy_nats=M` →
//! Bernoulli sampling that only accepts records passing the entropy
//! gate, stopping early after `N` survivors.
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
    min_entropy_nats: Option<f64>,
    max_entropy_nats: Option<f64>,
    kept: u64,
    entropy_dropped_low: u64,
    entropy_dropped_high: u64,
    /// Current accepted record's bytes, awaiting emit.
    line_buf: Vec<u8>,
    /// Position in `line_buf` of the next byte to emit.
    emit_cursor: usize,
    /// Once true, subsequent reads report EOF.
    done: bool,
}

/// Configuration bundle for [`SamplingReader::with_config`]. Kept as a
/// separate struct so adding a new gate doesn't bloat the constructor
/// signature.
#[derive(Default, Debug, Clone, Copy)]
pub struct SamplingConfig {
    pub sample_rate: Option<f64>,
    pub limit: Option<u64>,
    pub seed: u64,
    pub min_entropy_nats: Option<f64>,
    pub max_entropy_nats: Option<f64>,
}

impl<R: Read> SamplingReader<R> {
    /// Constructor for the two-field (sample_rate + limit) shape that
    /// PR-14 introduced. Kept as the backwards-compatible API; new call
    /// sites should prefer [`with_config`] which accepts entropy gates.
    pub fn new(inner: R, sample_rate: Option<f64>, limit: Option<u64>, seed: u64) -> Self {
        Self::with_config(
            inner,
            SamplingConfig {
                sample_rate,
                limit,
                seed,
                ..Default::default()
            },
        )
    }

    pub fn with_config(inner: R, cfg: SamplingConfig) -> Self {
        let rng = cfg
            .sample_rate
            .map(|_| ChaCha20Rng::seed_from_u64(cfg.seed));
        Self {
            inner: BufReader::with_capacity(2 * 1024 * 1024, inner),
            rng,
            sample_rate: cfg.sample_rate,
            limit: cfg.limit,
            min_entropy_nats: cfg.min_entropy_nats,
            max_entropy_nats: cfg.max_entropy_nats,
            kept: 0,
            entropy_dropped_low: 0,
            entropy_dropped_high: 0,
            line_buf: Vec::with_capacity(8 * 1024),
            emit_cursor: 0,
            done: false,
        }
    }

    /// Number of records accepted so far.
    pub fn records_kept(&self) -> u64 {
        self.kept
    }

    pub fn entropy_dropped_low(&self) -> u64 {
        self.entropy_dropped_low
    }

    pub fn entropy_dropped_high(&self) -> u64 {
        self.entropy_dropped_high
    }
}

/// Shannon entropy of a byte sequence, in nats. `H(p) = -Σ p_i · ln(p_i)`
/// over a 256-bin byte histogram. Returns 0 for empty input.
pub fn shannon_entropy_nats(bytes: &[u8]) -> f64 {
    if bytes.is_empty() {
        return 0.0;
    }
    let mut hist = [0u32; 256];
    for &b in bytes {
        hist[b as usize] += 1;
    }
    let n = bytes.len() as f64;
    let mut h = 0.0;
    for &count in &hist {
        if count > 0 {
            let p = count as f64 / n;
            h -= p * p.ln();
        }
    }
    h
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

            // 3b. Entropy gate. Computed only if a bound is set (Shannon
            //     entropy over byte histogram is ~200 MB/s per core, not
            //     free).
            if self.min_entropy_nats.is_some() || self.max_entropy_nats.is_some() {
                let h = shannon_entropy_nats(&self.line_buf);
                if let Some(min) = self.min_entropy_nats
                    && h < min
                {
                    self.entropy_dropped_low += 1;
                    self.line_buf.clear();
                    self.emit_cursor = 0;
                    continue;
                }
                if let Some(max) = self.max_entropy_nats
                    && h > max
                {
                    self.entropy_dropped_high += 1;
                    self.line_buf.clear();
                    self.emit_cursor = 0;
                    continue;
                }
            }

            // 4. Count toward limit (applied after all drops so "--limit N
            //    --sample-rate 0.01 --min-entropy 3" means "stop after N
            //    kept records that passed every gate").
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

    #[test]
    fn shannon_entropy_bounds() {
        // Uniform random over one symbol: entropy = 0.
        assert_eq!(shannon_entropy_nats(&[b'x'; 100]), 0.0);
        // All-256 bytes each appearing once: entropy = ln(256).
        let uniform: Vec<u8> = (0u8..=255u8).collect();
        let h = shannon_entropy_nats(&uniform);
        assert!((h - 256f64.ln()).abs() < 1e-9);
        // Empty input: 0.
        assert_eq!(shannon_entropy_nats(&[]), 0.0);
    }

    #[test]
    fn min_entropy_drops_low_entropy_records() {
        // Three records: all-x (H=0), plain text (H~medium), all-zeros (H=0).
        let input: &[u8] = b"xxxxxxxx\nhello world, this is text\n00000000\n";
        let sr = SamplingReader::with_config(
            input,
            SamplingConfig {
                min_entropy_nats: Some(1.0),
                ..Default::default()
            },
        );
        let (out, dropped_low, dropped_high) = {
            let mut buf = Vec::new();
            let mut reader = SamplingReader::with_config(
                input,
                SamplingConfig {
                    min_entropy_nats: Some(1.0),
                    ..Default::default()
                },
            );
            reader.read_to_end(&mut buf).unwrap();
            (
                buf,
                reader.entropy_dropped_low(),
                reader.entropy_dropped_high(),
            )
        };
        let _ = sr;
        assert_eq!(out, b"hello world, this is text\n");
        assert_eq!(dropped_low, 2);
        assert_eq!(dropped_high, 0);
    }

    #[test]
    fn max_entropy_drops_high_entropy_records() {
        // Binary-noise record = 255 distinct byte values (skipping the
        // newline byte 0x0A so read_until doesn't split it) followed by \n.
        // Entropy ≈ ln 255 ≈ 5.54 nats. Plain text ~3 nats. max=4 drops
        // the noisy one.
        let mut bin = Vec::new();
        for b in 0u8..=255u8 {
            if b != b'\n' {
                bin.push(b);
            }
        }
        bin.push(b'\n');
        let mut input = Vec::from(&b"hello world, some english text\n"[..]);
        input.extend_from_slice(&bin);

        let mut reader = SamplingReader::with_config(
            &input[..],
            SamplingConfig {
                max_entropy_nats: Some(4.0),
                ..Default::default()
            },
        );
        let mut out = Vec::new();
        reader.read_to_end(&mut out).unwrap();
        assert_eq!(out, b"hello world, some english text\n");
        assert_eq!(reader.entropy_dropped_high(), 1);
        assert_eq!(reader.entropy_dropped_low(), 0);
    }

    #[test]
    fn entropy_combines_with_bernoulli_and_limit() {
        // Entropy filter runs before the limit gate. With rate=1.0,
        // min_entropy=1, limit=2 on `xxxx / hello / world / xxxx / more`
        // we see: xxxx drop, hello keep (1), world keep (2), xxxx drop,
        // then "more" triggers kept>cap and reports EOF. So output is
        // "hello\nworld\n" and both xxxx records are counted as
        // dropped_low.
        let input: &[u8] = b"xxxx\nhello\nworld\nxxxx\nmore words here\n";
        let mut reader = SamplingReader::with_config(
            input,
            SamplingConfig {
                sample_rate: Some(1.0),
                limit: Some(2),
                min_entropy_nats: Some(1.0),
                seed: 0,
                ..Default::default()
            },
        );
        let mut out = Vec::new();
        reader.read_to_end(&mut out).unwrap();
        assert_eq!(out, b"hello\nworld\n");
        assert_eq!(reader.entropy_dropped_low(), 2);
    }
}
