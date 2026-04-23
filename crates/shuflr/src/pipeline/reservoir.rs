//! `--shuffle=reservoir` — Vitter's Algorithm R (002 §2.4).
//!
//! Emits **exactly K records**, a uniform random K-subset of the input.
//! Different from [`crate::pipeline::buffer`] (which emits all N records
//! in an approximately-shuffled order); reservoir is a *sampler*, not a
//! shuffler.
//!
//! Algorithm (Vitter 1985, Algorithm R):
//!
//! ```text
//!   reservoir := first K records
//!   for i = K, K+1, ..., N-1:
//!       j := uniform_int(0, i)          # inclusive
//!       if j < K: reservoir[j] := input[i]
//!   shuffle reservoir in-place           # so output order is random too
//!   emit reservoir
//! ```
//!
//! After the final pass, every input record is in the reservoir with
//! probability exactly K/N. We additionally Fisher-Yates the reservoir
//! before emission so consumers don't see it in the "K first records
//! + replacements" shape.
//!
//! Memory: `O(K × avg_record_size)` plus a small RNG. Deterministic
//! given `--seed`.

use std::io::{BufRead, BufWriter, Write};

use rand::{Rng, RngCore, SeedableRng};
use rand_chacha::ChaCha20Rng;

use crate::error::{Error, Result};
use crate::framing::{OnError, Stats};
use crate::io::Input;

#[derive(Debug, Clone)]
pub struct Config {
    /// Number of output records to draw. If the input has fewer than `k`
    /// records, the output size is `N` (all of them).
    pub k: usize,
    pub seed: u64,
    pub max_line: u64,
    pub on_error: OnError,
    pub ensure_trailing_newline: bool,
    /// Distributed partition: see `passthrough::Config::partition`.
    /// For reservoir specifically: each rank applies the same partition
    /// filter before the reservoir sees a record, so ranks sample
    /// disjoint K-subsets of their partitions.
    pub partition: Option<(u32, u32)>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            k: 10_000,
            seed: 0,
            max_line: 16 * 1024 * 1024,
            on_error: OnError::Skip,
            ensure_trailing_newline: true,
            partition: None,
        }
    }
}

pub fn run(mut input: Input, sink: impl Write, cfg: &Config) -> Result<Stats> {
    assert!(cfg.k >= 1, "reservoir size must be >= 1");
    let mut writer = BufWriter::with_capacity(2 * 1024 * 1024, sink);
    let mut stats = Stats::default();
    let mut rng = ChaCha20Rng::seed_from_u64(cfg.seed);

    // Reservoir: up to K complete records (each with its trailing '\n').
    let mut reservoir: Vec<Vec<u8>> = Vec::with_capacity(cfg.k);
    let mut line: Vec<u8> = Vec::with_capacity(8 * 1024);
    let mut i: u64 = 0; // per-record counter for the sampling decision

    loop {
        line.clear();
        let n = input.read_until(b'\n', &mut line).map_err(Error::Io)?;
        if n == 0 {
            break;
        }
        stats.records_in += 1;
        stats.bytes_in += n as u64;
        let has_newline = line.last() == Some(&b'\n');
        if !has_newline {
            stats.had_trailing_partial = true;
        }

        // --max-line policy.
        if (n as u64) > cfg.max_line {
            let keep = stats.apply_oversize_policy(cfg.on_error, 0, n as u64, cfg.max_line)?;
            if !keep {
                continue;
            }
        }

        // Distributed partition: filter before the reservoir sees the record.
        if let Some((rank, world_size)) = cfg.partition
            && world_size > 1
            && ((stats.records_in - 1) as u32) % world_size != rank
        {
            continue;
        }

        // Patch trailing newline so every reservoir entry is self-delimited.
        let mut rec = line.clone();
        if !has_newline && cfg.ensure_trailing_newline {
            rec.push(b'\n');
        }

        if reservoir.len() < cfg.k {
            reservoir.push(rec);
        } else {
            // Sample a random index in [0, i] inclusive (Vitter R).
            let j = rng.gen_range(0..=i);
            if (j as usize) < cfg.k {
                reservoir[j as usize] = rec;
            }
        }
        i += 1;
    }

    // Fisher-Yates the reservoir before emit, so the output order is random
    // rather than the "K first + replacements" shape left by the sampling pass.
    let len = reservoir.len();
    for pos in (1..len).rev() {
        let swap_with = (rng.next_u64() as usize) % (pos + 1);
        reservoir.swap(pos, swap_with);
    }

    for rec in &reservoir {
        writer.write_all(rec).map_err(Error::Io)?;
        stats.bytes_out += rec.len() as u64;
        stats.records_out += 1;
    }

    writer.flush().map_err(Error::Io)?;
    Ok(stats)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn run_bytes(bytes: &'static [u8], cfg: &Config) -> (Vec<u8>, Stats) {
        let inp = Input::from_reader(Box::new(bytes), None, None).unwrap();
        let mut out = Vec::new();
        let stats = run(inp, &mut out, cfg).unwrap();
        (out, stats)
    }

    #[test]
    fn output_size_exactly_k_for_input_larger_than_k() {
        let input: &[u8] = b"a\nb\nc\nd\ne\nf\ng\nh\ni\nj\n";
        let cfg = Config {
            k: 4,
            seed: 1,
            ..Config::default()
        };
        let (out, stats) = run_bytes(input, &cfg);
        assert_eq!(stats.records_out, 4);
        assert_eq!(out.split_inclusive(|b| *b == b'\n').count(), 4);
    }

    #[test]
    fn output_size_is_n_when_input_smaller_than_k() {
        let input: &[u8] = b"a\nb\nc\n";
        let cfg = Config {
            k: 100,
            seed: 1,
            ..Config::default()
        };
        let (out, stats) = run_bytes(input, &cfg);
        assert_eq!(stats.records_out, 3);
        assert_eq!(out.split_inclusive(|b| *b == b'\n').count(), 3);
    }

    #[test]
    fn deterministic_with_same_seed() {
        let input: &[u8] = b"a\nb\nc\nd\ne\nf\ng\nh\ni\nj\nk\nl\nm\nn\no\np\n";
        let cfg = Config {
            k: 5,
            seed: 99,
            ..Config::default()
        };
        let (a, _) = run_bytes(input, &cfg);
        let (b, _) = run_bytes(input, &cfg);
        assert_eq!(a, b);
    }

    #[test]
    fn every_output_record_is_from_input() {
        let input: &[u8] = b"a\nb\nc\nd\ne\nf\ng\nh\ni\nj\nk\nl\n";
        let cfg = Config {
            k: 4,
            seed: 7,
            ..Config::default()
        };
        let (out, _) = run_bytes(input, &cfg);
        let input_set: std::collections::HashSet<&[u8]> =
            input.split_inclusive(|b| *b == b'\n').collect();
        for rec in out.split_inclusive(|b| *b == b'\n') {
            assert!(
                input_set.contains(rec),
                "output record {rec:?} not in input"
            );
        }
    }

    #[test]
    fn approximately_uniform_coverage_over_many_seeds() {
        // Over many seeded runs, each input record should be included in the
        // reservoir at approximately K/N fraction of runs. Relaxed bounds so
        // the test isn't flaky.
        let n = 40;
        let k = 10;
        let trials = 500;
        let input: Vec<u8> = (0..n)
            .map(|i| format!("{i}\n"))
            .collect::<String>()
            .into_bytes();

        let mut counts = HashMap::<String, u32>::new();
        for seed in 0..trials {
            let inp = Input::from_reader(Box::new(std::io::Cursor::new(input.clone())), None, None)
                .unwrap();
            let mut out = Vec::new();
            let cfg = Config {
                k,
                seed,
                ..Config::default()
            };
            run(inp, &mut out, &cfg).unwrap();
            for rec in String::from_utf8(out).unwrap().lines() {
                *counts.entry(rec.to_string()).or_default() += 1;
            }
        }

        let expected = trials as f64 * (k as f64 / n as f64); // ≈ 125
        for i in 0..n {
            let c = counts.get(&i.to_string()).copied().unwrap_or(0) as f64;
            // Empirical count should be within 40% of expected (125±50).
            // 40% is loose but the test runs over 500 seeds which is a
            // modest sample; picking tighter bounds risks flakiness.
            let ratio = c / expected;
            assert!(
                (0.6..1.4).contains(&ratio),
                "record {i}: got {c} inclusions, expected ~{expected:.0} (ratio {ratio:.2})"
            );
        }
    }
}
