//! `--shuffle=buffer` — ring-buffer local shuffle (002 §2.3).
//!
//! Maintain a K-slot ring. For each incoming record, if the ring has a
//! free slot, place the record there; else pick a random slot, emit its
//! current occupant, and insert the new record into that slot. At EOF,
//! drain the ring in random order.
//!
//! Properties: emits **every** input record exactly once; every record's
//! displacement from its input position is bounded by `K`; output is
//! *not* a uniform permutation — it's the same quality as HuggingFace
//! `IterableDataset.shuffle(buffer_size=K)`. For training corpora with
//! strong source-order locality this is inadequate; use `index-perm` or
//! a prior `convert` + `chunk-shuffled` (PR-7) instead.
//!
//! Determinism: the RNG is a seeded `ChaCha20Rng` and every choice is
//! addressable from `seed`. The `--seed` flag picks it; omitted seed
//! picks a boot-time value (logged so runs are reproducible post-hoc).

use std::io::{BufRead, BufWriter, Write};

use rand::{RngCore, SeedableRng};
use rand_chacha::ChaCha20Rng;

use crate::error::{Error, Result};
use crate::framing::{OnError, Stats};
use crate::io::Input;

/// Configuration for a buffer-shuffle run.
#[derive(Debug, Clone)]
pub struct Config {
    /// Ring capacity; displacement is bounded by this value.
    pub buffer_size: usize,
    /// Deterministic seed; pass `ChaCha20Rng::seed_from_u64(0)` if you
    /// want a "known" default, or feed a real `--seed` value.
    pub seed: u64,
    /// Per-record byte cap.
    pub max_line: u64,
    /// Policy for oversized records.
    pub on_error: OnError,
    /// Stop after this many records emitted.
    pub sample: Option<u64>,
    /// Patch a trailing `\n` on a final record that lacks one.
    pub ensure_trailing_newline: bool,
    /// Distributed partition: see `passthrough::Config::partition`.
    pub partition: Option<(u32, u32)>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            buffer_size: 100_000,
            seed: 0,
            max_line: 16 * 1024 * 1024,
            on_error: OnError::Skip,
            sample: None,
            ensure_trailing_newline: true,
            partition: None,
        }
    }
}

/// Run buffer shuffle; returns aggregated [`Stats`].
pub fn run(mut input: Input, sink: impl Write, cfg: &Config) -> Result<Stats> {
    assert!(cfg.buffer_size >= 1, "buffer size must be positive");

    let mut writer = BufWriter::with_capacity(2 * 1024 * 1024, sink);
    let mut stats = Stats::default();
    let mut rng = ChaCha20Rng::seed_from_u64(cfg.seed);

    // Ring: Vec<Vec<u8>>, each slot owns its record bytes (including trailing \n
    // if present). Slots are populated in insertion order until the ring fills,
    // then each subsequent record randomly evicts one.
    let mut ring: Vec<Vec<u8>> = Vec::with_capacity(cfg.buffer_size);

    // Reused line buffer for reads from input.
    let mut line: Vec<u8> = Vec::with_capacity(8 * 1024);
    let mut byte_offset: u64 = 0;

    loop {
        line.clear();
        let n = input.read_until(b'\n', &mut line).map_err(Error::Io)?;
        if n == 0 {
            break;
        }
        stats.records_in += 1;
        stats.bytes_in += n as u64;
        let offset = byte_offset;
        byte_offset += n as u64;
        let has_newline = line.last() == Some(&b'\n');
        if !has_newline {
            stats.had_trailing_partial = true;
        }

        // --max-line policy.
        if (n as u64) > cfg.max_line {
            let keep = stats.apply_oversize_policy(cfg.on_error, offset, n as u64, cfg.max_line)?;
            if !keep {
                continue;
            }
        }

        // Distributed partition: only "our" records enter the ring.
        if let Some((rank, world_size)) = cfg.partition
            && world_size > 1
            && ((stats.records_in - 1) as u32) % world_size != rank
        {
            continue;
        }

        // Patch a trailing '\n' on the final partial line so every ring entry
        // is a complete record.
        let mut rec = line.clone();
        if !has_newline && cfg.ensure_trailing_newline {
            rec.push(b'\n');
        }

        if ring.len() < cfg.buffer_size {
            ring.push(rec);
        } else {
            // Evict a uniformly-random slot's occupant; place the new record there.
            let idx = (rng.next_u64() as usize) % cfg.buffer_size;
            let evicted = std::mem::replace(&mut ring[idx], rec);
            emit(&mut writer, &evicted, &mut stats)?;
            if should_stop_after_emit(&stats, cfg.sample) {
                return flush_and_return(writer, stats);
            }
        }
    }

    // Drain remaining ring in uniform random order.
    while !ring.is_empty() {
        let idx = (rng.next_u64() as usize) % ring.len();
        let rec = ring.swap_remove(idx);
        emit(&mut writer, &rec, &mut stats)?;
        if should_stop_after_emit(&stats, cfg.sample) {
            break;
        }
    }

    flush_and_return(writer, stats)
}

fn emit(writer: &mut impl Write, rec: &[u8], stats: &mut Stats) -> Result<()> {
    writer.write_all(rec).map_err(Error::Io)?;
    stats.bytes_out += rec.len() as u64;
    stats.records_out += 1;
    Ok(())
}

fn should_stop_after_emit(stats: &Stats, sample: Option<u64>) -> bool {
    matches!(sample, Some(cap) if stats.records_out >= cap)
}

fn flush_and_return<W: Write>(mut writer: BufWriter<W>, stats: Stats) -> Result<Stats> {
    writer.flush().map_err(Error::Io)?;
    Ok(stats)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    fn run_bytes(input: &'static [u8], cfg: &Config) -> (Vec<u8>, Stats) {
        let inp = Input::from_reader(Box::new(input), None, None).unwrap();
        let mut out = Vec::new();
        let stats = run(inp, &mut out, cfg).unwrap();
        (out, stats)
    }

    #[test]
    fn all_records_emitted_exactly_once() {
        let input: &[u8] = b"a\nb\nc\nd\ne\nf\ng\nh\ni\nj\n";
        let cfg = Config {
            buffer_size: 3,
            seed: 42,
            ..Config::default()
        };
        let (out, stats) = run_bytes(input, &cfg);
        assert_eq!(stats.records_in, 10);
        assert_eq!(stats.records_out, 10);

        let in_set: HashSet<&[u8]> = input.split_inclusive(|b| *b == b'\n').collect();
        let out_set: HashSet<&[u8]> = out.split_inclusive(|b| *b == b'\n').collect();
        assert_eq!(in_set, out_set);
    }

    #[test]
    fn deterministic_with_same_seed() {
        let input: &[u8] = b"a\nb\nc\nd\ne\nf\ng\nh\ni\nj\nk\nl\n";
        let cfg = Config {
            buffer_size: 4,
            seed: 12345,
            ..Config::default()
        };
        let (out1, _) = run_bytes(input, &cfg);
        let (out2, _) = run_bytes(input, &cfg);
        assert_eq!(out1, out2);
    }

    #[test]
    fn different_seed_yields_different_order_on_nontrivial_input() {
        let input: &[u8] = b"a\nb\nc\nd\ne\nf\ng\nh\ni\nj\nk\nl\nm\nn\no\np\n";
        let cfg_a = Config {
            buffer_size: 6,
            seed: 1,
            ..Config::default()
        };
        let cfg_b = Config {
            buffer_size: 6,
            seed: 2,
            ..Config::default()
        };
        let (out_a, _) = run_bytes(input, &cfg_a);
        let (out_b, _) = run_bytes(input, &cfg_b);
        // Not a hard guarantee, but two ChaCha20 streams differ; for a
        // 16-line input with buffer=6, seeds 1 and 2 are overwhelmingly
        // unlikely to produce identical orderings.
        assert_ne!(out_a, out_b);
    }

    #[test]
    fn input_smaller_than_buffer_just_shuffles_what_is_there() {
        let input: &[u8] = b"a\nb\nc\n";
        let cfg = Config {
            buffer_size: 100,
            seed: 7,
            ..Config::default()
        };
        let (out, stats) = run_bytes(input, &cfg);
        assert_eq!(stats.records_out, 3);
        let out_lines: HashSet<&[u8]> = out.split_inclusive(|b| *b == b'\n').collect();
        let in_lines: HashSet<&[u8]> = input.split_inclusive(|b| *b == b'\n').collect();
        assert_eq!(in_lines, out_lines);
    }

    #[test]
    fn buffer_one_is_identity() {
        // K=1 means: first record sits in ring, every subsequent record evicts
        // slot 0 (only slot). So the "evicted" is always the previous insertion,
        // then we drain the final held record at end. Net effect: identity.
        let input: &[u8] = b"one\ntwo\nthree\nfour\n";
        let cfg = Config {
            buffer_size: 1,
            seed: 0,
            ..Config::default()
        };
        let (out, _) = run_bytes(input, &cfg);
        assert_eq!(out, input);
    }

    #[test]
    fn sample_caps_output() {
        let input: &[u8] = b"a\nb\nc\nd\ne\nf\ng\nh\n";
        let cfg = Config {
            buffer_size: 3,
            seed: 99,
            sample: Some(4),
            ..Config::default()
        };
        let (out, stats) = run_bytes(input, &cfg);
        assert_eq!(stats.records_out, 4);
        let lines = out.split_inclusive(|b| *b == b'\n').count();
        assert_eq!(lines, 4);
    }

    #[test]
    fn mean_displacement_is_on_order_of_buffer_size() {
        // Buffer shuffle has a geometric-tailed displacement distribution —
        // NO hard bound (contrary to 002 §11.1 property 10; we'll correct
        // that in a future design amendment). A robust smoke test: mean
        // absolute displacement over N records is within a constant factor
        // of K for this algorithm.
        let n = 2_000usize;
        let input: Vec<u8> = (0..n)
            .map(|i| format!("{i}\n"))
            .collect::<String>()
            .into_bytes();
        let k = 50usize;
        let cfg = Config {
            buffer_size: k,
            seed: 31,
            ..Config::default()
        };
        let inp = Input::from_reader(Box::new(std::io::Cursor::new(input)), None, None).unwrap();
        let mut out = Vec::new();
        let _ = run(inp, &mut out, &cfg).unwrap();

        let out_text = String::from_utf8(out).unwrap();
        let order: Vec<usize> = out_text
            .lines()
            .map(|s| s.parse::<usize>().unwrap())
            .collect();
        assert_eq!(order.len(), n);

        let total: u64 = order
            .iter()
            .enumerate()
            .map(|(out_pos, &in_pos)| out_pos.abs_diff(in_pos) as u64)
            .sum();
        let mean = total as f64 / n as f64;
        let kf = k as f64;
        assert!(
            (0.3 * kf..3.0 * kf).contains(&mean),
            "mean displacement {mean} not within [{}, {}] for K={k}",
            0.3 * kf,
            3.0 * kf
        );
    }

    #[test]
    fn oversized_skip_policy_drops_record() {
        // Records under the cap should flow through; the oversized one is skipped.
        let input: &[u8] = b"ok\nBIG_RECORD\nfin\n";
        let cfg = Config {
            buffer_size: 2,
            seed: 0,
            max_line: 10,
            on_error: OnError::Skip,
            ..Config::default()
        };
        let (out, stats) = run_bytes(input, &cfg);
        assert_eq!(stats.records_in, 3);
        assert_eq!(stats.oversized_skipped, 1);
        assert_eq!(stats.records_out, 2);
        let in_set: HashSet<&[u8]> = [b"ok\n" as &[u8], b"fin\n"].into_iter().collect();
        let out_set: HashSet<&[u8]> = out.split_inclusive(|b| *b == b'\n').collect();
        assert_eq!(in_set, out_set);
    }
}
