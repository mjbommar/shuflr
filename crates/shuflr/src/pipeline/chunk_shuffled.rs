//! `--shuffle=chunk-shuffled` on seekable-zstd inputs (002 §2.1 + 004 §6.1).
//!
//! Each frame produced by `shuflr convert` is a record-aligned standalone
//! zstd frame (004 §2.1). That invariant lets us implement chunk-shuffle
//! without any boundary reconciliation: we permute the frame order, then
//! for each frame decode it, Fisher-Yates the records inside, and emit.
//!
//! Quality characterization (unchanged from 002 §2.1):
//!   - Not a uniform permutation. Output is a block-interleaved local
//!     shuffle with frame-level granularity.
//!   - Originally-adjacent lines co-occur with probability ~1/(records per
//!     frame); across-frame adjacency is as rare as a uniform shuffle.
//!   - Inadequate for inputs sorted by any attribute — run `shuflr index`
//!     + `--shuffle=index-perm` instead (PR-8).
//!
//! Memory: exactly one decoded frame at a time (`decompressed_size` bytes)
//! plus its offset index (8 bytes × records_per_frame). Target frame size
//! is 2 MiB, so RSS stays O(MiB) regardless of file size.

use std::io::{BufWriter, Write};

use rand::seq::SliceRandom;

use crate::error::{Error, Result};
use crate::framing::{OnError, Stats};
use crate::io::zstd_seekable::SeekableReader;
use crate::seed::Seed;

/// Configuration for a chunk-shuffled run on seekable input.
#[derive(Debug, Clone)]
pub struct Config {
    pub seed: u64,
    pub epoch: u64,
    pub max_line: u64,
    pub on_error: OnError,
    pub sample: Option<u64>,
    pub ensure_trailing_newline: bool,
    /// Distributed partition: rank R of W takes every W-th frame of the shuffled
    /// frame order starting at offset R. Disjoint across ranks.
    pub partition: Option<(u32, u32)>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            seed: 0,
            epoch: 0,
            max_line: 16 * 1024 * 1024,
            on_error: OnError::Skip,
            sample: None,
            ensure_trailing_newline: true,
            partition: None,
        }
    }
}

/// Run chunk-shuffled over a seekable-zstd input.
pub fn run(mut reader: SeekableReader, sink: impl Write, cfg: &Config) -> Result<Stats> {
    let seed = Seed::new(cfg.seed);
    let mut writer = BufWriter::with_capacity(2 * 1024 * 1024, sink);
    let mut stats = Stats::default();

    // Permute frame order using the epoch-scoped perm seed.
    let mut frame_order: Vec<usize> = (0..reader.num_frames()).collect();
    {
        let mut perm_rng = Seed::rng_from(seed.perm(cfg.epoch));
        frame_order.shuffle(&mut perm_rng);
    }

    // Distributed partition: take every W-th frame starting at R. Disjoint
    // across ranks; every frame is claimed by exactly one rank.
    let (rank, world_size) = cfg.partition.unwrap_or((0, 1));
    let my_frames: Vec<usize> = if world_size <= 1 {
        frame_order
    } else {
        frame_order
            .into_iter()
            .enumerate()
            .filter_map(|(pos, fid)| {
                if (pos as u32) % world_size == rank {
                    Some(fid)
                } else {
                    None
                }
            })
            .collect()
    };

    for frame_id in my_frames {
        let frame = reader.decompress_frame(frame_id)?;
        stats.bytes_in += frame.len() as u64;

        // Build offset index of records within the frame. Every record
        // ends on `\n` (writer-side invariant), so memchr_iter gives us
        // (end inclusive) positions.
        let offsets = record_offsets(&frame);
        stats.records_in += offsets.len() as u64;

        // Fisher-Yates the record order using a per-frame seed.
        let mut order: Vec<usize> = (0..offsets.len()).collect();
        let mut frame_rng = Seed::rng_from(seed.frame(cfg.epoch, frame_id as u64));
        // SliceRandom::shuffle uses Fisher-Yates under the hood.
        order.shuffle(&mut frame_rng);

        for rec_idx in order {
            let (start, end) = offsets[rec_idx];
            let rec = &frame[start..end];
            let len = rec.len() as u64;

            // --max-line policy.
            if len > cfg.max_line {
                let keep =
                    stats.apply_oversize_policy(cfg.on_error, start as u64, len, cfg.max_line)?;
                if !keep {
                    continue;
                }
            }

            writer.write_all(rec).map_err(Error::Io)?;
            stats.bytes_out += len;
            stats.records_out += 1;

            if let Some(cap) = cfg.sample
                && stats.records_out >= cap
            {
                writer.flush().map_err(Error::Io)?;
                return Ok(stats);
            }
        }
    }

    writer.flush().map_err(Error::Io)?;
    Ok(stats)
}

/// For a decoded frame, return (start_inclusive, end_exclusive) byte
/// offsets of every `\n`-terminated record. If the frame ends without a
/// trailing `\n` (shouldn't happen for our writer's output, but we
/// tolerate it), the final partial record is treated as one record.
fn record_offsets(frame: &[u8]) -> Vec<(usize, usize)> {
    let mut out = Vec::new();
    let mut start = 0usize;
    for nl in memchr::memchr_iter(b'\n', frame) {
        out.push((start, nl + 1));
        start = nl + 1;
    }
    if start < frame.len() {
        out.push((start, frame.len()));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::zstd_seekable::writer::{Writer, WriterConfig};
    use std::collections::HashSet;

    /// Build a seekable zstd file with the given records. Returns the temp file.
    fn seekable_file(records: &[&str], frame_size: usize) -> tempfile::NamedTempFile {
        let tf = tempfile::NamedTempFile::new().unwrap();
        {
            let file = std::fs::File::create(tf.path()).unwrap();
            let mut w = Writer::new(
                file,
                WriterConfig {
                    level: 3,
                    frame_size,
                    checksums: true,
                    record_aligned: true,
                },
            );
            for r in records {
                w.write_block(r.as_bytes()).unwrap();
            }
            w.finish().unwrap();
        }
        tf
    }

    #[test]
    fn chunk_shuffled_preserves_multiset() {
        let records: Vec<String> = (0..500).map(|i| format!("rec_{i:03}\n")).collect();
        let record_refs: Vec<&str> = records.iter().map(|s| s.as_str()).collect();
        let tf = seekable_file(&record_refs, 256);

        let reader = SeekableReader::open(tf.path()).unwrap();
        let mut out = Vec::new();
        let cfg = Config {
            seed: 42,
            ..Config::default()
        };
        let stats = run(reader, &mut out, &cfg).unwrap();

        assert_eq!(stats.records_in, 500);
        assert_eq!(stats.records_out, 500);

        let text = String::from_utf8(out).unwrap();
        let mut out_lines: Vec<&str> = text.lines().collect();
        let mut in_lines: Vec<&str> = records.iter().map(|s| s.trim_end_matches('\n')).collect();
        out_lines.sort_unstable();
        in_lines.sort_unstable();
        assert_eq!(in_lines, out_lines);
    }

    #[test]
    fn chunk_shuffled_deterministic_with_same_seed() {
        let records: Vec<String> = (0..200).map(|i| format!("r_{i:03}\n")).collect();
        let record_refs: Vec<&str> = records.iter().map(|s| s.as_str()).collect();
        let tf = seekable_file(&record_refs, 128);

        let run_with_seed = |seed: u64| {
            let reader = SeekableReader::open(tf.path()).unwrap();
            let mut out = Vec::new();
            run(
                reader,
                &mut out,
                &Config {
                    seed,
                    ..Config::default()
                },
            )
            .unwrap();
            out
        };

        let a = run_with_seed(7);
        let b = run_with_seed(7);
        let c = run_with_seed(8);
        assert_eq!(a, b, "same seed must produce byte-identical output");
        assert_ne!(a, c, "different seeds must produce different orders");
    }

    #[test]
    fn chunk_shuffled_actually_shuffles() {
        // With enough records across multiple frames, the output should
        // differ from the input in its ordering.
        let records: Vec<String> = (0..100).map(|i| format!("ordered_{i:03}\n")).collect();
        let record_refs: Vec<&str> = records.iter().map(|s| s.as_str()).collect();
        let tf = seekable_file(&record_refs, 64);

        let reader = SeekableReader::open(tf.path()).unwrap();
        assert!(reader.num_frames() >= 2);
        let mut out = Vec::new();
        run(
            reader,
            &mut out,
            &Config {
                seed: 31,
                ..Config::default()
            },
        )
        .unwrap();

        let original: String = records.concat();
        let shuffled = String::from_utf8(out).unwrap();
        assert_ne!(shuffled, original);

        // And every record still appears once.
        let expected: HashSet<&str> = records.iter().map(|s| s.as_str()).collect();
        let seen: HashSet<&str> = shuffled.split_inclusive('\n').collect();
        assert_eq!(expected, seen);
    }

    #[test]
    fn sample_caps_output() {
        let records: Vec<String> = (0..100).map(|i| format!("{i}\n")).collect();
        let record_refs: Vec<&str> = records.iter().map(|s| s.as_str()).collect();
        let tf = seekable_file(&record_refs, 64);

        let reader = SeekableReader::open(tf.path()).unwrap();
        let mut out = Vec::new();
        let cfg = Config {
            seed: 5,
            sample: Some(10),
            ..Config::default()
        };
        let stats = run(reader, &mut out, &cfg).unwrap();
        assert_eq!(stats.records_out, 10);
        let lines = String::from_utf8(out).unwrap();
        assert_eq!(lines.lines().count(), 10);
    }

    #[test]
    fn different_epochs_give_different_orders() {
        let records: Vec<String> = (0..200).map(|i| format!("r_{i:03}\n")).collect();
        let record_refs: Vec<&str> = records.iter().map(|s| s.as_str()).collect();
        let tf = seekable_file(&record_refs, 128);

        let a = {
            let reader = SeekableReader::open(tf.path()).unwrap();
            let mut out = Vec::new();
            run(
                reader,
                &mut out,
                &Config {
                    seed: 42,
                    epoch: 0,
                    ..Config::default()
                },
            )
            .unwrap();
            out
        };
        let b = {
            let reader = SeekableReader::open(tf.path()).unwrap();
            let mut out = Vec::new();
            run(
                reader,
                &mut out,
                &Config {
                    seed: 42,
                    epoch: 1,
                    ..Config::default()
                },
            )
            .unwrap();
            out
        };
        assert_ne!(a, b);
    }
}
