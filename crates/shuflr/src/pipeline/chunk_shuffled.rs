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
    /// Worker threads for the emit phase. 1 = sequential (single-
    /// threaded decode, the old behavior and the default). 0 =
    /// physical cores. Any other N = fixed pool.
    pub emit_threads: usize,
    /// Look-ahead depth for parallel emit — caps in-flight decoded
    /// frames. Ignored when `emit_threads == 1`.
    pub emit_prefetch: usize,
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
            emit_threads: 1,
            emit_prefetch: 8,
        }
    }
}

/// Run chunk-shuffled over a seekable-zstd input.
pub fn run(reader: SeekableReader, sink: impl Write, cfg: &Config) -> Result<Stats> {
    let seed = Seed::new(cfg.seed);
    let path = reader.path().to_path_buf();
    let n_frames = reader.num_frames();

    // Permute frame order using the epoch-scoped perm seed.
    let mut frame_order: Vec<usize> = (0..n_frames).collect();
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

    let mut writer = BufWriter::with_capacity(2 * 1024 * 1024, sink);
    let mut stats = Stats::default();

    if cfg.emit_threads == 1 {
        emit_sequential(reader, &my_frames, cfg, &seed, &mut writer, &mut stats)?;
    } else {
        emit_parallel(&path, &my_frames, cfg, &seed, &mut writer, &mut stats)?;
    }

    writer.flush().map_err(Error::Io)?;
    Ok(stats)
}

fn emit_sequential(
    mut reader: SeekableReader,
    my_frames: &[usize],
    cfg: &Config,
    seed: &Seed,
    writer: &mut BufWriter<impl Write>,
    stats: &mut Stats,
) -> Result<()> {
    for &frame_id in my_frames {
        let frame = reader.decompress_frame(frame_id)?;
        if emit_one_frame(&frame, frame_id, cfg, seed, writer, stats)? {
            return Ok(()); // --sample cap hit
        }
    }
    Ok(())
}

/// Prefetch pipeline for chunk-shuffled. Frames are emitted in
/// `my_frames` order (sequential), which makes the reorder buffer
/// simple: workers return `(position, Arc<Vec<u8>>)`, main pulls
/// position 0, 1, 2, … drains that frame completely, then moves on.
fn emit_parallel(
    path: &std::path::Path,
    my_frames: &[usize],
    cfg: &Config,
    seed: &Seed,
    writer: &mut BufWriter<impl Write>,
    stats: &mut Stats,
) -> Result<()> {
    use crossbeam_channel::{bounded, unbounded};
    use std::io::{Read, Seek, SeekFrom};
    use std::sync::Arc;

    if my_frames.is_empty() {
        return Ok(());
    }

    // Reopen the file once to pull the seek table; share read-only
    // refs to offsets and entries with workers. Each worker opens its
    // own File handle for pread.
    let reader = SeekableReader::open(path)?;
    let entries = reader.entries().to_vec();
    let mut offsets = Vec::with_capacity(entries.len() + 1);
    let mut acc: u64 = 0;
    for e in &entries {
        offsets.push(acc);
        acc += u64::from(e.compressed_size);
    }
    offsets.push(acc);
    drop(reader);

    let threads = if cfg.emit_threads == 0 {
        num_cpus::get_physical().max(1)
    } else {
        cfg.emit_threads
    };
    let prefetch = cfg.emit_prefetch.max(threads).max(1);
    let n = my_frames.len();

    let (job_tx, job_rx) = bounded::<(usize, usize)>(prefetch);
    let (done_tx, done_rx) = unbounded::<(usize, Arc<Vec<u8>>)>();
    let offsets_ref = &offsets;
    let entries_ref = &entries;

    let result: Result<()> = std::thread::scope(|s| -> Result<()> {
        let worker_handles: Vec<_> = (0..threads)
            .map(|_| {
                let rx = job_rx.clone();
                let tx = done_tx.clone();
                s.spawn(move || -> Result<()> {
                    let mut file = std::fs::File::open(path).map_err(Error::Io)?;
                    while let Ok((pos, frame_id)) = rx.recv() {
                        let offset = offsets_ref[frame_id];
                        let comp_size = entries_ref[frame_id].compressed_size as usize;
                        file.seek(SeekFrom::Start(offset)).map_err(Error::Io)?;
                        let mut comp = vec![0u8; comp_size];
                        file.read_exact(&mut comp).map_err(Error::Io)?;
                        let bytes = zstd::stream::decode_all(&comp[..]).map_err(Error::Io)?;
                        if tx.send((pos, Arc::new(bytes))).is_err() {
                            break;
                        }
                    }
                    Ok(())
                })
            })
            .collect();
        drop(done_tx);

        let mut submitted: usize = 0;
        while submitted < prefetch.min(n) {
            if job_tx.send((submitted, my_frames[submitted])).is_err() {
                break;
            }
            submitted += 1;
        }

        use std::collections::HashMap;
        let mut buffer: HashMap<usize, Arc<Vec<u8>>> = HashMap::with_capacity(prefetch);
        for consumed in 0..n {
            let frame_bytes: Arc<Vec<u8>> = loop {
                if let Some(b) = buffer.remove(&consumed) {
                    break b;
                }
                match done_rx.recv() {
                    Ok((pos, bytes)) => {
                        buffer.insert(pos, bytes);
                    }
                    Err(_) => {
                        return Err(Error::Input(
                            "chunk-shuffled emit workers exited early".into(),
                        ));
                    }
                }
            };

            let frame_id = my_frames[consumed];
            let stop = emit_one_frame(&frame_bytes, frame_id, cfg, seed, writer, stats)?;
            if stop {
                break;
            }

            // Refill the prefetch window behind us.
            if submitted < n && job_tx.send((submitted, my_frames[submitted])).is_err() {
                break;
            }
            if submitted < n {
                submitted += 1;
            }
        }

        drop(job_tx);
        drop(buffer);
        while done_rx.recv().is_ok() {}
        for h in worker_handles {
            h.join()
                .map_err(|_| Error::Input("chunk-shuffled worker panicked".into()))??;
        }
        Ok(())
    });
    result
}

/// Process one decoded frame: compute record offsets, shuffle record
/// order with the per-frame seed, apply oversize policy, emit records.
/// Returns `Ok(true)` if the `--sample` cap has been reached and the
/// caller should stop; `Ok(false)` to continue with the next frame.
fn emit_one_frame(
    frame: &[u8],
    frame_id: usize,
    cfg: &Config,
    seed: &Seed,
    writer: &mut BufWriter<impl Write>,
    stats: &mut Stats,
) -> Result<bool> {
    stats.bytes_in += frame.len() as u64;
    let offsets = record_offsets(frame);
    stats.records_in += offsets.len() as u64;

    let mut order: Vec<usize> = (0..offsets.len()).collect();
    let mut frame_rng = Seed::rng_from(seed.frame(cfg.epoch, frame_id as u64));
    order.shuffle(&mut frame_rng);

    for rec_idx in order {
        let (start, end) = offsets[rec_idx];
        let rec = &frame[start..end];
        let len = rec.len() as u64;

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
            return Ok(true);
        }
    }
    Ok(false)
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
    fn parallel_emit_matches_sequential() {
        // Same seed across all emit_threads settings must yield
        // byte-identical output.
        let records: Vec<String> = (0..600).map(|i| format!("rec_{i:03}\n")).collect();
        let record_refs: Vec<&str> = records.iter().map(|s| s.as_str()).collect();
        let tf = seekable_file(&record_refs, 128);

        let run_once = |emit_threads: usize, prefetch: usize| {
            let reader = SeekableReader::open(tf.path()).unwrap();
            let mut out = Vec::new();
            let cfg = Config {
                seed: 13,
                emit_threads,
                emit_prefetch: prefetch,
                ..Config::default()
            };
            run(reader, &mut out, &cfg).unwrap();
            out
        };

        let seq = run_once(1, 8);
        let par2 = run_once(2, 4);
        let par4 = run_once(4, 8);
        assert_eq!(seq, par2, "2-thread chunk-shuffled emit must match seq");
        assert_eq!(seq, par4, "4-thread chunk-shuffled emit must match seq");
    }

    #[test]
    fn parallel_emit_honors_sample_cap() {
        let records: Vec<String> = (0..500).map(|i| format!("r{i:03}\n")).collect();
        let record_refs: Vec<&str> = records.iter().map(|s| s.as_str()).collect();
        let tf = seekable_file(&record_refs, 128);

        let reader = SeekableReader::open(tf.path()).unwrap();
        let mut out = Vec::new();
        let cfg = Config {
            seed: 7,
            sample: Some(23),
            emit_threads: 4,
            emit_prefetch: 4,
            ..Config::default()
        };
        let stats = run(reader, &mut out, &cfg).unwrap();
        assert_eq!(stats.records_out, 23);
        let text = String::from_utf8(out).unwrap();
        assert_eq!(text.lines().count(), 23);
    }

    #[test]
    fn parallel_emit_respects_rank_partition() {
        let records: Vec<String> = (0..300).map(|i| format!("r{i:03}\n")).collect();
        let record_refs: Vec<&str> = records.iter().map(|s| s.as_str()).collect();
        let tf = seekable_file(&record_refs, 128);

        let w = 3u32;
        let mut seen: HashSet<String> = HashSet::new();
        for rank in 0..w {
            let reader = SeekableReader::open(tf.path()).unwrap();
            let mut out = Vec::new();
            run(
                reader,
                &mut out,
                &Config {
                    seed: 5,
                    emit_threads: 4,
                    emit_prefetch: 4,
                    partition: Some((rank, w)),
                    ..Config::default()
                },
            )
            .unwrap();
            for ln in String::from_utf8(out).unwrap().lines() {
                assert!(seen.insert(ln.to_string()), "duplicate across ranks: {ln}");
            }
        }
        assert_eq!(seen.len(), 300);
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
