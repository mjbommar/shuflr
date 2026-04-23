//! `--shuffle=index-perm` on seekable-zstd inputs.
//!
//! Unlike plain-file index-perm which pread's each record in O(1), zstd
//! is block-compressed — random access costs one frame decode. We
//! mitigate this with a small LRU cache of decoded frames
//! ([`super::super::io::zstd_seekable::FrameCache`]) but note that on a
//! truly uniform permutation the hit rate stays near
//! `cache_capacity / num_frames`, which on big files is <1%. The
//! dominating cost is therefore one frame decode per emitted record.
//!
//! Practical implication: for the 1.2M-record EDGAR seekable, emitting
//! the full permutation costs ~1.2M decodes ≈ tens of minutes. For the
//! `--sample=N` case with N << records, cost is ~N decodes, so
//! `--sample=10_000` finishes in seconds. Users who want faster full
//! uniform shuffle should decompress to plain `.jsonl` first and use
//! the plain-file [`super::index_perm`] path.

use std::io::{BufWriter, Write};
use std::path::Path;

use rand::seq::SliceRandom;

use crate::error::{Error, Result};
use crate::framing::Stats;
use crate::io::zstd_seekable::{FrameCache, RecordIndex, SeekableReader};
use crate::seed::Seed;

/// Default LRU frame cache capacity. 32 frames × ~2 MiB/frame ≈ 64 MiB
/// working set — modest, and bumps cache hit rate on accidentally-local
/// accesses without dominating RSS. Tune via [`Config::cache_capacity`].
pub const DEFAULT_CACHE_CAPACITY: usize = 32;

#[derive(Clone)]
pub struct Config {
    pub seed: u64,
    pub epoch: u64,
    pub sample: Option<u64>,
    pub ensure_trailing_newline: bool,
    /// LRU cache size measured in frames.
    pub cache_capacity: usize,
    /// Optional (rank, world_size) partition. Each rank takes every
    /// W-th index of the shuffled permutation starting at offset R.
    pub partition: Option<(u32, u32)>,
    /// Optional callback invoked per frame during an index-build cold
    /// path (`on_build_frame(frame_idx, n_frames)`). Used by the CLI
    /// to drive a progress bar; library and tests leave it `None`.
    pub on_build_frame: Option<std::sync::Arc<dyn Fn(usize, usize) + Send + Sync>>,
    /// Worker threads for the cold index-build. 0 = physical cores
    /// (default), 1 = sequential fallback. Ignored on a sidecar-hit.
    pub build_threads: usize,
    /// Worker threads for the emit phase (frame decode per record).
    /// 0 = physical cores, 1 = sequential (the old LRU-cache path).
    /// Non-sequential runs use a prefetch pipeline: main submits
    /// `(position, frame_id)` jobs, workers decompress ahead, main
    /// emits in shuffle order from a reorder buffer.
    pub emit_threads: usize,
    /// How many positions ahead the prefetch pipeline may look. Caps
    /// in-flight decoded frames so RSS stays bounded on a uniform
    /// permutation. Ignored for `emit_threads == 1`.
    pub emit_prefetch: usize,
}

impl std::fmt::Debug for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Config")
            .field("seed", &self.seed)
            .field("epoch", &self.epoch)
            .field("sample", &self.sample)
            .field("ensure_trailing_newline", &self.ensure_trailing_newline)
            .field("cache_capacity", &self.cache_capacity)
            .field("partition", &self.partition)
            .field("on_build_frame", &self.on_build_frame.is_some())
            .field("build_threads", &self.build_threads)
            .field("emit_threads", &self.emit_threads)
            .field("emit_prefetch", &self.emit_prefetch)
            .finish()
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            seed: 0,
            epoch: 0,
            sample: None,
            ensure_trailing_newline: true,
            cache_capacity: DEFAULT_CACHE_CAPACITY,
            partition: None,
            on_build_frame: None,
            build_threads: 0,
            emit_threads: 1,
            emit_prefetch: 32,
        }
    }
}

/// Aggregate diagnostics in addition to the standard [`Stats`].
#[derive(Default, Debug, Clone)]
pub struct RunMetrics {
    pub index_build_ms: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub records_scanned: u64,
    pub total_decompressed_in_build: u64,
}

/// Build the RecordIndex, using the parallel path when configured and
/// falling back to sequential for the `--build-threads=1` case. The
/// parallel path preserves frame-order output so both modes produce a
/// byte-identical sidecar (tested in `build_parallel_matches_sequential`).
fn build_index(
    reader: &mut SeekableReader,
    path: &Path,
    cfg: &Config,
) -> Result<(RecordIndex, u64)> {
    if cfg.build_threads == 1 {
        RecordIndex::build_with_progress(reader, |i, n| {
            if let Some(cb) = &cfg.on_build_frame {
                cb(i, n);
            }
        })
    } else {
        let on_frame: Box<dyn Fn(usize, usize) + Send + Sync> = match &cfg.on_build_frame {
            Some(cb) => {
                let cb = cb.clone();
                Box::new(move |i, n| cb(i, n))
            }
            None => Box::new(|_, _| {}),
        };
        RecordIndex::build_parallel(path, cfg.build_threads, on_frame)
    }
}

/// Run index-perm over a seekable-zstd input.
///
/// If a `<path>.shuflr-idx-zst` sidecar exists and its fingerprint matches
/// the current input's metadata, the sidecar is loaded instead of
/// re-scanning the file. Otherwise the index is built from scratch and
/// persisted atomically so the next run is cheap.
pub fn run(path: &Path, sink: impl Write, cfg: &Config) -> Result<(Stats, RunMetrics)> {
    use crate::Fingerprint;
    use crate::io::zstd_seekable::record_index;

    let mut reader = SeekableReader::open(path)?;
    let mut metrics = RunMetrics::default();

    // Fingerprint-gated sidecar load.
    let fingerprint = Fingerprint::from_metadata(path)?;
    let sidecar = record_index::sidecar_path(path);
    let t_build_start = std::time::Instant::now();
    let (index, scanned) = match RecordIndex::load(&sidecar) {
        Ok(loaded) if loaded.fingerprint == Some(fingerprint) => {
            tracing::info!(
                sidecar = %sidecar.display(),
                records = loaded.entries.len(),
                "loaded seekable-zstd record-index from sidecar",
            );
            (loaded, 0u64)
        }
        Ok(_) => {
            tracing::warn!(
                sidecar = %sidecar.display(),
                "sidecar fingerprint mismatches input; rebuilding",
            );
            let (built, scanned) = build_index(&mut reader, path, cfg)?;
            if let Err(e) = built.save(&sidecar, fingerprint) {
                tracing::warn!(err = %e, "failed to persist rebuilt sidecar; continuing");
            }
            (built, scanned)
        }
        Err(_) => {
            let (built, scanned) = build_index(&mut reader, path, cfg)?;
            if let Err(e) = built.save(&sidecar, fingerprint) {
                tracing::warn!(err = %e, "failed to persist new sidecar; continuing");
            } else {
                tracing::info!(
                    sidecar = %sidecar.display(),
                    records = built.entries.len(),
                    "built + saved seekable-zstd record-index sidecar",
                );
            }
            (built, scanned)
        }
    };
    metrics.index_build_ms = t_build_start.elapsed().as_millis() as u64;
    metrics.records_scanned = index.len() as u64;
    metrics.total_decompressed_in_build = scanned;

    let mut writer = BufWriter::with_capacity(2 * 1024 * 1024, sink);
    let mut stats = Stats::default();
    if index.is_empty() {
        writer.flush().map_err(Error::Io)?;
        return Ok((stats, metrics));
    }

    // Fisher-Yates over record positions using the PRF-hierarchy perm seed.
    let seed = Seed::new(cfg.seed);
    let mut perm: Vec<u32> = (0..index.len() as u32).collect();
    let mut rng = Seed::rng_from(seed.perm(cfg.epoch));
    perm.shuffle(&mut rng);

    // Partition: every W-th index starting at R.
    let (rank, world_size) = cfg.partition.unwrap_or((0, 1));
    let my_perm: Vec<u32> = if world_size <= 1 {
        perm
    } else {
        perm.into_iter()
            .enumerate()
            .filter_map(|(pos, i)| {
                if (pos as u32) % world_size == rank {
                    Some(i)
                } else {
                    None
                }
            })
            .collect()
    };

    if cfg.emit_threads == 1 {
        emit_sequential(
            path,
            &index,
            &my_perm,
            cfg,
            &mut reader,
            &mut writer,
            &mut stats,
            &mut metrics,
        )?;
    } else {
        emit_parallel(
            path,
            &index,
            &my_perm,
            cfg,
            &mut writer,
            &mut stats,
            &mut metrics,
        )?;
    }

    writer.flush().map_err(Error::Io)?;
    Ok((stats, metrics))
}

#[allow(clippy::too_many_arguments)]
fn emit_sequential(
    _path: &Path,
    index: &RecordIndex,
    my_perm: &[u32],
    cfg: &Config,
    reader: &mut SeekableReader,
    writer: &mut BufWriter<impl Write>,
    stats: &mut Stats,
    metrics: &mut RunMetrics,
) -> Result<()> {
    let mut cache = FrameCache::new(cfg.cache_capacity);
    for &idx in my_perm {
        let loc = &index.entries[idx as usize];
        let frame = cache.get(reader, loc.frame_id)?;
        let start = loc.offset_in_frame as usize;
        let end = start + loc.length as usize;
        let rec = &frame[start..end];
        let ends_with_nl = rec.last() == Some(&b'\n');
        writer.write_all(rec).map_err(Error::Io)?;
        stats.bytes_out += rec.len() as u64;
        if !ends_with_nl && cfg.ensure_trailing_newline {
            writer.write_all(b"\n").map_err(Error::Io)?;
            stats.bytes_out += 1;
        }
        stats.records_out += 1;
        stats.records_in += 1;
        stats.bytes_in += rec.len() as u64;

        if let Some(cap) = cfg.sample
            && stats.records_out >= cap
        {
            break;
        }
    }

    metrics.cache_hits = cache.hits;
    metrics.cache_misses = cache.misses;
    Ok(())
}

/// Parallel emit: prefetch-pipeline variant. Main submits
/// `(position, frame_id)` jobs to a bounded pool; workers open their
/// own File handles, `pread + decompress`, return
/// `(position, Arc<Vec<u8>>)` via an unbounded result channel. Main
/// uses a HashMap reorder buffer keyed by position so records leave in
/// the original shuffle order.
///
/// Prefetch depth bounds in-flight decoded frames. On a truly uniform
/// permutation over EDGAR (~2 MiB median frame), `emit_prefetch=32`
/// is ~64 MiB of RAM. Large single-record frames (up to 427 MiB on
/// EDGAR) can briefly spike RSS — same risk the sequential path
/// already carries.
fn emit_parallel(
    path: &Path,
    index: &RecordIndex,
    my_perm: &[u32],
    cfg: &Config,
    writer: &mut BufWriter<impl Write>,
    stats: &mut Stats,
    _metrics: &mut RunMetrics,
) -> Result<()> {
    use crossbeam_channel::{bounded, unbounded};
    use std::io::{Read, Seek, SeekFrom};
    use std::sync::Arc;

    // Open one reader-like channel of (offset, comp_size) per worker.
    // We reuse SeekableReader's seek-table parsing to get those.
    let reader = SeekableReader::open(path)?;
    let table_entries = reader.entries().to_vec();
    let mut offsets = Vec::with_capacity(table_entries.len() + 1);
    let mut acc: u64 = 0;
    for e in &table_entries {
        offsets.push(acc);
        acc += u64::from(e.compressed_size);
    }
    offsets.push(acc);
    drop(reader);

    let n = my_perm.len();
    let threads = if cfg.emit_threads == 0 {
        num_cpus::get_physical().max(1)
    } else {
        cfg.emit_threads
    };
    let prefetch = cfg.emit_prefetch.max(threads).max(1);

    let (job_tx, job_rx) = bounded::<(usize, u32)>(prefetch);
    let (done_tx, done_rx) = unbounded::<(usize, Arc<Vec<u8>>)>();
    let offsets_ref = &offsets;
    let table_entries_ref = &table_entries;

    let result: Result<()> = std::thread::scope(|s| -> Result<()> {
        let worker_handles: Vec<_> = (0..threads)
            .map(|_| {
                let rx = job_rx.clone();
                let tx = done_tx.clone();
                s.spawn(move || -> Result<()> {
                    let mut file = std::fs::File::open(path).map_err(Error::Io)?;
                    while let Ok((pos, frame_id)) = rx.recv() {
                        let offset = offsets_ref[frame_id as usize];
                        let comp_size =
                            table_entries_ref[frame_id as usize].compressed_size as usize;
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

        // Fill initial prefetch window.
        let mut submitted: usize = 0;
        let stop_at = n; // upper bound; will shrink if --sample trips
        while submitted < prefetch.min(stop_at) {
            let idx = my_perm[submitted] as usize;
            let frame_id = index.entries[idx].frame_id;
            if job_tx.send((submitted, frame_id)).is_err() {
                break;
            }
            submitted += 1;
        }

        // Consume in shuffle order; refill as we go.
        use std::collections::HashMap;
        let mut buffer: HashMap<usize, Arc<Vec<u8>>> = HashMap::with_capacity(prefetch);
        let mut sample_tripped = false;
        for consumed in 0..stop_at {
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
                            "emit workers exited before producing all requested frames".into(),
                        ));
                    }
                }
            };

            let loc = &index.entries[my_perm[consumed] as usize];
            let start = loc.offset_in_frame as usize;
            let end = start + loc.length as usize;
            let rec = &frame_bytes[start..end];
            let ends_with_nl = rec.last() == Some(&b'\n');
            writer.write_all(rec).map_err(Error::Io)?;
            stats.bytes_out += rec.len() as u64;
            if !ends_with_nl && cfg.ensure_trailing_newline {
                writer.write_all(b"\n").map_err(Error::Io)?;
                stats.bytes_out += 1;
            }
            stats.records_out += 1;
            stats.records_in += 1;
            stats.bytes_in += rec.len() as u64;

            if let Some(cap) = cfg.sample
                && stats.records_out >= cap
            {
                sample_tripped = true;
                break;
            }

            // Refill — keep the prefetch window full.
            if submitted < n {
                let next_idx = my_perm[submitted] as usize;
                let next_frame_id = index.entries[next_idx].frame_id;
                if job_tx.send((submitted, next_frame_id)).is_err() {
                    break;
                }
                submitted += 1;
            }
        }
        let _ = sample_tripped;

        // Drain any still-in-flight results before closing the queue;
        // otherwise workers block on send and we deadlock in join.
        drop(job_tx);
        drop(buffer);
        while done_rx.recv().is_ok() {}

        for h in worker_handles {
            h.join()
                .map_err(|_| Error::Input("emit worker panicked".into()))??;
        }
        Ok(())
    });
    result?;

    // Parallel emit path doesn't consult the LRU cache.
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::zstd_seekable::writer::{Writer, WriterConfig};
    use std::collections::BTreeSet;

    fn seekable_file(records: &[&str]) -> tempfile::NamedTempFile {
        let tf = tempfile::NamedTempFile::new().unwrap();
        let file = std::fs::File::create(tf.path()).unwrap();
        let mut w = Writer::new(
            file,
            WriterConfig {
                level: 3,
                frame_size: 128,
                checksums: true,
                record_aligned: true,
            },
        );
        for r in records {
            w.write_block(r.as_bytes()).unwrap();
        }
        w.finish().unwrap();
        tf
    }

    #[test]
    fn parallel_emit_matches_sequential_emit() {
        let records: Vec<String> = (0..400).map(|i| format!("rec_{i:03}\n")).collect();
        let record_refs: Vec<&str> = records.iter().map(|s| s.as_str()).collect();
        let tf = seekable_file(&record_refs);

        let run_once = |emit_threads: usize| {
            let mut out = Vec::new();
            let cfg = Config {
                seed: 99,
                emit_threads,
                emit_prefetch: 8,
                ..Default::default()
            };
            run(tf.path(), &mut out, &cfg).unwrap();
            out
        };
        let seq = run_once(1);
        let par2 = run_once(2);
        let par4 = run_once(4);
        assert_eq!(seq, par2, "2-thread emit must be byte-identical to seq");
        assert_eq!(seq, par4, "4-thread emit must be byte-identical to seq");
    }

    #[test]
    fn parallel_emit_honors_sample_cap() {
        let records: Vec<String> = (0..500).map(|i| format!("r{i:03}\n")).collect();
        let record_refs: Vec<&str> = records.iter().map(|s| s.as_str()).collect();
        let tf = seekable_file(&record_refs);
        let mut out = Vec::new();
        let cfg = Config {
            seed: 3,
            emit_threads: 4,
            emit_prefetch: 4,
            sample: Some(17),
            ..Default::default()
        };
        let (stats, _) = run(tf.path(), &mut out, &cfg).unwrap();
        assert_eq!(stats.records_out, 17);
        let text = String::from_utf8(out).unwrap();
        assert_eq!(text.lines().count(), 17);
    }

    #[test]
    fn parallel_emit_respects_rank_partition() {
        use std::collections::HashSet;
        let records: Vec<String> = (0..320).map(|i| format!("r{i:03}\n")).collect();
        let record_refs: Vec<&str> = records.iter().map(|s| s.as_str()).collect();
        let tf = seekable_file(&record_refs);
        let w = 4u32;
        let mut seen: HashSet<String> = HashSet::new();
        for rank in 0..w {
            let mut out = Vec::new();
            run(
                tf.path(),
                &mut out,
                &Config {
                    seed: 11,
                    emit_threads: 4,
                    emit_prefetch: 4,
                    partition: Some((rank, w)),
                    ..Default::default()
                },
            )
            .unwrap();
            for ln in String::from_utf8(out).unwrap().lines() {
                assert!(seen.insert(ln.to_string()), "rank overlap on {ln}");
            }
        }
        assert_eq!(seen.len(), 320);
    }

    #[test]
    fn full_permutation_preserves_multiset() {
        let records: Vec<String> = (0..300).map(|i| format!("rec_{i:03}\n")).collect();
        let record_refs: Vec<&str> = records.iter().map(|s| s.as_str()).collect();
        let tf = seekable_file(&record_refs);

        let mut out = Vec::new();
        let cfg = Config {
            seed: 7,
            ..Default::default()
        };
        let (stats, metrics) = run(tf.path(), &mut out, &cfg).unwrap();
        assert_eq!(stats.records_out, 300);
        assert_eq!(metrics.records_scanned, 300);

        let text = String::from_utf8(out).unwrap();
        let in_set: BTreeSet<String> = records
            .iter()
            .map(|s| s.trim_end_matches('\n').to_string())
            .collect();
        let out_set: BTreeSet<String> = text.lines().map(|s| s.to_string()).collect();
        assert_eq!(in_set, out_set);
    }

    #[test]
    fn determinism_by_seed() {
        let records: Vec<String> = (0..100).map(|i| format!("r{i:03}\n")).collect();
        let record_refs: Vec<&str> = records.iter().map(|s| s.as_str()).collect();
        let tf = seekable_file(&record_refs);

        let go = |seed: u64| {
            let mut out = Vec::new();
            let _ = run(
                tf.path(),
                &mut out,
                &Config {
                    seed,
                    ..Default::default()
                },
            )
            .unwrap();
            out
        };
        let a = go(42);
        let b = go(42);
        let c = go(43);
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn sample_caps_output_at_n() {
        let records: Vec<String> = (0..500).map(|i| format!("r{i:03}\n")).collect();
        let record_refs: Vec<&str> = records.iter().map(|s| s.as_str()).collect();
        let tf = seekable_file(&record_refs);

        let mut out = Vec::new();
        let cfg = Config {
            seed: 1,
            sample: Some(10),
            ..Default::default()
        };
        let (stats, _) = run(tf.path(), &mut out, &cfg).unwrap();
        assert_eq!(stats.records_out, 10);
        let lines: Vec<&str> = std::str::from_utf8(&out).unwrap().lines().collect();
        assert_eq!(lines.len(), 10);
        // All sampled records exist in the input
        let in_set: BTreeSet<&str> = records.iter().map(|s| s.trim_end_matches('\n')).collect();
        for ln in &lines {
            assert!(in_set.contains(ln));
        }
    }

    #[test]
    fn rank_partition_gives_disjoint_cover() {
        use std::collections::HashSet;

        let records: Vec<String> = (0..400).map(|i| format!("r{i:03}\n")).collect();
        let record_refs: Vec<&str> = records.iter().map(|s| s.as_str()).collect();
        let tf = seekable_file(&record_refs);

        let w = 4u32;
        let mut union: HashSet<String> = HashSet::new();
        let mut total = 0usize;
        for rank in 0..w {
            let mut out = Vec::new();
            let _ = run(
                tf.path(),
                &mut out,
                &Config {
                    seed: 9,
                    partition: Some((rank, w)),
                    ..Default::default()
                },
            )
            .unwrap();
            let text = String::from_utf8(out).unwrap();
            for ln in text.lines() {
                assert!(
                    union.insert(ln.to_string()),
                    "record {ln} appeared in two ranks"
                );
                total += 1;
            }
        }
        assert_eq!(total, 400);
        let expected: HashSet<&str> = records.iter().map(|s| s.trim_end_matches('\n')).collect();
        let got: HashSet<&str> = union.iter().map(|s| s.as_str()).collect();
        assert_eq!(got, expected);
    }
}
