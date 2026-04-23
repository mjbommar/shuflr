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

#[derive(Debug, Clone)]
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

/// Run index-perm over a seekable-zstd input.
pub fn run(path: &Path, sink: impl Write, cfg: &Config) -> Result<(Stats, RunMetrics)> {
    let mut reader = SeekableReader::open(path)?;
    let mut metrics = RunMetrics::default();
    let t_build_start = std::time::Instant::now();
    let (index, scanned) = RecordIndex::build(&mut reader)?;
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

    let mut cache = FrameCache::new(cfg.cache_capacity);
    for &idx in &my_perm {
        let loc = &index.entries[idx as usize];
        let frame = cache.get(&mut reader, loc.frame_id)?;
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
    writer.flush().map_err(Error::Io)?;
    Ok((stats, metrics))
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
