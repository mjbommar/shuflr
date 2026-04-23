//! `--shuffle=index-perm` on plain JSONL files (002 §2.2).
//!
//! Given an [`IndexFile`] over a seekable input, Fisher-Yates permutes the
//! record indices and emits records by `pread` from the file. This is the
//! only v1 mode that produces a **provably uniform** random permutation;
//! it's the right choice for LLM-training corpora that may carry source-
//! order locality (which chunk-shuffled cannot unbias).
//!
//! Memory: `O(count × 8 bytes)` for the permutation plus `O(record_size)`
//! for the read buffer. RAM scaling per 002 §2.2:
//!
//! ```text
//!   1e7 records →  80 MB
//!   1e9 records →   8 GB
//!  1e10 records →  80 GB (needs a large RAM host)
//! ```
//!
//! Beyond ~10⁹ lines, use `2pass` (deferred to v1.x) or fall back to
//! `chunk-shuffled` on a seekable-zstd conversion.

use std::io::Write;
#[cfg(unix)]
use std::os::unix::fs::FileExt;
use std::path::Path;

use rand::seq::SliceRandom;

use crate::error::{Error, Result};
use crate::framing::Stats;
use crate::index::IndexFile;
use crate::seed::Seed;

/// Configuration for an index-perm run.
#[derive(Debug, Clone)]
pub struct Config {
    pub seed: u64,
    pub epoch: u64,
    pub sample: Option<u64>,
    /// Patch a trailing `\n` on any record that lacks one (shouldn't
    /// happen for a well-formed input, but we tolerate it).
    pub ensure_trailing_newline: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            seed: 0,
            epoch: 0,
            sample: None,
            ensure_trailing_newline: true,
        }
    }
}

/// Run index-perm over `input_path` using `index`. Returns aggregated
/// stats. The input file is reopened here so we don't interfere with any
/// other user of the file handle.
#[cfg(unix)]
pub fn run(input_path: &Path, index: &IndexFile, sink: impl Write, cfg: &Config) -> Result<Stats> {
    let file = std::fs::File::open(input_path).map_err(|e| match e.kind() {
        std::io::ErrorKind::NotFound => Error::NotFound {
            path: input_path.display().to_string(),
        },
        std::io::ErrorKind::PermissionDenied => Error::PermissionDenied {
            path: input_path.display().to_string(),
        },
        _ => Error::Io(e),
    })?;

    let mut writer = std::io::BufWriter::with_capacity(2 * 1024 * 1024, sink);
    let mut stats = Stats::default();
    let count = index.count() as usize;
    if count == 0 {
        writer.flush().map_err(Error::Io)?;
        return Ok(stats);
    }

    // Fisher-Yates over indices.
    let seed = Seed::new(cfg.seed);
    let mut perm: Vec<u32> = (0..count as u32).collect();
    let mut rng = Seed::rng_from(seed.perm(cfg.epoch));
    perm.shuffle(&mut rng);

    // Reused record buffer sized to the largest record in the index; small
    // fixtures keep this cheap, large corpora cap here at max record length.
    let mut max_len = 0u64;
    for i in 0..count {
        max_len = max_len.max(index.record_len(i));
    }
    let mut buf: Vec<u8> = vec![0u8; max_len as usize];

    for &i in &perm {
        let idx = i as usize;
        let (start, end) = index.record_range(idx);
        let len = (end - start) as usize;
        let slice = &mut buf[..len];
        file.read_exact_at(slice, start).map_err(Error::Io)?;
        stats.records_in += 1;
        stats.bytes_in += len as u64;

        let ends_with_nl = slice.last() == Some(&b'\n');
        writer.write_all(slice).map_err(Error::Io)?;
        stats.bytes_out += len as u64;
        if !ends_with_nl && cfg.ensure_trailing_newline {
            writer.write_all(b"\n").map_err(Error::Io)?;
            stats.bytes_out += 1;
        }
        stats.records_out += 1;

        if let Some(cap) = cfg.sample
            && stats.records_out >= cap
        {
            break;
        }
    }

    writer.flush().map_err(Error::Io)?;
    Ok(stats)
}

/// Non-Unix fallback: seek + read. Unix uses `pread` via `FileExt::read_exact_at`.
#[cfg(not(unix))]
pub fn run(input_path: &Path, index: &IndexFile, sink: impl Write, cfg: &Config) -> Result<Stats> {
    use std::io::{Read, Seek, SeekFrom};

    let mut file = std::fs::File::open(input_path).map_err(|e| match e.kind() {
        std::io::ErrorKind::NotFound => Error::NotFound {
            path: input_path.display().to_string(),
        },
        std::io::ErrorKind::PermissionDenied => Error::PermissionDenied {
            path: input_path.display().to_string(),
        },
        _ => Error::Io(e),
    })?;

    let mut writer = std::io::BufWriter::with_capacity(2 * 1024 * 1024, sink);
    let mut stats = Stats::default();
    let count = index.count() as usize;
    if count == 0 {
        writer.flush().map_err(Error::Io)?;
        return Ok(stats);
    }

    let seed = Seed::new(cfg.seed);
    let mut perm: Vec<u32> = (0..count as u32).collect();
    let mut rng = Seed::rng_from(seed.perm(cfg.epoch));
    perm.shuffle(&mut rng);

    let max_len: u64 = (0..count).map(|i| index.record_len(i)).max().unwrap_or(0);
    let mut buf: Vec<u8> = vec![0u8; max_len as usize];

    for &i in &perm {
        let idx = i as usize;
        let (start, end) = index.record_range(idx);
        let len = (end - start) as usize;
        let slice = &mut buf[..len];
        file.seek(SeekFrom::Start(start)).map_err(Error::Io)?;
        file.read_exact(slice).map_err(Error::Io)?;
        stats.records_in += 1;
        stats.bytes_in += len as u64;
        let ends_with_nl = slice.last() == Some(&b'\n');
        writer.write_all(slice).map_err(Error::Io)?;
        stats.bytes_out += len as u64;
        if !ends_with_nl && cfg.ensure_trailing_newline {
            writer.write_all(b"\n").map_err(Error::Io)?;
            stats.bytes_out += 1;
        }
        stats.records_out += 1;
        if let Some(cap) = cfg.sample
            && stats.records_out >= cap
        {
            break;
        }
    }
    writer.flush().map_err(Error::Io)?;
    Ok(stats)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::Fingerprint;
    use std::collections::BTreeSet;

    fn build_file(records: &[&str]) -> (tempfile::NamedTempFile, IndexFile) {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let joined: String = records.concat();
        std::fs::write(tmp.path(), &joined).unwrap();
        let idx = IndexFile::build(joined.as_bytes(), Fingerprint([0; 32])).unwrap();
        (tmp, idx)
    }

    #[test]
    fn is_a_true_permutation_small_input() {
        let records: Vec<String> = (0..25).map(|i| format!("r{i:02}\n")).collect();
        let record_refs: Vec<&str> = records.iter().map(|s| s.as_str()).collect();
        let (f, idx) = build_file(&record_refs);

        let mut out = Vec::new();
        let stats = run(
            f.path(),
            &idx,
            &mut out,
            &Config {
                seed: 7,
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(stats.records_in, 25);
        assert_eq!(stats.records_out, 25);

        let text = String::from_utf8(out).unwrap();
        let out_lines: BTreeSet<&str> = text.lines().collect();
        let in_lines: BTreeSet<&str> = records.iter().map(|s| s.trim_end()).collect();
        assert_eq!(in_lines, out_lines, "multiset preserved");
    }

    #[test]
    fn deterministic_given_seed() {
        let records: Vec<String> = (0..100).map(|i| format!("rec_{i:03}\n")).collect();
        let record_refs: Vec<&str> = records.iter().map(|s| s.as_str()).collect();
        let (f, idx) = build_file(&record_refs);

        let run_seed = |seed: u64| {
            let mut out = Vec::new();
            run(
                f.path(),
                &idx,
                &mut out,
                &Config {
                    seed,
                    ..Default::default()
                },
            )
            .unwrap();
            out
        };

        let a = run_seed(42);
        let b = run_seed(42);
        let c = run_seed(43);
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn sample_caps_output() {
        let records: Vec<String> = (0..50).map(|i| format!("{i}\n")).collect();
        let record_refs: Vec<&str> = records.iter().map(|s| s.as_str()).collect();
        let (f, idx) = build_file(&record_refs);

        let mut out = Vec::new();
        let stats = run(
            f.path(),
            &idx,
            &mut out,
            &Config {
                seed: 3,
                sample: Some(7),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(stats.records_out, 7);
        assert_eq!(String::from_utf8(out).unwrap().lines().count(), 7);
    }

    #[test]
    fn different_epochs_yield_different_orders() {
        let records: Vec<String> = (0..200).map(|i| format!("r_{i:03}\n")).collect();
        let record_refs: Vec<&str> = records.iter().map(|s| s.as_str()).collect();
        let (f, idx) = build_file(&record_refs);
        let go = |epoch: u64| {
            let mut out = Vec::new();
            run(
                f.path(),
                &idx,
                &mut out,
                &Config {
                    seed: 9,
                    epoch,
                    ..Default::default()
                },
            )
            .unwrap();
            out
        };
        assert_ne!(go(0), go(1));
        assert_eq!(go(5), go(5));
    }

    #[test]
    fn empty_file_emits_nothing() {
        let f = tempfile::NamedTempFile::new().unwrap();
        let idx = IndexFile::build(&[][..], Fingerprint([0; 32])).unwrap();
        let mut out = Vec::new();
        let stats = run(f.path(), &idx, &mut out, &Config::default()).unwrap();
        assert_eq!(stats.records_out, 0);
        assert!(out.is_empty());
    }
}
