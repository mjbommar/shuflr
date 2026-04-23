//! Post-write verification for zstd-seekable files (004 §4).
//!
//! Reopens a freshly-written seekable file and:
//! 1. Parses the seek-table trailer (exercise of `SeekTable::read_from_tail`).
//! 2. For every frame: decompresses, asserts the decompressed length equals
//!    the seek-table entry's `decompressed_size`, and counts records (`\n`).
//! 3. Returns a [`VerifyReport`] with per-frame totals and any discrepancies.
//!
//! The per-frame XXH64-low-32 checksum is NOT re-verified in v1 because the
//! writer uses an FNV placeholder; swapping in real XXH64 (and enabling
//! checksum re-verification) is a follow-up. Even without it, decompressing
//! every frame catches the vast majority of real corruption: zstd's internal
//! checksums fire on corrupted compressed bytes, and a mismatched
//! `decompressed_size` catches seek-table tampering.

use std::path::Path;

use crate::error::{Error, Result};

use super::reader::SeekableReader;

#[derive(Debug, Clone, Default)]
pub struct VerifyReport {
    pub frames: u64,
    pub total_decompressed: u64,
    pub records: u64,
    pub frame_errors: Vec<String>,
}

impl VerifyReport {
    pub fn ok(&self) -> bool {
        self.frame_errors.is_empty()
    }
}

/// Run a full verification pass over the given seekable file. Returns
/// `Err` on IO or format errors; returns `Ok(report)` with `report.ok() ==
/// false` if individual frames failed their content checks.
pub fn run(path: &Path) -> Result<VerifyReport> {
    let mut reader = SeekableReader::open(path)?;
    let mut report = VerifyReport {
        frames: reader.num_frames() as u64,
        ..Default::default()
    };
    for i in 0..reader.num_frames() {
        match reader.decompress_frame(i) {
            Ok(bytes) => {
                let expected = reader.entries()[i].decompressed_size as usize;
                if bytes.len() != expected {
                    report.frame_errors.push(format!(
                        "frame {i}: decompressed size {} != claimed {}",
                        bytes.len(),
                        expected
                    ));
                }
                report.total_decompressed += bytes.len() as u64;
                report.records += memchr::memchr_iter(b'\n', &bytes).count() as u64;
            }
            Err(e) => {
                report
                    .frame_errors
                    .push(format!("frame {i}: decode failed: {e}"));
            }
        }
    }
    Ok(report)
}

/// Convenience: verify and turn any non-ok report into an `Error::Input`.
pub fn run_strict(path: &Path) -> Result<VerifyReport> {
    let report = run(path)?;
    if !report.ok() {
        return Err(Error::Input(format!(
            "verify failed for {}: {} of {} frames flagged; first: {}",
            path.display(),
            report.frame_errors.len(),
            report.frames,
            report.frame_errors.first().cloned().unwrap_or_default(),
        )));
    }
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::zstd_seekable::writer::{Writer, WriterConfig};

    fn build_seekable(path: &Path, records: &[&[u8]]) {
        let file = std::fs::File::create(path).unwrap();
        let mut w = Writer::new(
            file,
            WriterConfig {
                level: 3,
                frame_size: 64,
                checksums: true,
                record_aligned: true,
            },
        );
        for r in records {
            w.write_block(r).unwrap();
        }
        w.finish().unwrap();
    }

    #[test]
    fn clean_file_verifies_ok() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        build_seekable(
            tmp.path(),
            &[b"one\n", b"two\n", b"three\n", b"four\n", b"five\n"],
        );
        let report = run(tmp.path()).unwrap();
        assert!(report.ok(), "errors: {:?}", report.frame_errors);
        assert_eq!(report.records, 5);
        assert!(report.frames >= 1);
    }

    #[test]
    fn seek_table_corruption_flagged() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        build_seekable(
            tmp.path(),
            &[b"alpha\n", b"bravo\n", b"charlie\n", b"delta\n"],
        );
        // Corrupt the seek-table footer magic — this should fail
        // SeekableReader::open outright.
        let mut bytes = std::fs::read(tmp.path()).unwrap();
        let len = bytes.len();
        bytes[len - 3] ^= 0xff; // flip bits in the trailer magic
        bytes[len - 2] ^= 0xff;
        std::fs::write(tmp.path(), &bytes).unwrap();

        assert!(
            run(tmp.path()).is_err(),
            "corrupted trailer magic must be caught during open"
        );
    }

    #[test]
    fn empty_file_verifies_trivially() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        build_seekable(tmp.path(), &[]);
        let report = run(tmp.path()).unwrap();
        assert!(report.ok());
        assert_eq!(report.frames, 0);
        assert_eq!(report.records, 0);
    }

    #[test]
    fn run_strict_returns_err_on_corruption() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        build_seekable(tmp.path(), &[b"a\n", b"b\n", b"c\n"]);
        let mut bytes = std::fs::read(tmp.path()).unwrap();
        // Deep corruption: stomp the whole first compressed frame body.
        for b in bytes.iter_mut().take(30).skip(10) {
            *b = 0xff;
        }
        std::fs::write(tmp.path(), &bytes).unwrap();
        assert!(run_strict(tmp.path()).is_err());
    }
}
