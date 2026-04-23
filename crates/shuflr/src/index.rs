//! `.shuflr-idx` byte-offset index for `--shuffle=index-perm` (002 §2.2).
//!
//! The index stores `count + 1` u64 offsets over an input file. Record `i`
//! occupies byte range `[offsets[i], offsets[i+1])` in the source; the
//! sentinel at `offsets[count]` equals the file size, so record lengths are
//! implicit. Persisted atomically as `<input>.shuflr-idx` so subsequent
//! `index-perm` runs over the same corpus start immediately.
//!
//! Wire format (little-endian unless noted):
//!
//! ```text
//!   Magic         [u8; 8]  = b"SHUFLIDX"
//!   Version       u8       = 1
//!   Reserved      [u8; 7]  = 0   (alignment padding; future flags)
//!   Fingerprint   [u8; 32] = blake3(path_basename || size || mtime_secs)
//!   Count         u64
//!   Offsets       [u64; Count + 1]
//! ```
//!
//! Total size: `56 + 8 * (count + 1)` bytes. A 1B-record corpus indexes
//! to ~8 GiB — within reach; 10B records → 80 GiB RAM, use `2pass`.

use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use crate::error::{Error, Result};

pub const MAGIC: &[u8; 8] = b"SHUFLIDX";
pub const CURRENT_VERSION: u8 = 1;

/// The canonical sidecar path for a given input: `<input>.shuflr-idx`.
pub fn sidecar_path(input: &Path) -> PathBuf {
    let mut s = input.as_os_str().to_os_string();
    s.push(".shuflr-idx");
    PathBuf::from(s)
}

/// A fingerprint derived from inexpensive filesystem metadata. Deliberately
/// cheap so we can re-verify on every open; for content-hash strictness use
/// `Fingerprint::from_content`.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct Fingerprint(pub [u8; 32]);

impl Fingerprint {
    pub fn from_metadata(path: &Path) -> Result<Self> {
        let meta = fs::metadata(path).map_err(Error::Io)?;
        let size = meta.len();
        let mtime_secs = meta
            .modified()
            .ok()
            .and_then(|t| t.duration_since(SystemTime::UNIX_EPOCH).ok())
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let basename = path
            .file_name()
            .map(|s| s.as_encoded_bytes().to_vec())
            .unwrap_or_default();

        let mut h = blake3::Hasher::new();
        h.update(b"shuflr-v1-fingerprint\0");
        h.update(&basename);
        h.update(b"\0");
        h.update(&size.to_le_bytes());
        h.update(&mtime_secs.to_le_bytes());
        Ok(Self(*h.finalize().as_bytes()))
    }
}

/// An in-memory byte-offset index over a JSONL input.
#[derive(Debug)]
pub struct IndexFile {
    pub version: u8,
    pub fingerprint: Fingerprint,
    /// `count + 1` entries; `offsets[i..i+1]` bounds record `i`.
    pub offsets: Vec<u64>,
}

impl IndexFile {
    pub fn count(&self) -> u64 {
        (self.offsets.len().saturating_sub(1)) as u64
    }

    /// Byte range `[start, end)` of record `i`.
    pub fn record_range(&self, i: usize) -> (u64, u64) {
        (self.offsets[i], self.offsets[i + 1])
    }

    pub fn record_len(&self, i: usize) -> u64 {
        self.offsets[i + 1] - self.offsets[i]
    }

    /// Scan `reader` sequentially, recording byte offsets of every record
    /// start. Uses `BufRead::read_until` so it's a single pass over the
    /// input with a reused line buffer.
    pub fn build<R: Read>(reader: R, fingerprint: Fingerprint) -> Result<Self> {
        let mut buf: Vec<u8> = Vec::with_capacity(8 * 1024);
        let mut offsets: Vec<u64> = Vec::new();
        let mut cursor: u64 = 0;
        let mut reader = BufReader::with_capacity(2 * 1024 * 1024, reader);
        offsets.push(0);
        loop {
            buf.clear();
            let n = reader.read_until(b'\n', &mut buf).map_err(Error::Io)?;
            if n == 0 {
                break;
            }
            cursor += n as u64;
            offsets.push(cursor);
        }
        Ok(Self {
            version: CURRENT_VERSION,
            fingerprint,
            offsets,
        })
    }

    /// Serialize to `out`. Call `save` to do the atomic file write.
    pub fn write_to(&self, mut out: impl Write) -> Result<()> {
        out.write_all(MAGIC).map_err(Error::Io)?;
        out.write_all(&[self.version]).map_err(Error::Io)?;
        out.write_all(&[0u8; 7]).map_err(Error::Io)?; // reserved padding
        out.write_all(&self.fingerprint.0).map_err(Error::Io)?;
        out.write_all(&self.count().to_le_bytes())
            .map_err(Error::Io)?;
        for off in &self.offsets {
            out.write_all(&off.to_le_bytes()).map_err(Error::Io)?;
        }
        Ok(())
    }

    /// Deserialize from `r`.
    pub fn read_from(mut r: impl Read) -> Result<Self> {
        let mut magic = [0u8; 8];
        r.read_exact(&mut magic).map_err(Error::Io)?;
        if &magic != MAGIC {
            return Err(Error::Input(format!(
                "not a shuflr index (magic {magic:?} != {MAGIC:?})"
            )));
        }
        let mut version_buf = [0u8; 1];
        r.read_exact(&mut version_buf).map_err(Error::Io)?;
        if version_buf[0] != CURRENT_VERSION {
            return Err(Error::Input(format!(
                "unsupported shuflr-idx version {} (this build expects {CURRENT_VERSION})",
                version_buf[0]
            )));
        }
        let mut reserved = [0u8; 7];
        r.read_exact(&mut reserved).map_err(Error::Io)?;

        let mut fp = [0u8; 32];
        r.read_exact(&mut fp).map_err(Error::Io)?;
        let mut count_buf = [0u8; 8];
        r.read_exact(&mut count_buf).map_err(Error::Io)?;
        let count = u64::from_le_bytes(count_buf);

        let entries = (count as usize) + 1;
        let mut offsets = Vec::with_capacity(entries);
        let mut off_buf = [0u8; 8];
        for _ in 0..entries {
            r.read_exact(&mut off_buf).map_err(Error::Io)?;
            offsets.push(u64::from_le_bytes(off_buf));
        }
        Ok(Self {
            version: version_buf[0],
            fingerprint: Fingerprint(fp),
            offsets,
        })
    }

    /// Atomic save: write to `path.tmp`, fsync, rename onto `path`.
    pub fn save(&self, path: &Path) -> Result<()> {
        let tmp = {
            let mut s = path.as_os_str().to_os_string();
            s.push(".tmp");
            PathBuf::from(s)
        };
        {
            let mut f = fs::File::create(&tmp).map_err(Error::Io)?;
            self.write_to(&mut f)?;
            f.sync_all().map_err(Error::Io)?;
        }
        fs::rename(&tmp, path).map_err(Error::Io)?;
        Ok(())
    }

    /// Load from path. Verifies magic and version; does NOT verify the
    /// fingerprint (caller decides whether to trust it).
    pub fn load(path: &Path) -> Result<Self> {
        let f = fs::File::open(path).map_err(Error::Io)?;
        Self::read_from(BufReader::with_capacity(2 * 1024 * 1024, f))
    }

    /// Error cleanly if the recorded fingerprint doesn't match `expected`.
    pub fn verify_fingerprint(&self, expected: Fingerprint) -> Result<()> {
        if self.fingerprint == expected {
            Ok(())
        } else {
            Err(Error::InputChanged(
                "index fingerprint mismatches current file; rebuild with `shuflr index`".into(),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_tracks_record_boundaries() {
        let text = b"alpha\nbravo\ncharlie\ndelta\n";
        let idx = IndexFile::build(&text[..], Fingerprint([0; 32])).unwrap();
        assert_eq!(idx.count(), 4);
        assert_eq!(idx.record_range(0), (0, 6));
        assert_eq!(idx.record_range(1), (6, 12));
        assert_eq!(idx.record_range(2), (12, 20));
        assert_eq!(idx.record_range(3), (20, 26));
    }

    #[test]
    fn build_handles_missing_trailing_newline() {
        let text = b"first\nsecond";
        let idx = IndexFile::build(&text[..], Fingerprint([0; 32])).unwrap();
        assert_eq!(idx.count(), 2);
        assert_eq!(idx.record_range(1), (6, 12));
    }

    #[test]
    fn empty_input_zero_records() {
        let idx = IndexFile::build(&b""[..], Fingerprint([0; 32])).unwrap();
        assert_eq!(idx.count(), 0);
        assert_eq!(idx.offsets, vec![0]);
    }

    #[test]
    fn write_read_roundtrip() {
        let idx = IndexFile::build(&b"a\nb\nc\nd\n"[..], Fingerprint([0xab; 32])).unwrap();
        let mut buf = Vec::new();
        idx.write_to(&mut buf).unwrap();
        let parsed = IndexFile::read_from(&buf[..]).unwrap();
        assert_eq!(parsed.count(), idx.count());
        assert_eq!(parsed.offsets, idx.offsets);
        assert_eq!(parsed.fingerprint, idx.fingerprint);
        assert_eq!(parsed.version, CURRENT_VERSION);
    }

    #[test]
    fn wrong_magic_rejected() {
        let bad = b"NOT_SHUF\x01";
        let err = IndexFile::read_from(&bad[..]).unwrap_err();
        assert!(matches!(err, Error::Input(_)));
    }

    #[test]
    fn wrong_version_rejected() {
        let mut bad = Vec::new();
        bad.extend_from_slice(MAGIC);
        bad.push(99);
        bad.extend_from_slice(&[0u8; 7]);
        bad.extend_from_slice(&[0u8; 32]);
        bad.extend_from_slice(&0u64.to_le_bytes());
        bad.extend_from_slice(&0u64.to_le_bytes());
        let err = IndexFile::read_from(&bad[..]).unwrap_err();
        assert!(matches!(err, Error::Input(_)));
    }

    #[test]
    fn save_and_load_atomic() {
        let tmp = tempfile::tempdir().unwrap();
        let idx_path = tmp.path().join("data.shuflr-idx");
        let idx = IndexFile::build(&b"x\ny\nz\n"[..], Fingerprint([7; 32])).unwrap();
        idx.save(&idx_path).unwrap();
        let loaded = IndexFile::load(&idx_path).unwrap();
        assert_eq!(loaded.offsets, idx.offsets);
        assert_eq!(loaded.fingerprint, idx.fingerprint);
    }

    #[test]
    fn fingerprint_metadata_is_stable_on_unchanged_file() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(tmp.path(), b"hello\n").unwrap();
        let a = Fingerprint::from_metadata(tmp.path()).unwrap();
        let b = Fingerprint::from_metadata(tmp.path()).unwrap();
        assert_eq!(a, b);
    }

    #[test]
    fn sidecar_path_is_input_plus_suffix() {
        let p = Path::new("/tmp/foo.jsonl");
        assert_eq!(sidecar_path(p), PathBuf::from("/tmp/foo.jsonl.shuflr-idx"));
    }
}
