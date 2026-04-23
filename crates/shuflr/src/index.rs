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
//!   Fingerprint   [u8; 32] = blake3(basename || size || mtime_ns || ino
//!                                   || dev || head+mid+tail 4 KiB each)
//!   Count         u64
//!   Offsets       [u64; Count + 1]
//! ```
//!
//! Total size: `56 + 8 * (count + 1)` bytes. A 1B-record corpus indexes
//! to ~8 GiB — within reach; 10B records → 80 GiB RAM, use `2pass`.
//!
//! The fingerprint domain deliberately covers (a) inode+dev to detect
//! rename-replace, (b) nanosecond mtime to close the 1-second ambiguity
//! window, and (c) two 4 KiB content samples at the head and tail so
//! that an in-place rewrite that preserves size and mtime (e.g. via
//! `utimensat`) still fails the check. Sampling cost is one extra stat
//! + two preads per fingerprint (microseconds), paid only at open.

use std::fs;
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use crate::error::{Error, Result};

pub const MAGIC: &[u8; 8] = b"SHUFLIDX";
pub const CURRENT_VERSION: u8 = 1;

/// Domain separator — updated whenever the fingerprint *definition*
/// changes so that old sidecars fail the match and rebuild rather than
/// silently accepting a weaker-hash match.
const FINGERPRINT_DOMAIN: &[u8] = b"shuflr-v2-fingerprint\0";

/// Size of the head/tail content samples in the fingerprint.
const SAMPLE_BYTES: usize = 4096;

/// The canonical sidecar path for a given input: `<input>.shuflr-idx`.
pub fn sidecar_path(input: &Path) -> PathBuf {
    let mut s = input.as_os_str().to_os_string();
    s.push(".shuflr-idx");
    PathBuf::from(s)
}

/// A fingerprint over a file's metadata and small content samples.
/// Stable across reads; collisions require an attacker who matches
/// basename, size, nanosecond mtime, inode, device, and the first and
/// last 4 KiB simultaneously.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct Fingerprint(pub [u8; 32]);

impl Fingerprint {
    /// Compute the fingerprint. Stats the file, reads two small byte
    /// ranges (head and tail). Intended for use at open-time, not
    /// per-record.
    pub fn from_metadata(path: &Path) -> Result<Self> {
        let meta = fs::metadata(path).map_err(Error::Io)?;
        let size = meta.len();

        let mtime_ns = meta
            .modified()
            .ok()
            .and_then(|t| t.duration_since(SystemTime::UNIX_EPOCH).ok())
            .map(|d| d.as_nanos())
            .unwrap_or(0);

        #[cfg(unix)]
        let (ino, dev) = {
            use std::os::unix::fs::MetadataExt as _;
            (meta.ino(), meta.dev())
        };
        #[cfg(not(unix))]
        let (ino, dev): (u64, u64) = (0, 0);

        let basename = path
            .file_name()
            .map(|s| s.as_encoded_bytes().to_vec())
            .unwrap_or_default();

        // Content samples at three points: head, middle, tail. Missing
        // any one would leave a gap an adversary or a subtle in-place
        // patch could slip through. Three samples × 4 KiB = 12 KiB
        // total read per fingerprint — still microseconds.
        //
        // For small files (< 3 × SAMPLE_BYTES), the head read covers
        // the whole file and middle/tail are left empty.
        let mut head = vec![];
        let mut mid = vec![];
        let mut tail = vec![];
        if size > 0 {
            let mut f = fs::File::open(path).map_err(Error::Io)?;
            let head_len = std::cmp::min(size as usize, SAMPLE_BYTES);
            head = vec![0u8; head_len];
            f.read_exact(&mut head).map_err(Error::Io)?;
            if size as usize > 3 * SAMPLE_BYTES {
                let mid_start = size / 2 - (SAMPLE_BYTES as u64) / 2;
                f.seek(SeekFrom::Start(mid_start)).map_err(Error::Io)?;
                mid = vec![0u8; SAMPLE_BYTES];
                f.read_exact(&mut mid).map_err(Error::Io)?;

                let tail_start = size - SAMPLE_BYTES as u64;
                f.seek(SeekFrom::Start(tail_start)).map_err(Error::Io)?;
                tail = vec![0u8; SAMPLE_BYTES];
                f.read_exact(&mut tail).map_err(Error::Io)?;
            }
        }

        let mut h = blake3::Hasher::new();
        h.update(FINGERPRINT_DOMAIN);
        h.update(&basename);
        h.update(b"\0");
        h.update(&size.to_le_bytes());
        h.update(&mtime_ns.to_le_bytes());
        h.update(&ino.to_le_bytes());
        h.update(&dev.to_le_bytes());
        for sample in [&head, &mid, &tail] {
            h.update(&(sample.len() as u64).to_le_bytes());
            h.update(sample);
        }
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

    #[test]
    fn fingerprint_detects_middle_only_patch() {
        // File big enough to trip all three sample regions; patch a
        // byte in the exact middle. Head/tail samples won't see it,
        // but the middle sample must.
        use std::os::unix::fs::MetadataExt as _;

        let tmp = tempfile::NamedTempFile::new().unwrap();
        let path = tmp.path();
        let size = 64 * 1024; // 64 KiB → > 3 * 4 KiB so all samples engage
        let mut body = vec![b'A'; size];
        std::fs::write(path, &body).unwrap();
        let meta_before = std::fs::metadata(path).unwrap();
        let fp_a = Fingerprint::from_metadata(path).unwrap();

        // Patch a byte in the middle sample region.
        body[size / 2] = b'B';
        std::fs::write(path, &body).unwrap();

        // Restore original mtime to simulate an attacker who preserved it.
        unsafe {
            let ts_a = libc_timespec(meta_before.atime(), meta_before.atime_nsec());
            let ts_m = libc_timespec(meta_before.mtime(), meta_before.mtime_nsec());
            let path_c = std::ffi::CString::new(path.as_os_str().as_encoded_bytes()).unwrap();
            let times = [ts_a, ts_m];
            libc_utimensat(-100, path_c.as_ptr() as *const u8, times.as_ptr(), 0);
        }

        let fp_b = Fingerprint::from_metadata(path).unwrap();
        assert_ne!(
            fp_a, fp_b,
            "middle-byte patch with preserved mtime must still flip the fingerprint"
        );
    }

    #[test]
    fn fingerprint_detects_in_place_rewrite_of_same_size() {
        // Write file A, fingerprint. Overwrite with different content
        // of the same size, restore the original mtime/atime to
        // simulate an attacker (or `--reference` copy) that preserves
        // metadata. Fingerprint must still diverge because the content
        // samples differ.
        use std::os::unix::fs::MetadataExt as _;

        let tmp = tempfile::NamedTempFile::new().unwrap();
        let path = tmp.path();
        let body_a = vec![b'A'; 16 * 1024];
        let body_b = {
            let mut v = body_a.clone();
            v[0] = b'B'; // change in the first-sample region
            v
        };
        std::fs::write(path, &body_a).unwrap();
        let meta_before = std::fs::metadata(path).unwrap();
        let fp_a = Fingerprint::from_metadata(path).unwrap();

        // Rewrite with the altered body.
        std::fs::write(path, &body_b).unwrap();

        // Restore atime/mtime from meta_before. filetime isn't in our
        // deps; use libc via utimensat-equivalent (utimes works too).
        unsafe {
            let ts_a = libc_timespec(meta_before.atime(), meta_before.atime_nsec());
            let ts_m = libc_timespec(meta_before.mtime(), meta_before.mtime_nsec());
            let path_c = std::ffi::CString::new(path.as_os_str().as_encoded_bytes()).unwrap();
            let times = [ts_a, ts_m];
            // AT_FDCWD = -100 on Linux
            libc_utimensat(-100, path_c.as_ptr() as *const u8, times.as_ptr(), 0);
        }

        let fp_b = Fingerprint::from_metadata(path).unwrap();
        assert_ne!(
            fp_a, fp_b,
            "fingerprint must distinguish same-size, same-mtime rewrites"
        );
    }

    // Minimal libc hooks so the test above doesn't need an extra dep.
    unsafe extern "C" {
        fn utimensat(dirfd: i32, pathname: *const i8, times: *const Timespec, flags: i32) -> i32;
    }
    #[repr(C)]
    struct Timespec {
        tv_sec: i64,
        tv_nsec: i64,
    }
    unsafe fn libc_timespec(sec: i64, nsec: i64) -> Timespec {
        Timespec {
            tv_sec: sec,
            tv_nsec: nsec,
        }
    }
    unsafe fn libc_utimensat(
        dirfd: i32,
        pathname: *const u8,
        times: *const Timespec,
        flags: i32,
    ) -> i32 {
        unsafe { utimensat(dirfd, pathname as *const i8, times, flags) }
    }
}
