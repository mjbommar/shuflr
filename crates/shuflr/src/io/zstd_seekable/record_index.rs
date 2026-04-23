//! Per-record index over a seekable-zstd file.
//!
//! Each record in the file is identified by `(frame_id, offset_in_frame,
//! length)` — enough to locate it in one pread + one frame-decode. Built
//! by decoding every frame once and scanning for record boundaries.
//! Persisted to a `.shuflr-idx-zst` sidecar so repeat `index-perm` runs
//! skip the full-file scan (which is the dominant cost on a large
//! corpus: ~126 s on the 30 GB EDGAR seekable per bench/001).
//!
//! Wire format:
//!
//! ```text
//!   Magic         [u8; 8]  = b"SHUFLRZI"
//!   Version       u8       = 1
//!   Reserved      [u8; 7]  = 0
//!   Fingerprint   [u8; 32] = blake3(basename ‖ size ‖ mtime_secs)
//!   Count         u64
//!   Entries       [(u32 frame_id, u32 offset_in_frame, u32 length); Count]
//! ```
//!
//! Size: 56 + 12·N bytes. A 1.2M-record EDGAR corpus → ~14 MiB sidecar.

use std::fs;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use super::reader::SeekableReader;
use crate::error::{Error, Result};
use crate::index::Fingerprint;

pub const MAGIC: &[u8; 8] = b"SHUFLRZI";
pub const CURRENT_VERSION: u8 = 1;

/// Canonical sidecar path: `<input>.shuflr-idx-zst`.
pub fn sidecar_path(input: &Path) -> PathBuf {
    let mut s = input.as_os_str().to_os_string();
    s.push(".shuflr-idx-zst");
    PathBuf::from(s)
}

/// Location of one record inside a seekable-zstd file.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct RecordLocation {
    pub frame_id: u32,
    pub offset_in_frame: u32,
    /// Length in bytes, including any trailing `\n`.
    pub length: u32,
}

/// In-memory record index. `fingerprint` is populated when the index
/// was loaded from (or saved to) a sidecar; `None` for an index that
/// was just built and hasn't been persisted.
#[derive(Debug, Default)]
pub struct RecordIndex {
    pub entries: Vec<RecordLocation>,
    pub fingerprint: Option<Fingerprint>,
}

impl RecordIndex {
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Build by decoding every frame sequentially and recording each
    /// record's location. Returns the total decompressed bytes scanned,
    /// so callers can log throughput.
    pub fn build(reader: &mut SeekableReader) -> Result<(Self, u64)> {
        let n_frames = reader.num_frames();
        let mut entries = Vec::new();
        let mut total_bytes: u64 = 0;
        for frame_id in 0..n_frames {
            let bytes = reader.decompress_frame(frame_id)?;
            total_bytes += bytes.len() as u64;
            let fid = u32::try_from(frame_id).map_err(|_| {
                Error::Input(format!(
                    "frame_id {frame_id} exceeds u32; files beyond 4B frames are unsupported"
                ))
            })?;
            // Scan for record boundaries. Each `\n` ends a record.
            let mut start: usize = 0;
            for nl in memchr::memchr_iter(b'\n', &bytes) {
                let end = nl + 1;
                let length = u32::try_from(end - start).map_err(|_| {
                    Error::Input(format!(
                        "frame {frame_id} has a record larger than 4 GiB; unsupported"
                    ))
                })?;
                let off = u32::try_from(start).map_err(|_| {
                    Error::Input(format!(
                        "frame {frame_id} offset {start} exceeds u32; frame too large"
                    ))
                })?;
                entries.push(RecordLocation {
                    frame_id: fid,
                    offset_in_frame: off,
                    length,
                });
                start = end;
            }
            // Trailing partial (no trailing \n in this frame) — treat as
            // a record. Writer invariant says every frame ends on \n,
            // but we tolerate non-conforming input.
            if start < bytes.len() {
                let length = (bytes.len() - start) as u32;
                let off = start as u32;
                entries.push(RecordLocation {
                    frame_id: fid,
                    offset_in_frame: off,
                    length,
                });
            }
        }
        Ok((
            Self {
                entries,
                fingerprint: None,
            },
            total_bytes,
        ))
    }

    /// Serialize to `out` in the wire format above.
    pub fn write_to(&self, mut out: impl Write, fingerprint: Fingerprint) -> Result<()> {
        out.write_all(MAGIC).map_err(Error::Io)?;
        out.write_all(&[CURRENT_VERSION]).map_err(Error::Io)?;
        out.write_all(&[0u8; 7]).map_err(Error::Io)?;
        out.write_all(&fingerprint.0).map_err(Error::Io)?;
        out.write_all(&(self.entries.len() as u64).to_le_bytes())
            .map_err(Error::Io)?;
        for e in &self.entries {
            out.write_all(&e.frame_id.to_le_bytes())
                .map_err(Error::Io)?;
            out.write_all(&e.offset_in_frame.to_le_bytes())
                .map_err(Error::Io)?;
            out.write_all(&e.length.to_le_bytes()).map_err(Error::Io)?;
        }
        Ok(())
    }

    /// Atomic save: write to `path.tmp`, fsync, rename onto `path`.
    pub fn save(&self, path: &Path, fingerprint: Fingerprint) -> Result<()> {
        let tmp = {
            let mut s = path.as_os_str().to_os_string();
            s.push(".tmp");
            PathBuf::from(s)
        };
        {
            let mut f = fs::File::create(&tmp).map_err(Error::Io)?;
            self.write_to(&mut f, fingerprint)?;
            f.sync_all().map_err(Error::Io)?;
        }
        fs::rename(&tmp, path).map_err(Error::Io)?;
        Ok(())
    }

    /// Deserialize from reader.
    pub fn read_from(mut r: impl Read) -> Result<Self> {
        let mut magic = [0u8; 8];
        r.read_exact(&mut magic).map_err(Error::Io)?;
        if &magic != MAGIC {
            return Err(Error::Input(format!(
                "not a shuflr seekable-zstd index (magic {magic:?} != {MAGIC:?})"
            )));
        }
        let mut version = [0u8; 1];
        r.read_exact(&mut version).map_err(Error::Io)?;
        if version[0] != CURRENT_VERSION {
            return Err(Error::Input(format!(
                "unsupported seekable-zstd index version {} (expected {CURRENT_VERSION})",
                version[0]
            )));
        }
        let mut reserved = [0u8; 7];
        r.read_exact(&mut reserved).map_err(Error::Io)?;
        let mut fp = [0u8; 32];
        r.read_exact(&mut fp).map_err(Error::Io)?;
        let mut count_buf = [0u8; 8];
        r.read_exact(&mut count_buf).map_err(Error::Io)?;
        let count = u64::from_le_bytes(count_buf) as usize;
        let mut entries = Vec::with_capacity(count);
        let mut buf = [0u8; 12];
        for _ in 0..count {
            r.read_exact(&mut buf).map_err(Error::Io)?;
            let frame_id = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]);
            let offset_in_frame = u32::from_le_bytes([buf[4], buf[5], buf[6], buf[7]]);
            let length = u32::from_le_bytes([buf[8], buf[9], buf[10], buf[11]]);
            entries.push(RecordLocation {
                frame_id,
                offset_in_frame,
                length,
            });
        }
        Ok(Self {
            entries,
            fingerprint: Some(Fingerprint(fp)),
        })
    }

    pub fn load(path: &Path) -> Result<Self> {
        let f = fs::File::open(path).map_err(Error::Io)?;
        Self::read_from(std::io::BufReader::with_capacity(2 * 1024 * 1024, f))
    }
}

/// A small LRU cache of decoded frames. Each entry holds one frame's
/// decompressed bytes keyed by `frame_id`. Capacity is measured in
/// frames, not bytes — caller should pick a value such that
/// `capacity × avg_frame_size` fits the memory budget.
pub struct FrameCache {
    capacity: usize,
    slots: Vec<Option<CacheSlot>>,
    /// Frame-id → slot index for O(1) lookup.
    index: std::collections::HashMap<u32, usize>,
    /// Monotonic tick assigned to each access; lowest tick = LRU.
    tick: u64,
    pub hits: u64,
    pub misses: u64,
}

struct CacheSlot {
    frame_id: u32,
    bytes: Vec<u8>,
    last_access: u64,
}

impl FrameCache {
    pub fn new(capacity: usize) -> Self {
        let slots = (0..capacity.max(1)).map(|_| None).collect();
        Self {
            capacity: capacity.max(1),
            slots,
            index: std::collections::HashMap::with_capacity(capacity.max(1)),
            tick: 0,
            hits: 0,
            misses: 0,
        }
    }

    /// Get the decoded bytes for `frame_id`, decoding through `reader`
    /// on miss and evicting the LRU slot if the cache is full.
    pub fn get<'s>(&'s mut self, reader: &mut SeekableReader, frame_id: u32) -> Result<&'s [u8]> {
        self.tick = self.tick.wrapping_add(1);
        // Fast path: hit. Update access tick and return.
        if let Some(&slot_idx) = self.index.get(&frame_id) {
            if let Some(slot) = self.slots[slot_idx].as_mut() {
                self.hits += 1;
                slot.last_access = self.tick;
                // Re-borrow immutably to satisfy the lifetime of the returned slice.
                return Ok(match &self.slots[slot_idx] {
                    Some(s) => &s.bytes,
                    None => unreachable!("slot just updated is Some"),
                });
            }
        }
        self.misses += 1;
        let bytes = reader.decompress_frame(frame_id as usize)?;

        // Find an empty slot or evict LRU.
        let slot_idx = if let Some(empty) = self.slots.iter().position(Option::is_none) {
            empty
        } else {
            let victim = self
                .slots
                .iter()
                .enumerate()
                .min_by_key(|(_, s)| s.as_ref().map(|s| s.last_access).unwrap_or(u64::MAX))
                .map(|(i, _)| i)
                .unwrap_or(0);
            if let Some(old) = self.slots[victim].take() {
                self.index.remove(&old.frame_id);
            }
            victim
        };
        self.slots[slot_idx] = Some(CacheSlot {
            frame_id,
            bytes,
            last_access: self.tick,
        });
        self.index.insert(frame_id, slot_idx);
        Ok(match &self.slots[slot_idx] {
            Some(s) => &s.bytes,
            None => unreachable!("slot we just populated is Some"),
        })
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn hit_rate(&self) -> f64 {
        let total = self.hits + self.misses;
        if total == 0 {
            0.0
        } else {
            self.hits as f64 / total as f64
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::zstd_seekable::writer::{Writer, WriterConfig};

    fn build_fixture(records: &[&[u8]]) -> tempfile::NamedTempFile {
        let tf = tempfile::NamedTempFile::new().unwrap();
        let file = std::fs::File::create(tf.path()).unwrap();
        let mut w = Writer::new(
            file,
            WriterConfig {
                level: 3,
                frame_size: 64, // small so we get multiple frames
                checksums: true,
                record_aligned: true,
            },
        );
        for r in records {
            w.write_block(r).unwrap();
        }
        w.finish().unwrap();
        tf
    }

    #[test]
    fn index_covers_every_record() {
        let records: Vec<String> = (0..200).map(|i| format!("rec_{i:03}\n")).collect();
        let record_refs: Vec<&[u8]> = records.iter().map(|s| s.as_bytes()).collect();
        let tf = build_fixture(&record_refs);

        let mut reader = SeekableReader::open(tf.path()).unwrap();
        let (idx, scanned) = RecordIndex::build(&mut reader).unwrap();
        assert_eq!(idx.len(), 200);
        assert!(scanned > 0);
        // Every entry points into a valid frame.
        for e in &idx.entries {
            assert!((e.frame_id as usize) < reader.num_frames());
            assert!(e.length > 0);
        }
    }

    #[test]
    fn cache_hits_repeat_accesses() {
        let records: Vec<String> = (0..50).map(|i| format!("rec_{i:03}\n")).collect();
        let record_refs: Vec<&[u8]> = records.iter().map(|s| s.as_bytes()).collect();
        let tf = build_fixture(&record_refs);

        let mut reader = SeekableReader::open(tf.path()).unwrap();
        let mut cache = FrameCache::new(4);
        for _ in 0..10 {
            let _ = cache.get(&mut reader, 0).unwrap();
        }
        assert!(cache.hits >= 9, "expected cache hits on repeat access");
        assert_eq!(cache.misses, 1);
    }

    #[test]
    fn cache_evicts_lru_when_full() {
        let records: Vec<String> = (0..500).map(|i| format!("r{i:04}\n")).collect();
        let record_refs: Vec<&[u8]> = records.iter().map(|s| s.as_bytes()).collect();
        let tf = build_fixture(&record_refs);

        let mut reader = SeekableReader::open(tf.path()).unwrap();
        assert!(reader.num_frames() >= 5, "need enough frames for this test");
        let mut cache = FrameCache::new(2);
        // Sequentially touch frames 0, 1, 2, 3, 0 — with cap=2, frame 0
        // should have been evicted by the time we come back.
        let _ = cache.get(&mut reader, 0).unwrap();
        let _ = cache.get(&mut reader, 1).unwrap();
        let _ = cache.get(&mut reader, 2).unwrap();
        let _ = cache.get(&mut reader, 3).unwrap();
        let before = cache.misses;
        let _ = cache.get(&mut reader, 0).unwrap();
        assert_eq!(cache.misses, before + 1, "frame 0 should have been evicted");
    }

    #[test]
    fn sidecar_roundtrip_preserves_entries() {
        let records: Vec<String> = (0..75).map(|i| format!("r{i:02}\n")).collect();
        let record_refs: Vec<&[u8]> = records.iter().map(|s| s.as_bytes()).collect();
        let tf = build_fixture(&record_refs);

        let mut reader = SeekableReader::open(tf.path()).unwrap();
        let (idx, _) = RecordIndex::build(&mut reader).unwrap();

        let sidecar = tempfile::NamedTempFile::new().unwrap();
        let fp = Fingerprint([0xcd; 32]);
        idx.save(sidecar.path(), fp).unwrap();

        let loaded = RecordIndex::load(sidecar.path()).unwrap();
        assert_eq!(loaded.entries, idx.entries);
        assert_eq!(loaded.fingerprint, Some(fp));
    }

    #[test]
    fn sidecar_rejects_wrong_magic() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(tmp.path(), b"WRONG_MAGIC_data").unwrap();
        assert!(RecordIndex::load(tmp.path()).is_err());
    }

    #[test]
    fn sidecar_path_uses_expected_suffix() {
        let p = Path::new("/tmp/x.jsonl.zst");
        assert_eq!(
            sidecar_path(p),
            PathBuf::from("/tmp/x.jsonl.zst.shuflr-idx-zst")
        );
    }

    #[test]
    fn emit_all_records_by_index_matches_sequential_decode() {
        use std::collections::BTreeSet;

        let records: Vec<String> = (0..200).map(|i| format!("r{i:03}\n")).collect();
        let record_refs: Vec<&[u8]> = records.iter().map(|s| s.as_bytes()).collect();
        let tf = build_fixture(&record_refs);

        let mut reader = SeekableReader::open(tf.path()).unwrap();
        let (idx, _) = RecordIndex::build(&mut reader).unwrap();
        let mut cache = FrameCache::new(4);
        let mut emitted: BTreeSet<String> = BTreeSet::new();
        for e in &idx.entries {
            let frame = cache.get(&mut reader, e.frame_id).unwrap();
            let start = e.offset_in_frame as usize;
            let end = start + e.length as usize;
            let bytes = &frame[start..end];
            let s = std::str::from_utf8(bytes).unwrap().trim_end_matches('\n');
            emitted.insert(s.to_string());
        }
        let expected: BTreeSet<String> = records
            .iter()
            .map(|s| s.trim_end_matches('\n').to_string())
            .collect();
        assert_eq!(emitted, expected);
    }
}
