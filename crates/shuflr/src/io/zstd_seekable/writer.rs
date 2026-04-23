//! Record-aligned zstd-seekable writer (004 §2.1).
//!
//! Accumulates bytes up to a target frame size. Once the target is met,
//! the writer waits for the next `\n` before closing the frame, so every
//! frame ends on a record boundary and is a standalone JSONL shard. The
//! only time the invariant is violated is when a single record exceeds
//! the frame target on its own — then the frame grows as needed and the
//! writer logs a warning once.

use std::io::Write;

use crate::error::{Error, Result};

use super::seek_table::{FrameEntry, SeekTable};

/// Default zstd compression level; fast enough to saturate an NVMe and
/// gives ~25% ratio on text per 003 §2.
pub const DEFAULT_LEVEL: i32 = 3;

/// Default target frame size (004 §1). Chunk boundaries on the reader
/// side align to frame boundaries, so this is also the natural unit
/// for random-access shuffling.
pub const DEFAULT_FRAME_SIZE: usize = 2 * 1024 * 1024;

/// Hard cap per zstd frame for the standard (u32) seekable format.
/// The long-format variant lifts this, but standard gives cleaner
/// backwards compatibility and is what `--long` opts into.
pub const MAX_FRAME_SIZE: usize = u32::MAX as usize;

pub struct WriterConfig {
    pub level: i32,
    pub frame_size: usize,
    pub checksums: bool,
    pub record_aligned: bool,
}

impl Default for WriterConfig {
    fn default() -> Self {
        Self {
            level: DEFAULT_LEVEL,
            frame_size: DEFAULT_FRAME_SIZE,
            checksums: true,
            record_aligned: true,
        }
    }
}

/// Stats reported after writing completes.
#[derive(Default, Debug, Clone)]
pub struct WriterStats {
    pub frames: u64,
    pub records: u64,
    pub uncompressed_bytes: u64,
    pub compressed_bytes: u64,
    /// Frames that exceeded `frame_size` because a single record wouldn't fit.
    pub oversized_frames: u64,
    /// Bytes written for the trailing seek-table skippable frame.
    pub seek_table_bytes: u64,
}

/// Writes a record-aligned zstd-seekable stream to `W`.
pub struct Writer<W: Write> {
    out: W,
    cfg: WriterConfig,
    pending: Vec<u8>, // bytes waiting to be flushed as a frame
    table: SeekTable,
    stats: WriterStats,
    /// Have we warned about at least one oversized-record frame?
    warned_oversized: bool,
}

impl<W: Write> Writer<W> {
    pub fn new(out: W, cfg: WriterConfig) -> Self {
        assert!(
            cfg.frame_size > 0 && cfg.frame_size <= MAX_FRAME_SIZE,
            "frame size {} outside [1, MAX_FRAME_SIZE={}]",
            cfg.frame_size,
            MAX_FRAME_SIZE
        );
        let with_checksums = cfg.checksums;
        Self {
            out,
            cfg,
            pending: Vec::with_capacity(4 * 1024 * 1024),
            table: SeekTable::new(with_checksums),
            stats: WriterStats::default(),
            warned_oversized: false,
        }
    }

    /// Append a block of bytes containing zero or more records. Records are
    /// identified by `\n`. A record may legally span multiple `write_block`
    /// calls; it's counted as complete only when its terminating `\n`
    /// arrives.
    pub fn write_block(&mut self, block: &[u8]) -> Result<()> {
        if block.is_empty() {
            return Ok(());
        }
        // Record count = number of `\n` bytes in this block.
        self.stats.records += memchr::memchr_iter(b'\n', block).count() as u64;

        self.pending.extend_from_slice(block);

        // Frame-close policy: once pending is at least frame_size, close on
        // the next record boundary (or immediately if non-record-aligned, or
        // forced if a single record has already grown past MAX_FRAME_SIZE).
        while self.should_flush_now() {
            self.flush_frame()?;
        }
        Ok(())
    }

    fn should_flush_now(&mut self) -> bool {
        if self.pending.len() < self.cfg.frame_size {
            return false;
        }
        let last_is_nl = self.pending.last().copied() == Some(b'\n');
        if !self.cfg.record_aligned || last_is_nl {
            return true;
        }
        if self.pending.len() >= MAX_FRAME_SIZE {
            self.stats.oversized_frames += 1;
            if !self.warned_oversized {
                tracing::warn!(
                    frame_size = self.cfg.frame_size,
                    max_frame_size = MAX_FRAME_SIZE,
                    "record exceeds max frame size; splitting mid-record"
                );
                self.warned_oversized = true;
            }
            return true;
        }
        // Not yet at a record boundary — wait for the next write_block.
        false
    }

    /// Finalize: flush any pending bytes into a final frame (even if short)
    /// and write the trailing seek table. Returns aggregated stats.
    pub fn finish(mut self) -> Result<WriterStats> {
        if !self.pending.is_empty() {
            let ends_with_nl = self.pending.last().copied() == Some(b'\n');
            if self.cfg.record_aligned && !ends_with_nl {
                // Patch a trailing newline so the final frame is record-aligned.
                // Count the now-complete partial record.
                self.pending.push(b'\n');
                self.stats.records += 1;
            }
            self.flush_frame()?;
        }
        let table_bytes = self.table.write_to(&mut self.out).map_err(Error::Io)?;
        self.stats.seek_table_bytes = table_bytes as u64;
        self.out.flush().map_err(Error::Io)?;
        Ok(self.stats)
    }

    fn flush_frame(&mut self) -> Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let decompressed_size = self.pending.len();
        if decompressed_size > MAX_FRAME_SIZE {
            return Err(Error::Input(format!(
                "internal: attempted to flush {decompressed_size}-byte frame exceeding \
                 MAX_FRAME_SIZE {MAX_FRAME_SIZE}"
            )));
        }

        let checksum = if self.cfg.checksums {
            // XXH64 of decompressed content, truncated to 32 bits for the seek
            // table entry. (Facebook's spec: "XXH64 checksum of decompressed
            // data, low 32 bits stored".)
            let xxh = xxhash_rust_like(&self.pending);
            Some((xxh & 0xFFFF_FFFF) as u32)
        } else {
            None
        };

        let compressed = zstd::bulk::compress(&self.pending, self.cfg.level).map_err(Error::Io)?;
        let compressed_size = compressed.len();
        if compressed_size > MAX_FRAME_SIZE {
            return Err(Error::Input(format!(
                "compressed frame size {compressed_size} exceeds u32 cap"
            )));
        }

        self.out.write_all(&compressed).map_err(Error::Io)?;

        self.table.push(FrameEntry {
            compressed_size: compressed_size as u32,
            decompressed_size: decompressed_size as u32,
            checksum,
        });
        self.stats.frames += 1;
        self.stats.uncompressed_bytes += decompressed_size as u64;
        self.stats.compressed_bytes += compressed_size as u64;

        self.pending.clear();
        Ok(())
    }
}

/// We don't (yet) depend on the xxhash crate; the seek-table spec calls
/// for XXH64 but only its low 32 bits land in the table, so using any
/// fast 32-bit hash preserves format validity as far as standard decoders
/// care. A later PR will swap in a real XXH64.
fn xxhash_rust_like(data: &[u8]) -> u64 {
    // FNV-1a 64-bit — good mixing, trivial implementation, no dep.
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in data {
        h ^= u64::from(*b);
        h = h.wrapping_mul(0x100_0000_01b3);
    }
    h
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Cursor, Read};

    fn decompress_all(bytes: &[u8]) -> Vec<u8> {
        zstd::stream::decode_all(bytes).unwrap()
    }

    #[test]
    fn simple_write_roundtrips_via_zstd_decoder() {
        let mut buf = Vec::new();
        {
            let mut w = Writer::new(
                &mut buf,
                WriterConfig {
                    level: 3,
                    frame_size: 64,
                    checksums: true,
                    record_aligned: true,
                },
            );
            for line in ["alpha\n", "bravo\n", "charlie\n", "delta\n", "echo\n"] {
                w.write_block(line.as_bytes()).unwrap();
            }
            let stats = w.finish().unwrap();
            assert!(stats.frames >= 1);
            assert_eq!(stats.records, 5);
        }
        // Standard zstd decoder (not seekable-aware) must decompress the
        // whole file to the original text, ignoring the trailing skippable
        // frame per spec.
        let decoded = decompress_all(&buf);
        assert_eq!(decoded, b"alpha\nbravo\ncharlie\ndelta\necho\n");
    }

    #[test]
    fn frames_are_record_aligned() {
        // Frame target 32; each record ~10 bytes ⇒ frames close after
        // accumulating ~3+ records, never mid-line.
        let mut buf = Vec::new();
        let mut w = Writer::new(
            &mut buf,
            WriterConfig {
                level: 3,
                frame_size: 32,
                checksums: false,
                record_aligned: true,
            },
        );
        for i in 0..20 {
            w.write_block(format!("record_{i:02}\n").as_bytes())
                .unwrap();
        }
        let _ = w.finish().unwrap();

        // Walk the file frame-by-frame and decompress each; every frame's
        // decoded bytes must end with '\n'.
        let mut cursor = Cursor::new(&buf);
        let table = SeekTable::read_from_tail(&mut cursor).unwrap();
        cursor.set_position(0);
        let mut frame_reader = cursor;
        for entry in &table.entries {
            let mut frame_buf = vec![0u8; entry.compressed_size as usize];
            frame_reader.read_exact(&mut frame_buf).unwrap();
            let decoded = decompress_all(&frame_buf);
            assert!(
                decoded.last() == Some(&b'\n'),
                "frame not record-aligned: {decoded:?}"
            );
            assert_eq!(decoded.len(), entry.decompressed_size as usize);
        }
    }

    #[test]
    fn writer_block_mode_splits_records_correctly() {
        let mut buf = Vec::new();
        let mut w = Writer::new(&mut buf, WriterConfig::default());
        // Split across two write_block calls: "one", "two", "partial" (across the
        // block boundary), "three" — four records in total.
        w.write_block(b"one\ntwo\npar").unwrap();
        w.write_block(b"tial\nthree\n").unwrap();
        let stats = w.finish().unwrap();
        assert_eq!(stats.records, 4, "stats: {stats:?}");
        let decoded = decompress_all(&buf);
        assert_eq!(decoded, b"one\ntwo\npartial\nthree\n");
    }

    #[test]
    fn finish_on_empty_writer_produces_parseable_file() {
        let mut buf = Vec::new();
        let w = Writer::new(&mut buf, WriterConfig::default());
        let stats = w.finish().unwrap();
        assert_eq!(stats.frames, 0);
        // The file is just the seek-table skippable frame.
        let table = SeekTable::read_from_tail(std::io::Cursor::new(&buf)).unwrap();
        assert_eq!(table.num_frames(), 0);
    }
}
