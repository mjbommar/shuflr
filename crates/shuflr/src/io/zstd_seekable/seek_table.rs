//! Facebook zstd-seekable seek-table framing.
//!
//! A seekable zstd file is a sequence of regular zstd frames followed by
//! one trailing skippable frame with the structure below. Any zstd decoder
//! reads the file normally; seekable-aware decoders can binary-search the
//! table to jump to any frame in O(log N).
//!
//! Wire format (little-endian throughout):
//!
//! ```text
//!   Skippable_Magic      u32  = 0x184D2A5E
//!   Frame_Size           u32  = total bytes of (entries + footer)
//!   Seek_Table_Entries
//!     repeat num_frames times:
//!       Compressed_Size     u32
//!       Decompressed_Size   u32
//!       [Checksum           u32]   -- only if descriptor bit 7 set
//!   Seek_Table_Footer
//!     Number_Of_Frames    u32
//!     Seek_Table_Descriptor  u8  (bit 7 = checksum present)
//!     Seekable_Magic_Number u32 = 0x8F92EAB1
//! ```
//!
//! Reference: <https://github.com/facebook/zstd/blob/dev/contrib/seekable_format/zstd_seekable_compression_format.md>

use std::io::{self, Read, Seek, SeekFrom, Write};

/// Skippable-frame magic for the seekable table.
pub const SKIPPABLE_MAGIC: u32 = 0x184D_2A5E;
/// Footer magic that identifies the skippable frame as a seekable table.
pub const SEEKABLE_MAGIC: u32 = 0x8F92_EAB1;

/// One entry per zstd frame in the seekable file.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct FrameEntry {
    pub compressed_size: u32,
    pub decompressed_size: u32,
    /// XXH64 of the decompressed content, truncated to 32 bits (as stored on wire).
    pub checksum: Option<u32>,
}

/// Accumulates frame entries as a seekable file is being written, then
/// serializes the trailing skippable frame.
#[derive(Default, Debug)]
pub struct SeekTable {
    pub entries: Vec<FrameEntry>,
    /// Whether checksums are stored for each frame. Must match the `checksum`
    /// field of every entry.
    pub with_checksums: bool,
}

impl SeekTable {
    pub fn new(with_checksums: bool) -> Self {
        Self {
            entries: Vec::new(),
            with_checksums,
        }
    }

    pub fn push(&mut self, entry: FrameEntry) {
        debug_assert_eq!(
            entry.checksum.is_some(),
            self.with_checksums,
            "checksum presence must match the table configuration"
        );
        self.entries.push(entry);
    }

    pub fn num_frames(&self) -> u32 {
        self.entries.len() as u32
    }

    /// Total decompressed size, for stats.
    pub fn total_decompressed(&self) -> u64 {
        self.entries
            .iter()
            .map(|e| u64::from(e.decompressed_size))
            .sum()
    }

    /// Total compressed size, summed over frames (not including the seek-table frame).
    pub fn total_compressed(&self) -> u64 {
        self.entries
            .iter()
            .map(|e| u64::from(e.compressed_size))
            .sum()
    }

    /// Serialize the trailing skippable frame to `out`. Returns bytes written.
    pub fn write_to(&self, mut out: impl Write) -> io::Result<usize> {
        let entry_size = if self.with_checksums { 12 } else { 8 };
        let entries_bytes = self.entries.len() * entry_size;
        let footer_bytes = 4 /* num_frames */ + 1 /* descriptor */ + 4 /* magic */;
        let frame_body_size = entries_bytes + footer_bytes;

        let descriptor: u8 = if self.with_checksums { 1 << 7 } else { 0 };

        // Skippable frame header
        out.write_all(&SKIPPABLE_MAGIC.to_le_bytes())?;
        out.write_all(&(frame_body_size as u32).to_le_bytes())?;
        // Entries
        for e in &self.entries {
            out.write_all(&e.compressed_size.to_le_bytes())?;
            out.write_all(&e.decompressed_size.to_le_bytes())?;
            if let Some(c) = e.checksum {
                out.write_all(&c.to_le_bytes())?;
            }
        }
        // Footer
        out.write_all(&self.num_frames().to_le_bytes())?;
        out.write_all(&[descriptor])?;
        out.write_all(&SEEKABLE_MAGIC.to_le_bytes())?;

        Ok(8 + frame_body_size)
    }

    /// Read a seek table from the tail of an existing seekable file.
    pub fn read_from_tail(mut reader: impl Read + Seek) -> io::Result<Self> {
        // Footer is 9 bytes, but we also need the 8-byte skippable-frame header
        // to validate the leading magic + frame size. Seek from end.
        let total_len = reader.seek(SeekFrom::End(0))?;
        if total_len < 17 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "file too short to contain a seek table",
            ));
        }

        // Read last 9 bytes (footer).
        reader.seek(SeekFrom::End(-9))?;
        let mut footer = [0u8; 9];
        reader.read_exact(&mut footer)?;

        let num_frames = u32::from_le_bytes([footer[0], footer[1], footer[2], footer[3]]);
        let descriptor = footer[4];
        let magic = u32::from_le_bytes([footer[5], footer[6], footer[7], footer[8]]);
        if magic != SEEKABLE_MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "not a zstd-seekable file: expected trailer magic {SEEKABLE_MAGIC:#x}, found {magic:#x}"
                ),
            ));
        }
        // Reserved bits must be zero.
        if descriptor & 0b0111_1111 != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "seekable descriptor has reserved bits set",
            ));
        }
        let with_checksums = descriptor & (1 << 7) != 0;

        let entry_size: u64 = if with_checksums { 12 } else { 8 };
        let body_bytes: u64 = u64::from(num_frames) * entry_size + 9; /* footer */
        // Header = SKIPPABLE_MAGIC (4) + frame_size (4) = 8 bytes.
        let frame_header_start = total_len
            .checked_sub(8 + body_bytes)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "impossible seek table"))?;

        reader.seek(SeekFrom::Start(frame_header_start))?;
        let mut header = [0u8; 8];
        reader.read_exact(&mut header)?;
        let skip_magic = u32::from_le_bytes([header[0], header[1], header[2], header[3]]);
        let frame_size = u32::from_le_bytes([header[4], header[5], header[6], header[7]]);
        if skip_magic != SKIPPABLE_MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "seek table precedes unexpected magic; not a seekable file",
            ));
        }
        if u64::from(frame_size) != body_bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "seek-table frame size mismatch: header says {frame_size}, footer implies {body_bytes}"
                ),
            ));
        }

        // Read entries sequentially.
        let mut entries = Vec::with_capacity(num_frames as usize);
        for _ in 0..num_frames {
            let mut buf = [0u8; 12];
            reader.read_exact(&mut buf[..entry_size as usize])?;
            let comp = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]);
            let dec = u32::from_le_bytes([buf[4], buf[5], buf[6], buf[7]]);
            let checksum = if with_checksums {
                Some(u32::from_le_bytes([buf[8], buf[9], buf[10], buf[11]]))
            } else {
                None
            };
            entries.push(FrameEntry {
                compressed_size: comp,
                decompressed_size: dec,
                checksum,
            });
        }

        Ok(Self {
            entries,
            with_checksums,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_no_checksum() {
        let mut table = SeekTable::new(false);
        table.push(FrameEntry {
            compressed_size: 1024,
            decompressed_size: 4096,
            checksum: None,
        });
        table.push(FrameEntry {
            compressed_size: 2048,
            decompressed_size: 8192,
            checksum: None,
        });

        // Prepend a dummy zstd frame so tail-reading has something to skip past.
        let mut buf = Vec::new();
        buf.extend_from_slice(&[0u8; 100]); // pretend zstd frames
        let written = table.write_to(&mut buf).unwrap();
        assert!(written >= 25);

        let parsed = SeekTable::read_from_tail(std::io::Cursor::new(&buf)).unwrap();
        assert_eq!(parsed.num_frames(), 2);
        assert!(!parsed.with_checksums);
        assert_eq!(parsed.entries[0].compressed_size, 1024);
        assert_eq!(parsed.entries[1].decompressed_size, 8192);
    }

    #[test]
    fn roundtrip_with_checksum() {
        let mut table = SeekTable::new(true);
        table.push(FrameEntry {
            compressed_size: 42,
            decompressed_size: 100,
            checksum: Some(0xdead_beef),
        });

        let mut buf = vec![0u8; 50];
        table.write_to(&mut buf).unwrap();

        let parsed = SeekTable::read_from_tail(std::io::Cursor::new(&buf)).unwrap();
        assert!(parsed.with_checksums);
        assert_eq!(parsed.entries[0].checksum, Some(0xdead_beef));
    }

    #[test]
    fn rejects_non_seekable() {
        let buf = vec![0u8; 100];
        let err = SeekTable::read_from_tail(std::io::Cursor::new(buf)).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn empty_table_still_valid() {
        let table = SeekTable::new(false);
        let mut buf = Vec::new();
        table.write_to(&mut buf).unwrap();
        let parsed = SeekTable::read_from_tail(std::io::Cursor::new(&buf)).unwrap();
        assert_eq!(parsed.num_frames(), 0);
    }
}
