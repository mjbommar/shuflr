//! Seekable-zstd reader: opens a file, parses its trailing seek table,
//! and exposes `decompress_frame(idx)` for any frame.
//!
//! Each frame is a standalone zstd frame (so any zstd-aware decoder
//! can decode it). Frame offsets within the file are derived by
//! prefix-summing per-frame compressed sizes from the seek table. Our
//! [`super::writer::Writer`] emits frames back-to-back from offset 0,
//! with no leading skippable frames in PR-4, so the first frame starts
//! at byte 0. Future PRs that prepend a metadata frame will update
//! `frame_offset()` accordingly.

use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use crate::error::{Error, Result};

use super::seek_table::{FrameEntry, SeekTable};

pub struct SeekableReader {
    file: File,
    path: PathBuf,
    table: SeekTable,
    /// Prefix sums of compressed sizes: offsets[i] = start byte of frame i.
    offsets: Vec<u64>,
}

impl SeekableReader {
    /// Open a seekable-zstd file. Fails cleanly if the tail doesn't
    /// contain a valid seek-table skippable frame.
    pub fn open(path: &Path) -> Result<Self> {
        let mut file = File::open(path).map_err(|e| match e.kind() {
            std::io::ErrorKind::NotFound => Error::NotFound {
                path: path.display().to_string(),
            },
            std::io::ErrorKind::PermissionDenied => Error::PermissionDenied {
                path: path.display().to_string(),
            },
            _ => Error::Io(e),
        })?;
        let table = SeekTable::read_from_tail(&mut file).map_err(Error::Io)?;

        let mut offsets = Vec::with_capacity(table.num_frames() as usize + 1);
        let mut acc = 0u64;
        for e in &table.entries {
            offsets.push(acc);
            acc += u64::from(e.compressed_size);
        }
        // Sentinel so offsets[i+1] - offsets[i] is always well-defined.
        offsets.push(acc);

        Ok(Self {
            file,
            path: path.to_path_buf(),
            table,
            offsets,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn num_frames(&self) -> usize {
        self.table.entries.len()
    }

    pub fn total_decompressed(&self) -> u64 {
        self.table.total_decompressed()
    }

    pub fn entries(&self) -> &[FrameEntry] {
        &self.table.entries
    }

    /// Decompress frame `idx` into a freshly-allocated `Vec<u8>`.
    pub fn decompress_frame(&mut self, idx: usize) -> Result<Vec<u8>> {
        if idx >= self.num_frames() {
            return Err(Error::Input(format!(
                "frame {idx} out of range (file has {} frames)",
                self.num_frames()
            )));
        }
        let comp_size = self.table.entries[idx].compressed_size as usize;
        let offset = self.offsets[idx];

        self.file.seek(SeekFrom::Start(offset)).map_err(Error::Io)?;
        let mut comp = vec![0u8; comp_size];
        self.file.read_exact(&mut comp).map_err(Error::Io)?;

        let decoded = zstd::stream::decode_all(&comp[..]).map_err(Error::Io)?;
        Ok(decoded)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::zstd_seekable::writer::{Writer, WriterConfig};

    fn build_seekable_file(records: &[&[u8]]) -> tempfile::NamedTempFile {
        let f = tempfile::NamedTempFile::new().unwrap();
        {
            let file = std::fs::File::create(f.path()).unwrap();
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
        f
    }

    #[test]
    fn reader_roundtrips_every_frame() {
        let records: Vec<&[u8]> = vec![
            b"alpha\n",
            b"bravo\n",
            b"charlie\n",
            b"delta\n",
            b"echo\n",
            b"foxtrot\n",
            b"golf\n",
            b"hotel\n",
            b"india\n",
            b"juliet\n",
        ];
        let tmpfile = build_seekable_file(&records);

        let mut r = SeekableReader::open(tmpfile.path()).unwrap();
        let total: u64 = records.iter().map(|b| b.len() as u64).sum();
        assert_eq!(r.total_decompressed(), total);

        // Concatenate every frame's decoded bytes — must equal input.
        let mut recovered = Vec::new();
        for i in 0..r.num_frames() {
            let bytes = r.decompress_frame(i).unwrap();
            recovered.extend_from_slice(&bytes);
        }
        let original: Vec<u8> = records.concat();
        assert_eq!(recovered, original);
    }

    #[test]
    fn reader_supports_random_access() {
        let records: Vec<String> = (0..30).map(|i| format!("record_{i:02}\n")).collect();
        let byte_records: Vec<&[u8]> = records.iter().map(|s| s.as_bytes()).collect();
        let tmpfile = build_seekable_file(&byte_records);

        let mut r = SeekableReader::open(tmpfile.path()).unwrap();
        assert!(r.num_frames() >= 2, "need >1 frame for meaningful test");
        // Access frames in a non-monotonic order.
        let last = r.num_frames() - 1;
        let _ = r.decompress_frame(last).unwrap();
        let _ = r.decompress_frame(0).unwrap();
        let _ = r.decompress_frame(last).unwrap();
    }

    #[test]
    fn reader_rejects_non_seekable_file() {
        use std::io::Write;
        let f = tempfile::NamedTempFile::new().unwrap();
        f.as_file().write_all(b"not a zstd file at all").unwrap();
        assert!(SeekableReader::open(f.path()).is_err());
    }
}
