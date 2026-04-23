//! Input sources. PR-2 ships plain-file `read()` and stdin; mmap, pread, and
//! seekable-zstd arrive in later PRs.

pub mod magic;

use std::fs::File;
use std::io::{self, BufRead, BufReader, Read};
use std::path::{Path, PathBuf};

use crate::error::{Error, Result};

/// Default buffer capacity for the [`Input`] reader (2 MiB).
///
/// Big enough to amortize per-syscall overhead on modern NVMe; small enough
/// to stay within a single L2 slice and not blow the page cache.
pub const DEFAULT_BUFFER_CAPACITY: usize = 2 * 1024 * 1024;

/// Opened input source, type-erased to keep the downstream pipeline simple.
pub struct Input {
    inner: BufReader<Box<dyn Read + Send>>,
    /// File size from `stat()` at open time; `None` for stdin.
    size: Option<u64>,
    /// Original path, for error messages and logging.
    path: Option<PathBuf>,
    /// Detected format (002 §4.5). `Plain` for stdin unless peeking finds a magic.
    format: magic::Format,
}

impl Input {
    /// Open a file path, or `Path::new("-")` to read stdin.
    pub fn open(path: &Path) -> Result<Self> {
        if path == Path::new("-") {
            return Self::from_stdin();
        }
        let file = File::open(path).map_err(|e| match e.kind() {
            io::ErrorKind::NotFound => Error::NotFound {
                path: path.display().to_string(),
            },
            io::ErrorKind::PermissionDenied => Error::PermissionDenied {
                path: path.display().to_string(),
            },
            _ => Error::Io(e),
        })?;
        let size = file.metadata().map(|m| m.len()).ok();
        Self::from_reader(Box::new(file), size, Some(path.to_path_buf()))
    }

    /// Wrap an already-open stdin.
    pub fn from_stdin() -> Result<Self> {
        Self::from_reader(Box::new(io::stdin()), None, None)
    }

    /// Build from any `Read + Send` (used in tests).
    pub fn from_reader(
        reader: Box<dyn Read + Send>,
        size: Option<u64>,
        path: Option<PathBuf>,
    ) -> Result<Self> {
        let mut inner = BufReader::with_capacity(DEFAULT_BUFFER_CAPACITY, reader);
        // Peek the first bytes without consuming, then detect.
        let head = inner.fill_buf()?;
        let format = magic::detect(head);
        Ok(Self {
            inner,
            size,
            path,
            format,
        })
    }

    pub fn size_hint(&self) -> Option<u64> {
        self.size
    }

    pub fn path(&self) -> Option<&Path> {
        self.path.as_deref()
    }

    pub fn format(&self) -> magic::Format {
        self.format
    }

    /// Reject a compressed input with a clear hint when the current mode can't handle it.
    pub fn reject_if_compressed(&self) -> Result<()> {
        if self.format == magic::Format::Plain {
            return Ok(());
        }
        let path = self
            .path
            .as_ref()
            .map(|p| p.display().to_string())
            .unwrap_or_else(|| "<stdin>".to_string());
        Err(Error::CompressedInputUnsupported {
            path,
            format: self.format.name(),
            suggestion: self.format.decompress_cmd(),
        })
    }
}

impl Read for Input {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.inner.read(buf)
    }
}

impl BufRead for Input {
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        self.inner.fill_buf()
    }
    fn consume(&mut self, amt: usize) {
        self.inner.consume(amt)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_reader_detects_plain() {
        let data: &[u8] = b"{\"a\":1}\n{\"a\":2}\n";
        let inp = Input::from_reader(Box::new(data), Some(data.len() as u64), None).unwrap();
        assert_eq!(inp.format(), magic::Format::Plain);
        assert_eq!(inp.size_hint(), Some(data.len() as u64));
    }

    #[test]
    fn from_reader_detects_gzip() {
        let head: &[u8] = &[0x1f, 0x8b, 0x08, 0x00, 0x00];
        let inp = Input::from_reader(Box::new(head), None, None).unwrap();
        assert_eq!(inp.format(), magic::Format::Gzip);
        assert!(inp.reject_if_compressed().is_err());
    }
}
