//! Input sources.
//!
//! After PR-3: plain files, stdin, and transparently-decompressed streaming
//! inputs for `.gz` / `.zst` / `.bz2` / `.xz` (feature-gated). Random-access
//! I/O (pread, mmap, io_uring, zstd_seekable) arrives in later PRs.

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

/// Opened input source. The `BufRead` handle always exposes decompressed
/// plain bytes regardless of the on-disk format.
pub struct Input {
    inner: Box<dyn BufRead + Send>,
    /// File size from `stat()` at open time; `None` for stdin.
    size: Option<u64>,
    /// Original path, for error messages and logging.
    path: Option<PathBuf>,
    /// Format detected on disk. `Plain` after transparent decompression.
    raw_format: magic::Format,
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
        build(Box::new(file), size, Some(path.to_path_buf()))
    }

    /// Wrap an already-open stdin.
    pub fn from_stdin() -> Result<Self> {
        build(Box::new(io::stdin()), None, None)
    }

    /// Build from any `Read + Send` (used in tests).
    pub fn from_reader(
        reader: Box<dyn Read + Send>,
        size: Option<u64>,
        path: Option<PathBuf>,
    ) -> Result<Self> {
        build(reader, size, path)
    }

    pub fn size_hint(&self) -> Option<u64> {
        self.size
    }

    pub fn path(&self) -> Option<&Path> {
        self.path.as_deref()
    }

    /// The format detected on disk before any transparent decompression.
    pub fn raw_format(&self) -> magic::Format {
        self.raw_format
    }

    /// Is this input an already-sequential stream (stdin or compressed)?
    /// Random-access modes reject streaming inputs because they can't seek.
    pub fn is_streaming(&self) -> bool {
        self.path.is_none() || self.raw_format != magic::Format::Plain
    }

    /// Used by sequential modes that want an informative error when given
    /// a compressed input without the corresponding decompressor feature.
    /// With PR-3 decompressors in place, a file that makes it through
    /// `open()` already has its bytes decoded — this method is now mostly
    /// a no-op, kept for API stability and future seekable-only modes.
    pub fn reject_if_compressed(&self) -> Result<()> {
        // After build() succeeds, compressed inputs have been transparently
        // decoded; the only way to see a non-Plain raw_format + no wrapped
        // decoder would be the `--compressed-as-bytes` escape hatch, which
        // PR-3 does not expose.
        Ok(())
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

/// Peek magic bytes, wrap in the appropriate decoder if feature-enabled,
/// and return a plain-bytes `BufRead` over a 2 MiB buffer.
fn build(raw: Box<dyn Read + Send>, size: Option<u64>, path: Option<PathBuf>) -> Result<Input> {
    // Small dedicated buffer for the peek. 16 bytes is enough for every
    // format's magic except LZ4 (4) / XZ (6) / bzip2 (3) / gzip (2) / zstd (4).
    let mut peeker = BufReader::with_capacity(64, raw);
    let head = peeker.fill_buf().map_err(Error::Io)?.to_vec();
    let format = magic::detect(&head);

    let plain_reader: Box<dyn Read + Send> = match format {
        magic::Format::Plain => Box::new(peeker),
        magic::Format::Gzip => wrap_gzip(peeker, &path)?,
        magic::Format::Zstd => wrap_zstd(peeker, &path)?,
        magic::Format::Bzip2 => wrap_bzip2(peeker, &path)?,
        magic::Format::Xz => wrap_xz(peeker, &path)?,
        magic::Format::Lz4 => wrap_lz4(peeker, &path)?,
    };

    Ok(Input {
        inner: Box::new(BufReader::with_capacity(
            DEFAULT_BUFFER_CAPACITY,
            plain_reader,
        )),
        size,
        path,
        raw_format: format,
    })
}

fn display_path(path: &Option<PathBuf>) -> String {
    path.as_ref()
        .map(|p| p.display().to_string())
        .unwrap_or_else(|| "<stdin>".to_string())
}

fn unsupported(format: magic::Format, path: &Option<PathBuf>, missing_feature: &str) -> Error {
    Error::Input(format!(
        "'{}' is {} but shuflr was built without the `{}` feature; \
         rebuild with `cargo install shuflr --features {}`, or pipe through `{}`",
        display_path(path),
        format.name(),
        missing_feature,
        missing_feature,
        format.decompress_cmd(),
    ))
}

#[cfg(feature = "gzip")]
fn wrap_gzip<R: Read + Send + 'static>(
    r: R,
    _path: &Option<PathBuf>,
) -> Result<Box<dyn Read + Send>> {
    Ok(Box::new(flate2::read::MultiGzDecoder::new(r)))
}
#[cfg(not(feature = "gzip"))]
fn wrap_gzip<R: Read + Send + 'static>(
    _r: R,
    path: &Option<PathBuf>,
) -> Result<Box<dyn Read + Send>> {
    Err(unsupported(magic::Format::Gzip, path, "gzip"))
}

#[cfg(feature = "zstd")]
fn wrap_zstd<R: Read + Send + 'static>(
    r: R,
    _path: &Option<PathBuf>,
) -> Result<Box<dyn Read + Send>> {
    let dec = zstd::stream::Decoder::new(r).map_err(Error::Io)?;
    Ok(Box::new(dec))
}
#[cfg(not(feature = "zstd"))]
fn wrap_zstd<R: Read + Send + 'static>(
    _r: R,
    path: &Option<PathBuf>,
) -> Result<Box<dyn Read + Send>> {
    Err(unsupported(magic::Format::Zstd, path, "zstd"))
}

#[cfg(feature = "bzip2")]
fn wrap_bzip2<R: Read + Send + 'static>(
    r: R,
    _path: &Option<PathBuf>,
) -> Result<Box<dyn Read + Send>> {
    Ok(Box::new(bzip2::read::MultiBzDecoder::new(r)))
}
#[cfg(not(feature = "bzip2"))]
fn wrap_bzip2<R: Read + Send + 'static>(
    _r: R,
    path: &Option<PathBuf>,
) -> Result<Box<dyn Read + Send>> {
    Err(unsupported(magic::Format::Bzip2, path, "bzip2"))
}

#[cfg(feature = "xz")]
fn wrap_xz<R: Read + Send + 'static>(
    r: R,
    _path: &Option<PathBuf>,
) -> Result<Box<dyn Read + Send>> {
    Ok(Box::new(xz2::read::XzDecoder::new_multi_decoder(r)))
}
#[cfg(not(feature = "xz"))]
fn wrap_xz<R: Read + Send + 'static>(
    _r: R,
    path: &Option<PathBuf>,
) -> Result<Box<dyn Read + Send>> {
    Err(unsupported(magic::Format::Xz, path, "xz"))
}

fn wrap_lz4<R: Read + Send + 'static>(
    _r: R,
    path: &Option<PathBuf>,
) -> Result<Box<dyn Read + Send>> {
    // LZ4 streaming is rare enough for LLM corpora that shipping it isn't
    // on the v1 path (003 open question #5). Error clearly.
    Err(unsupported(magic::Format::Lz4, path, "lz4"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_reader_detects_plain() {
        let data: &[u8] = b"{\"a\":1}\n{\"a\":2}\n";
        let inp = Input::from_reader(Box::new(data), Some(data.len() as u64), None).unwrap();
        assert_eq!(inp.raw_format(), magic::Format::Plain);
        assert_eq!(inp.size_hint(), Some(data.len() as u64));
    }

    #[cfg(feature = "gzip")]
    #[test]
    fn gzip_roundtrips_transparently() {
        use flate2::Compression;
        use flate2::write::GzEncoder;
        use std::io::Write;

        let original = b"alpha\nbravo\ncharlie\n";
        let mut enc = GzEncoder::new(Vec::new(), Compression::default());
        enc.write_all(original).unwrap();
        let compressed = enc.finish().unwrap();

        let mut inp =
            Input::from_reader(Box::new(std::io::Cursor::new(compressed)), None, None).unwrap();
        assert_eq!(inp.raw_format(), magic::Format::Gzip);

        let mut out = Vec::new();
        inp.read_to_end(&mut out).unwrap();
        assert_eq!(out, original);
    }

    #[cfg(feature = "zstd")]
    #[test]
    fn zstd_roundtrips_transparently() {
        let original = b"alpha\nbravo\ncharlie\n";
        let compressed = zstd::stream::encode_all(&original[..], 3).unwrap();

        let mut inp =
            Input::from_reader(Box::new(std::io::Cursor::new(compressed)), None, None).unwrap();
        assert_eq!(inp.raw_format(), magic::Format::Zstd);

        let mut out = Vec::new();
        inp.read_to_end(&mut out).unwrap();
        assert_eq!(out, original);
    }

    #[cfg(feature = "bzip2")]
    #[test]
    fn bzip2_roundtrips_transparently() {
        use bzip2::Compression;
        use bzip2::write::BzEncoder;
        use std::io::Write;

        let original = b"alpha\nbravo\ncharlie\n";
        let mut enc = BzEncoder::new(Vec::new(), Compression::default());
        enc.write_all(original).unwrap();
        let compressed = enc.finish().unwrap();

        let mut inp =
            Input::from_reader(Box::new(std::io::Cursor::new(compressed)), None, None).unwrap();
        assert_eq!(inp.raw_format(), magic::Format::Bzip2);

        let mut out = Vec::new();
        inp.read_to_end(&mut out).unwrap();
        assert_eq!(out, original);
    }

    #[cfg(feature = "xz")]
    #[test]
    fn xz_roundtrips_transparently() {
        use std::io::Write;
        use xz2::write::XzEncoder;

        let original = b"alpha\nbravo\ncharlie\n";
        let mut enc = XzEncoder::new(Vec::new(), 6);
        enc.write_all(original).unwrap();
        let compressed = enc.finish().unwrap();

        let mut inp =
            Input::from_reader(Box::new(std::io::Cursor::new(compressed)), None, None).unwrap();
        assert_eq!(inp.raw_format(), magic::Format::Xz);

        let mut out = Vec::new();
        inp.read_to_end(&mut out).unwrap();
        assert_eq!(out, original);
    }
}
