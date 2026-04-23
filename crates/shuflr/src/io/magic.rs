//! Format detection via magic bytes (003 §6.1).

/// Formats we recognize from a leading-bytes peek.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Format {
    /// Plain JSONL (or anything non-compressed).
    Plain,
    Gzip,
    Zstd,
    Bzip2,
    Xz,
    Lz4,
}

impl Format {
    pub fn name(self) -> &'static str {
        match self {
            Self::Plain => "plain",
            Self::Gzip => "gzip",
            Self::Zstd => "zstd",
            Self::Bzip2 => "bzip2",
            Self::Xz => "xz",
            Self::Lz4 => "lz4",
        }
    }

    /// Shell command that decompresses this format to stdout. Used in error messages.
    pub fn decompress_cmd(self) -> &'static str {
        match self {
            Self::Plain => "",
            Self::Gzip => "gunzip -c",
            Self::Zstd => "zstdcat",
            Self::Bzip2 => "bunzip2 -c",
            Self::Xz => "xzcat",
            Self::Lz4 => "lz4cat",
        }
    }
}

/// Identify a format from the first few bytes. Returns `Plain` when no magic matches.
pub fn detect(head: &[u8]) -> Format {
    if head.len() >= 2 && head[0] == 0x1f && head[1] == 0x8b {
        return Format::Gzip;
    }
    if head.len() >= 4 && head[..4] == [0x28, 0xb5, 0x2f, 0xfd] {
        return Format::Zstd;
    }
    if head.len() >= 3 && head[..3] == [0x42, 0x5a, 0x68] {
        return Format::Bzip2;
    }
    if head.len() >= 6 && head[..6] == [0xfd, 0x37, 0x7a, 0x58, 0x5a, 0x00] {
        return Format::Xz;
    }
    if head.len() >= 4 && head[..4] == [0x04, 0x22, 0x4d, 0x18] {
        return Format::Lz4;
    }
    Format::Plain
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_plain() {
        assert_eq!(detect(b"{}"), Format::Plain);
        assert_eq!(detect(b""), Format::Plain);
        assert_eq!(detect(b"hello"), Format::Plain);
    }

    #[test]
    fn detects_gzip() {
        assert_eq!(detect(&[0x1f, 0x8b, 0x08, 0x00]), Format::Gzip);
    }

    #[test]
    fn detects_zstd() {
        assert_eq!(detect(&[0x28, 0xb5, 0x2f, 0xfd, 0x00]), Format::Zstd);
    }

    #[test]
    fn detects_bzip2() {
        assert_eq!(detect(b"BZh91AY"), Format::Bzip2);
    }

    #[test]
    fn detects_xz() {
        assert_eq!(
            detect(&[0xfd, 0x37, 0x7a, 0x58, 0x5a, 0x00, 0x01]),
            Format::Xz
        );
    }

    #[test]
    fn detects_lz4() {
        assert_eq!(detect(&[0x04, 0x22, 0x4d, 0x18, 0x64]), Format::Lz4);
    }
}
