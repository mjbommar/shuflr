//! Library-wide error type, per 002 §10.3.

use std::io;

/// All fallible operations in this crate return `Result<T> = Result<T, Error>`.
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Underlying OS / filesystem error.
    #[error("io: {0}")]
    Io(#[from] io::Error),

    /// User gave us an unusable input (bad path, unsupported format, etc.).
    #[error("input: {0}")]
    Input(String),

    /// Input path does not exist. CLI translates to EX_NOINPUT (66).
    #[error("no such file: {path}")]
    NotFound { path: String },

    /// Process cannot read the input. CLI translates to EX_NOPERM (77).
    #[error("cannot read '{path}': permission denied")]
    PermissionDenied { path: String },

    /// A single record exceeded `--max-line` and `--on-error=fail` was set.
    #[error("oversized record at byte {offset}: {len} bytes exceeds --max-line {cap}")]
    OversizedRecord { offset: u64, len: u64, cap: u64 },

    /// Backing file was truncated / replaced after open (002 §4.5).
    #[error("input changed since open: {0}")]
    InputChanged(String),

    /// Compressed input was given to a mode that does not support it (003 §6).
    #[error(
        "compressed input '{path}' ({format}) is not supported by this mode; \
         decompress first with `{suggestion}`, or use --shuffle=buffer|reservoir|none"
    )]
    CompressedInputUnsupported {
        path: String,
        format: &'static str,
        suggestion: &'static str,
    },
}
