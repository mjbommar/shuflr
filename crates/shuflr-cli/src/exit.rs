//! Exit codes, adhering to sysexits.h where applicable (002 §6.5).

use std::process::ExitCode;

/// Discrete exit conditions. `From<Code> for ExitCode` yields the numeric value.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Code {
    Ok = 0,
    /// `analyze` judged the corpus unsafe for chunk-shuffled.
    AnalyzeUnsafe = 3,
    /// `shuflr` was run in a harness but no subcommand is implemented yet.
    Unimplemented = 69, // EX_UNAVAILABLE
    /// Flag parse failure or invalid flag combination (EX_USAGE).
    Usage = 64,
    /// Malformed JSONL under `--verify` or `--on-error=fail` (EX_DATAERR).
    DataErr = 65,
    /// Input file missing (EX_NOINPUT).
    NoInput = 66,
    /// Uncaught panic; we translate to EX_SOFTWARE.
    Software = 70,
    /// Bind/socket creation failed (EX_CANTCREAT).
    CantCreate = 73,
    /// Permission denied on read or bind (EX_NOPERM).
    NoPerm = 77,
}

impl From<Code> for ExitCode {
    fn from(c: Code) -> Self {
        ExitCode::from(c as u8)
    }
}
