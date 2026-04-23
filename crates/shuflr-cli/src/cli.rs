//! Command-line argument parsing.
//!
//! The CLI is subcommand-driven (002 §6.1). A bare invocation like
//! `shuflr file.jsonl` is rewritten to `shuflr stream file.jsonl` in [`parse`]
//! so `stream` is the implicit default.

use std::path::PathBuf;

use clap::{Args, Parser, Subcommand, ValueEnum};
use clap_complete::Shell;

/// The names of subcommands that must NOT be treated as implicit `stream` inputs.
const SUBCOMMAND_NAMES: &[&str] = &[
    "stream",
    "serve",
    "convert",
    "info",
    "analyze",
    "index",
    "verify",
    "completions",
    "man",
    "help",
];

#[derive(Parser, Debug)]
#[command(
    name = "shuflr",
    version,
    about = "Stream large JSONL in shuffled order, without loading it into memory.",
    long_about = "shuflr streams records from JSONL files (optionally compressed) in \
                  shuffled order without loading the file into memory. Works as a CLI \
                  pipe or a gRPC service. See the subcommands for specific workflows; \
                  `shuflr file.jsonl` is shorthand for `shuflr stream file.jsonl`.",
    disable_help_subcommand = true,
    arg_required_else_help = true
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    /// Stream shuffled records to stdout (default when no subcommand given)
    Stream(StreamArgs),

    /// Serve shuffled records over gRPC or UDS
    #[cfg(feature = "grpc")]
    Serve(ServeArgs),

    /// Convert JSONL (optionally compressed) to zstd-seekable format
    #[cfg(feature = "zstd")]
    Convert(ConvertArgs),

    /// Show seekable file metadata (reads only the seek table; < 10ms)
    #[cfg(feature = "zstd")]
    Info(InfoArgs),

    /// Scan a corpus and warn if chunk-shuffled would be unsafe
    Analyze(AnalyzeArgs),

    /// Build a .shuflr-idx file for index-perm mode
    Index(IndexArgs),

    /// Validate JSONL framing; exit 65 on malformed records
    Verify(VerifyArgs),

    /// Emit a shell completion script
    Completions(CompletionsArgs),

    /// Emit a roff man page to stdout (redirect to e.g. /usr/local/share/man/man1/shuflr.1)
    Man(ManArgs),
}

impl Command {
    /// The effective log-level string for this invocation, or `"info"` for
    /// subcommands that don't take one (completions / info).
    pub fn log_level(&self) -> &str {
        match self {
            Self::Stream(a) => &a.log_level,
            #[cfg(feature = "grpc")]
            Self::Serve(a) => &a.log_level,
            #[cfg(feature = "zstd")]
            Self::Convert(a) => &a.log_level,
            #[cfg(feature = "zstd")]
            Self::Info(_) => "info",
            Self::Analyze(_) => "info",
            Self::Index(_) => "info",
            Self::Verify(_) => "info",
            Self::Completions(_) => "warn",
            Self::Man(_) => "warn",
        }
    }
}

/// Arguments shared by `stream`, `analyze`, and `verify` — anything that reads inputs.
#[derive(Args, Debug, Clone)]
pub struct InputArgs {
    /// Input file(s). '-' or omitted reads from stdin.
    #[arg(value_name = "INPUT", default_value = "-")]
    pub inputs: Vec<PathBuf>,
}

#[derive(Args, Debug)]
pub struct StreamArgs {
    #[command(flatten)]
    pub input: InputArgs,

    /// Shuffle mode
    #[arg(
        short = 's',
        long,
        value_enum,
        default_value_t = ShuffleMode::ChunkShuffled,
        value_name = "MODE",
    )]
    pub shuffle: ShuffleMode,

    /// Reproducibility seed (env: SHUFLR_SEED)
    #[arg(long, env = "SHUFLR_SEED", value_name = "U64")]
    pub seed: Option<u64>,

    /// Stop after N records (per epoch)
    #[arg(short = 'n', long, value_name = "N")]
    pub sample: Option<u64>,

    /// Passes over the input (0 = infinite)
    #[arg(short = 'e', long, default_value_t = 1, value_name = "N")]
    pub epochs: u64,

    /// Rank index for distributed partitioning (0-based)
    #[arg(long, requires = "world_size", value_name = "R")]
    pub rank: Option<u32>,

    /// Total rank count; used with --rank
    #[arg(long, requires = "rank", value_name = "W")]
    pub world_size: Option<u32>,

    /// Policy for records that exceed --max-line
    #[arg(long, value_enum, default_value_t = OnErrorPolicy::Skip, value_name = "POLICY")]
    pub on_error: OnErrorPolicy,

    /// Per-record byte cap
    #[arg(long, default_value = "16MiB", value_parser = parse_bytes, value_name = "BYTES")]
    pub max_line: u64,

    /// Ring size for --shuffle=buffer (displacement bound)
    #[arg(long, default_value_t = 100_000, value_name = "K")]
    pub buffer_size: u64,

    /// Number of records for --shuffle=reservoir (Vitter Algorithm R)
    #[arg(long, default_value_t = 10_000, value_name = "K")]
    pub reservoir_size: u64,

    /// Progress bar visibility
    #[arg(long, value_enum, default_value_t = When::Auto, value_name = "WHEN")]
    pub progress: When,

    /// Logging level (or $SHUFLR_LOG)
    #[arg(long, env = "SHUFLR_LOG", default_value = "info", value_name = "LEVEL")]
    pub log_level: String,
}

#[cfg(feature = "grpc")]
#[derive(Args, Debug)]
pub struct ServeArgs {
    /// gRPC listener address (e.g. grpc+tcp://127.0.0.1:50051 or grpc+unix:///path)
    #[arg(long, value_name = "ADDR")]
    pub grpc: String,

    /// Map dataset_id to server-local path; repeatable
    #[arg(long = "dataset", value_name = "ID=PATH")]
    pub datasets: Vec<String>,

    /// Allow non-loopback bind (requires --tls-cert/--tls-key)
    #[arg(long)]
    pub bind_public: bool,

    /// Logging level (or $SHUFLR_LOG)
    #[arg(long, env = "SHUFLR_LOG", default_value = "info", value_name = "LEVEL")]
    pub log_level: String,
}

#[cfg(feature = "zstd")]
#[derive(Args, Debug)]
pub struct ConvertArgs {
    #[command(flatten)]
    pub input: InputArgs,

    /// Output path; '-' writes to stdout
    #[arg(short = 'o', long, value_name = "PATH")]
    pub output: PathBuf,

    /// zstd compression level (1..=22)
    #[arg(short = 'l', long, default_value_t = 3, value_parser = clap::value_parser!(u32).range(1..=22))]
    pub level: u32,

    /// Target frame size (record-aligned, approximate)
    #[arg(short = 'f', long, default_value = "2MiB", value_parser = parse_bytes, value_name = "BYTES")]
    pub frame_size: u64,

    /// Compression thread count (0 = cpu count)
    #[arg(short = 'T', long, default_value_t = 0, value_name = "N")]
    pub threads: u32,

    /// Override input-format auto-detect
    #[arg(long, value_enum, default_value_t = InputFormat::Auto)]
    pub input_format: InputFormat,

    /// Disable XXH64 per-frame checksums
    #[arg(long)]
    pub no_checksum: bool,

    /// Disable record-aligned frames (saves ~3% ratio; breaks chunk-shuffled compat)
    #[arg(long)]
    pub no_record_align: bool,

    /// After writing, re-read and verify (adds ~30% runtime)
    #[arg(long)]
    pub verify: bool,

    /// Stop after N records are written (head-sample mode). Combines with
    /// --sample-rate: stop after N records have been accepted by the
    /// Bernoulli filter.
    #[arg(short = 'n', long, value_name = "N")]
    pub limit: Option<u64>,

    /// Bernoulli filter: include each input record with probability P
    /// (0.0..=1.0). Stream-friendly — runs until EOF (or --limit).
    /// For exactly-K uniform sampling use `shuflr stream --shuffle=reservoir`
    /// piped into convert.
    #[arg(long, value_name = "P", value_parser = parse_probability)]
    pub sample_rate: Option<f64>,

    /// Drop records with Shannon entropy below this many bits-per-byte.
    /// Useful for kicking out boilerplate (typical boilerplate < 2 bits;
    /// English text ~4 bits; random data 8 bits). Units: bits. Max 8.
    #[arg(long, value_name = "BITS", value_parser = parse_entropy_bits)]
    pub min_entropy: Option<f64>,

    /// Drop records with Shannon entropy above this many bits-per-byte.
    /// Useful for flagging binary/garbage records (>7 bits typical).
    #[arg(long, value_name = "BITS", value_parser = parse_entropy_bits)]
    pub max_entropy: Option<f64>,

    /// Reproducibility seed for --sample-rate. Inherits SHUFLR_SEED if set.
    #[arg(long, env = "SHUFLR_SEED", value_name = "U64")]
    pub seed: Option<u64>,

    /// Progress bar visibility
    #[arg(long, value_enum, default_value_t = When::Auto, value_name = "WHEN")]
    pub progress: When,

    /// Logging level (or $SHUFLR_LOG)
    #[arg(long, env = "SHUFLR_LOG", default_value = "info", value_name = "LEVEL")]
    pub log_level: String,
}

#[cfg(feature = "zstd")]
#[derive(Args, Debug)]
pub struct InfoArgs {
    /// Input file
    #[arg(value_name = "FILE")]
    pub input: PathBuf,

    /// Emit machine-readable JSON instead of a human summary
    #[arg(long)]
    pub json: bool,
}

#[derive(Args, Debug)]
pub struct AnalyzeArgs {
    #[command(flatten)]
    pub input: InputArgs,

    /// Sample this many chunks for distribution checks
    #[arg(long, default_value_t = 32, value_name = "N")]
    pub sample_chunks: u32,

    /// Exit 3 on unsafe verdict (for scripting); default exits 0 with warning
    #[arg(long)]
    pub strict: bool,

    /// Emit machine-readable JSON instead of a human summary
    #[arg(long)]
    pub json: bool,
}

#[derive(Args, Debug)]
pub struct IndexArgs {
    #[command(flatten)]
    pub input: InputArgs,

    /// Output index path (default: <input>.shuflr-idx)
    #[arg(short = 'o', long, value_name = "PATH")]
    pub output: Option<PathBuf>,
}

#[derive(Args, Debug)]
pub struct VerifyArgs {
    #[command(flatten)]
    pub input: InputArgs,

    /// Also parse each record as JSON (depth cap 128)
    #[arg(long)]
    pub deep: bool,
}

#[derive(Args, Debug)]
pub struct CompletionsArgs {
    /// Target shell
    #[arg(value_enum)]
    pub shell: Shell,
}

#[derive(Args, Debug)]
pub struct ManArgs {
    /// Subcommand to document. Omit for the top-level page.
    #[arg(value_name = "SUBCOMMAND")]
    pub subcommand: Option<String>,
}

#[derive(Copy, Clone, Debug, ValueEnum, PartialEq, Eq)]
pub enum ShuffleMode {
    /// Pass-through, no shuffle
    None,
    /// Round-robin across chunks (fastest shuffled; lowest quality)
    ChunkRr,
    /// Intra-chunk Fisher-Yates + weighted interleave (default)
    ChunkShuffled,
    /// Provably uniform permutation via persisted byte-offset index
    IndexPerm,
    /// K-buffer local shuffle (displacement bounded by K)
    Buffer,
    /// Vitter Algorithm R; emits exactly K records
    Reservoir,
}

#[derive(Copy, Clone, Debug, ValueEnum, PartialEq, Eq)]
pub enum OnErrorPolicy {
    /// Drop malformed/oversized records and count them (default)
    Skip,
    /// Abort on first offending record
    Fail,
    /// Emit the record anyway
    Passthrough,
}

impl From<OnErrorPolicy> for shuflr::OnError {
    fn from(p: OnErrorPolicy) -> Self {
        match p {
            OnErrorPolicy::Skip => shuflr::OnError::Skip,
            OnErrorPolicy::Fail => shuflr::OnError::Fail,
            OnErrorPolicy::Passthrough => shuflr::OnError::Passthrough,
        }
    }
}

#[derive(Copy, Clone, Debug, ValueEnum, PartialEq, Eq)]
pub enum When {
    Never,
    Auto,
    Always,
}

#[derive(Copy, Clone, Debug, ValueEnum, PartialEq, Eq)]
pub enum InputFormat {
    Auto,
    Plain,
    Gzip,
    Zstd,
    #[cfg(feature = "bzip2")]
    Bz2,
    #[cfg(feature = "xz")]
    Xz,
}

/// Parse a bits-per-byte entropy in `[0.0, 8.0]` (Shannon, log2 base —
/// 8 bits is the maximum for a uniform-random byte stream).
fn parse_entropy_bits(raw: &str) -> std::result::Result<f64, String> {
    let h: f64 = raw
        .parse()
        .map_err(|e| format!("invalid entropy '{raw}': {e}"))?;
    if !(0.0..=8.0).contains(&h) {
        return Err(format!(
            "entropy {h} bits is outside [0, 8] (max entropy for a byte is 8 bits)"
        ));
    }
    Ok(h)
}

/// Parse a probability in `[0.0, 1.0]`. Rejects values outside that range
/// with a clear message (rather than silently clamping).
fn parse_probability(raw: &str) -> std::result::Result<f64, String> {
    let p: f64 = raw
        .parse()
        .map_err(|e| format!("invalid probability '{raw}': {e}"))?;
    if !(0.0..=1.0).contains(&p) {
        return Err(format!(
            "probability {p} is outside [0.0, 1.0] (pass e.g. 0.01 for 1%)"
        ));
    }
    Ok(p)
}

/// Parse a byte count like `16MiB`, `2MB`, `1024` into bytes.
pub(crate) fn parse_bytes(raw: &str) -> std::result::Result<u64, String> {
    let raw = raw.trim();
    let (num, suffix) = raw
        .find(|c: char| !c.is_ascii_digit() && c != '.')
        .map(|i| raw.split_at(i))
        .unwrap_or((raw, ""));
    let num: f64 = num
        .parse()
        .map_err(|e| format!("invalid number '{num}': {e}"))?;
    let mult: u64 = match suffix.trim().to_ascii_lowercase().as_str() {
        "" | "b" => 1,
        "k" | "kb" => 1_000,
        "ki" | "kib" => 1 << 10,
        "m" | "mb" => 1_000_000,
        "mi" | "mib" => 1 << 20,
        "g" | "gb" => 1_000_000_000,
        "gi" | "gib" => 1 << 30,
        "t" | "tb" => 1_000_000_000_000,
        "ti" | "tib" => 1 << 40,
        other => return Err(format!("unknown byte suffix '{other}' (try KiB, MiB, GiB)")),
    };
    Ok((num * mult as f64) as u64)
}

/// Parse args, rewriting implicit-stream invocations.
///
/// If argv[1] is not a known subcommand, flag, or help marker, we insert
/// `"stream"` at position 1. This makes `shuflr file.jsonl` shorthand for
/// `shuflr stream file.jsonl`.
pub fn parse() -> Cli {
    let raw: Vec<std::ffi::OsString> = std::env::args_os().collect();
    Cli::parse_from(rewrite_implicit_stream(raw))
}

fn rewrite_implicit_stream(mut argv: Vec<std::ffi::OsString>) -> Vec<std::ffi::OsString> {
    if argv.len() < 2 {
        return argv;
    }
    // If any arg is a known subcommand name, trust the user; no rewrite.
    let explicit_subcommand = argv[1..].iter().any(|a| {
        let s = a.to_string_lossy();
        SUBCOMMAND_NAMES.contains(&s.as_ref())
    });
    if explicit_subcommand {
        return argv;
    }
    // If the user only asked for a top-level help/version marker, let clap handle it.
    let only_top_level_pass = argv[1..].iter().all(|a| {
        matches!(
            a.to_string_lossy().as_ref(),
            "--help" | "-h" | "--help-full" | "--version" | "-V"
        )
    });
    if only_top_level_pass {
        return argv;
    }
    argv.insert(1, std::ffi::OsString::from("stream"));
    argv
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn implicit_stream_bare_file() {
        let in_ = vec!["shuflr".into(), "data.jsonl".into()];
        let out = rewrite_implicit_stream(in_);
        assert_eq!(
            out,
            vec!["shuflr", "stream", "data.jsonl"]
                .into_iter()
                .map(std::ffi::OsString::from)
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn explicit_subcommand_preserved() {
        let in_ = vec!["shuflr".into(), "verify".into(), "x.jsonl".into()];
        let out = rewrite_implicit_stream(in_.clone());
        assert_eq!(out, in_);
    }

    #[test]
    fn top_level_flag_preserved() {
        let in_ = vec!["shuflr".into(), "--version".into()];
        let out = rewrite_implicit_stream(in_.clone());
        assert_eq!(out, in_);
    }

    #[test]
    fn implicit_stream_with_leading_flag() {
        // `shuflr --shuffle none data.jsonl` should become `shuflr stream --shuffle none data.jsonl`
        let in_ = vec![
            "shuflr".into(),
            "--shuffle".into(),
            "none".into(),
            "data.jsonl".into(),
        ];
        let out = rewrite_implicit_stream(in_);
        assert_eq!(
            out,
            vec!["shuflr", "stream", "--shuffle", "none", "data.jsonl"]
                .into_iter()
                .map(std::ffi::OsString::from)
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn parse_bytes_variants() {
        assert_eq!(parse_bytes("1024").unwrap(), 1024);
        assert_eq!(parse_bytes("16KiB").unwrap(), 16 << 10);
        assert_eq!(parse_bytes("2MiB").unwrap(), 2 << 20);
        assert_eq!(parse_bytes("1GB").unwrap(), 1_000_000_000);
        assert_eq!(
            parse_bytes("1.5MiB").unwrap(),
            (1.5 * (1 << 20) as f64) as u64
        );
        assert!(parse_bytes("2zz").is_err());
    }
}
