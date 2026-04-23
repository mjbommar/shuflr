//! `shuflr` — streaming shuffled JSONL.
//!
//! PR-1 scope: argument parsing, subcommand dispatch, exit codes, and a
//! panic hook that keeps the failure mode boring. All subcommands are
//! stubbed; see `commands` and the design docs for what lands next.

#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

mod cli;
mod commands;
mod exit;

use std::io::Write as _;
use std::process::ExitCode;

fn main() -> ExitCode {
    install_panic_hook();
    install_logging();

    let parsed = cli::parse();
    let code = match parsed.command {
        cli::Command::Stream(a) => commands::stream(a),
        #[cfg(feature = "grpc")]
        cli::Command::Serve(a) => commands::serve(a),
        #[cfg(feature = "zstd")]
        cli::Command::Convert(a) => commands::convert(a),
        #[cfg(feature = "zstd")]
        cli::Command::Info(a) => commands::info(a),
        cli::Command::Analyze(a) => commands::analyze(a),
        cli::Command::Index(a) => commands::index(a),
        cli::Command::Verify(a) => commands::verify(a),
        cli::Command::Completions(a) => commands::completions(a),
    };
    code.into()
}

/// Route panics to a concise stderr line and exit 70 rather than spewing a backtrace.
/// Users pass `RUST_BACKTRACE=1` when they actually want the detail.
fn install_panic_hook() {
    let default = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let _ = writeln!(
            std::io::stderr(),
            "shuflr: internal error; please report at \
             https://github.com/mjbommar/shuflr/issues"
        );
        // Keep the original hook for RUST_BACKTRACE=1 users.
        if std::env::var_os("RUST_BACKTRACE").is_some() {
            default(info);
        }
    }));
}

/// Install a tracing subscriber that honors `SHUFLR_LOG` / `RUST_LOG`,
/// defaults to `info`, and writes to stderr so stdout stays clean.
fn install_logging() {
    use tracing_subscriber::{EnvFilter, fmt};

    let filter = EnvFilter::try_from_env("SHUFLR_LOG")
        .or_else(|_| EnvFilter::try_from_default_env())
        .unwrap_or_else(|_| EnvFilter::new("info"));

    let _ = fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .with_target(false)
        .try_init();
}
