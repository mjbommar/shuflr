//! `shuflr` — streaming shuffled JSONL.
//!
//! PR-2 scope: CLI dispatch + `--shuffle=none` serial passthrough on plain
//! JSONL files and stdin. Everything else is stubbed. See
//! `docs/design/002-revised-plan.md` and amendments 003/004 for the plan.

#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

mod cli;
mod commands;
mod exit;
mod progress;

use std::io::Write as _;
use std::process::ExitCode;

fn main() -> ExitCode {
    install_panic_hook();

    let parsed = cli::parse();
    install_logging(parsed.command.log_level());

    let code = match parsed.command {
        cli::Command::Stream(a) => commands::stream_dispatch(a),
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
        if std::env::var_os("RUST_BACKTRACE").is_some() {
            default(info);
        }
    }));
}

/// Install a tracing subscriber with a filter derived from (in order of priority):
/// 1. `SHUFLR_LOG` env var (EnvFilter syntax — supports `info,hyper=warn` etc.)
/// 2. `RUST_LOG` (standard convention; inherited from ecosystem tools)
/// 3. The `--log-level` flag for the active subcommand
fn install_logging(flag_default: &str) {
    use tracing_subscriber::{EnvFilter, fmt};

    let filter = EnvFilter::try_from_env("SHUFLR_LOG")
        .or_else(|_| EnvFilter::try_from_default_env())
        .unwrap_or_else(|_| EnvFilter::new(flag_default));

    let _ = fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .with_target(false)
        .try_init();
}
