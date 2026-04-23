//! Subcommand handlers.
//!
//! Every handler in PR-1 is a stub that emits a "not yet implemented"
//! message pointing at the design-doc section responsible for it, and
//! returns a typed exit code. Implementation lands in subsequent PRs
//! per 004 §9 (Implementation Order).

use std::io::Write as _;

use crate::cli;
use crate::exit;

pub fn stream(_args: cli::StreamArgs) -> exit::Code {
    stub(
        "stream",
        "002 §2, §6.2; next: PR-2 (--shuffle=none + io::pread)",
    )
}

#[cfg(feature = "grpc")]
pub fn serve(_args: cli::ServeArgs) -> exit::Code {
    stub(
        "serve",
        "002 §7 (shuflr.v1 proto); deferred to a post-PR-7 milestone",
    )
}

#[cfg(feature = "zstd")]
pub fn convert(_args: cli::ConvertArgs) -> exit::Code {
    stub(
        "convert",
        "004 §1–§4; targeted for PR-4 (record-aligned seekable writer)",
    )
}

#[cfg(feature = "zstd")]
pub fn info(_args: cli::InfoArgs) -> exit::Code {
    stub(
        "info",
        "004 §5; lands with PR-4 alongside the seekable writer",
    )
}

pub fn analyze(_args: cli::AnalyzeArgs) -> exit::Code {
    stub("analyze", "002 §6.4; lands after PR-3 (streaming inputs)")
}

pub fn index(_args: cli::IndexArgs) -> exit::Code {
    stub(
        "index",
        "002 §2.2 (index-perm); lands with PR-8 (provably uniform mode)",
    )
}

pub fn verify(_args: cli::VerifyArgs) -> exit::Code {
    stub(
        "verify",
        "002 §11.2 (edge-case matrix); lands with PR-3 (streaming framing)",
    )
}

pub fn completions(args: cli::CompletionsArgs) -> exit::Code {
    use clap::CommandFactory as _;
    let mut cmd = cli::Cli::command();
    let name = cmd.get_name().to_string();
    clap_complete::generate(args.shell, &mut cmd, name, &mut std::io::stdout());
    exit::Code::Ok
}

fn stub(name: &'static str, pointer: &'static str) -> exit::Code {
    let _ = writeln!(
        std::io::stderr(),
        "shuflr: '{name}' is not yet implemented.\n\
         see docs/design/{pointer}"
    );
    exit::Code::Unimplemented
}
