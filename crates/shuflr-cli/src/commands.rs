//! Subcommand handlers.
//!
//! `stream` is wired end-to-end for `--shuffle=none`. Everything else
//! is a stub that prints a pointer to the design-doc section covering
//! it and returns a typed exit code.

use std::io::{self, Write as _};
use std::time::Instant;

use crate::cli;
use crate::exit;

pub fn stream(args: cli::StreamArgs) -> exit::Code {
    match stream_inner(args) {
        Ok(()) => exit::Code::Ok,
        Err(e) => report_library_error(&e),
    }
}

fn stream_inner(args: cli::StreamArgs) -> shuflr::Result<()> {
    // PR-2 scope: --shuffle=none only. Other modes return Unimplemented from the
    // dispatch layer below, so this path is unreachable for them.
    debug_assert_eq!(args.shuffle, cli::ShuffleMode::None);

    if args.input.inputs.len() > 1 {
        tracing::warn!(
            "PR-2 concatenates multiple inputs in order; chunk/index modes are not yet available"
        );
    }

    let total_start = Instant::now();
    let stdout = io::stdout();
    let mut sink = stdout.lock();
    let mut total = shuflr::Stats::default();

    for path in &args.input.inputs {
        let input = shuflr::io::Input::open(path)?;
        let size_hint = input.size_hint();
        tracing::info!(
            path = %path.display(),
            bytes = size_hint.unwrap_or(0),
            format = ?input.format(),
            "opened input",
        );

        let cfg = shuflr::pipeline::PassthroughConfig {
            max_line: args.max_line,
            on_error: args.on_error.into(),
            sample: remaining_sample(args.sample, &total),
            ensure_trailing_newline: true,
        };

        let started = Instant::now();
        let stats = shuflr::pipeline::passthrough(input, &mut sink, &cfg)?;
        let elapsed = started.elapsed();

        tracing::info!(
            path = %path.display(),
            records_in = stats.records_in,
            records_out = stats.records_out,
            bytes_in = stats.bytes_in,
            bytes_out = stats.bytes_out,
            oversized_skipped = stats.oversized_skipped,
            oversized_passthrough = stats.oversized_passthrough,
            throughput_mb_s = mbs(stats.bytes_in, elapsed),
            elapsed_ms = elapsed.as_millis() as u64,
            "finished input",
        );

        accumulate(&mut total, &stats);

        // Honor --sample across the concatenated logical stream.
        if let Some(cap) = args.sample
            && total.records_out >= cap
        {
            break;
        }
    }

    let elapsed = total_start.elapsed();
    tracing::info!(
        records_in = total.records_in,
        records_out = total.records_out,
        bytes_in = total.bytes_in,
        bytes_out = total.bytes_out,
        throughput_mb_s = mbs(total.bytes_in, elapsed),
        elapsed_ms = elapsed.as_millis() as u64,
        "done",
    );

    sink.flush().map_err(shuflr::Error::Io)?;
    Ok(())
}

fn remaining_sample(sample: Option<u64>, so_far: &shuflr::Stats) -> Option<u64> {
    sample.map(|cap| cap.saturating_sub(so_far.records_out))
}

fn accumulate(total: &mut shuflr::Stats, step: &shuflr::Stats) {
    total.records_in += step.records_in;
    total.records_out += step.records_out;
    total.bytes_in += step.bytes_in;
    total.bytes_out += step.bytes_out;
    total.oversized_skipped += step.oversized_skipped;
    total.oversized_passthrough += step.oversized_passthrough;
    total.had_trailing_partial |= step.had_trailing_partial;
}

fn mbs(bytes: u64, elapsed: std::time::Duration) -> f64 {
    let secs = elapsed.as_secs_f64();
    if secs == 0.0 {
        return 0.0;
    }
    (bytes as f64) / secs / 1_048_576.0
}

fn report_library_error(e: &shuflr::Error) -> exit::Code {
    let _ = writeln!(io::stderr(), "shuflr: error: {e}");
    match e {
        shuflr::Error::Io(err) => match err.kind() {
            io::ErrorKind::NotFound => exit::Code::NoInput,
            io::ErrorKind::PermissionDenied => exit::Code::NoPerm,
            _ => exit::Code::Software,
        },
        shuflr::Error::NotFound { .. } => exit::Code::NoInput,
        shuflr::Error::PermissionDenied { .. } => exit::Code::NoPerm,
        shuflr::Error::Input(_) => exit::Code::Usage,
        shuflr::Error::OversizedRecord { .. } => exit::Code::DataErr,
        shuflr::Error::InputChanged(_) => exit::Code::Software,
        shuflr::Error::CompressedInputUnsupported { .. } => exit::Code::Usage,
    }
}

/// Route non-`None` shuffle modes to the stream handler; others stub out.
pub fn stream_dispatch(args: cli::StreamArgs) -> exit::Code {
    if args.shuffle == cli::ShuffleMode::None {
        return stream(args);
    }
    stub(
        "stream",
        format!(
            "--shuffle={:?} is not yet implemented. PR-2 ships --shuffle=none; \
             see docs/design/002 §2 and 004 §9 for the roadmap.",
            args.shuffle
        ),
    )
}

#[cfg(feature = "grpc")]
pub fn serve(_args: cli::ServeArgs) -> exit::Code {
    stub(
        "serve",
        "002 §7 (shuflr.v1 proto); deferred to a post-PR-7 milestone".into(),
    )
}

#[cfg(feature = "zstd")]
pub fn convert(_args: cli::ConvertArgs) -> exit::Code {
    stub(
        "convert",
        "004 §1–§4; targeted for PR-4 (record-aligned seekable writer)".into(),
    )
}

#[cfg(feature = "zstd")]
pub fn info(_args: cli::InfoArgs) -> exit::Code {
    stub(
        "info",
        "004 §5; lands with PR-4 alongside the seekable writer".into(),
    )
}

pub fn analyze(_args: cli::AnalyzeArgs) -> exit::Code {
    stub(
        "analyze",
        "002 §6.4; lands after PR-3 (streaming inputs)".into(),
    )
}

pub fn index(_args: cli::IndexArgs) -> exit::Code {
    stub(
        "index",
        "002 §2.2 (index-perm); lands with PR-8 (provably uniform mode)".into(),
    )
}

pub fn verify(_args: cli::VerifyArgs) -> exit::Code {
    stub(
        "verify",
        "002 §11.2 (edge-case matrix); lands with PR-3 (streaming framing)".into(),
    )
}

pub fn completions(args: cli::CompletionsArgs) -> exit::Code {
    use clap::CommandFactory as _;
    let mut cmd = cli::Cli::command();
    let name = cmd.get_name().to_string();
    clap_complete::generate(args.shell, &mut cmd, name, &mut std::io::stdout());
    exit::Code::Ok
}

fn stub(name: &'static str, pointer: String) -> exit::Code {
    let _ = writeln!(
        std::io::stderr(),
        "shuflr: '{name}' is not yet implemented.\nsee docs/design/{pointer}"
    );
    exit::Code::Unimplemented
}
