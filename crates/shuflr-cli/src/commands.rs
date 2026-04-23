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
            format = ?input.raw_format(),
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

/// Route supported shuffle modes to their handlers; others stub out.
pub fn stream_dispatch(args: cli::StreamArgs) -> exit::Code {
    match args.shuffle {
        cli::ShuffleMode::None => stream(args),
        cli::ShuffleMode::Buffer => match stream_buffer_inner(args) {
            Ok(()) => exit::Code::Ok,
            Err(e) => report_library_error(&e),
        },
        #[cfg(feature = "zstd")]
        cli::ShuffleMode::ChunkShuffled => match stream_chunk_shuffled_inner(args) {
            Ok(()) => exit::Code::Ok,
            Err(e) => report_library_error(&e),
        },
        other => stub(
            "stream",
            format!(
                "--shuffle={other:?} is not yet implemented. Supported modes so far: \
                 none, buffer, chunk-shuffled (seekable .zst only). See docs/design/002 \
                 §2 and 004 §9 for the roadmap."
            ),
        ),
    }
}

#[cfg(feature = "zstd")]
fn stream_chunk_shuffled_inner(args: cli::StreamArgs) -> shuflr::Result<()> {
    if args.input.inputs.len() != 1 {
        return Err(shuflr::Error::Input(
            "--shuffle=chunk-shuffled currently takes exactly one seekable-zstd input. \
             Multi-input and weighted-mix support land with PR-8."
                .into(),
        ));
    }
    let path = &args.input.inputs[0];
    if path == std::path::Path::new("-") {
        return Err(shuflr::Error::Input(
            "--shuffle=chunk-shuffled requires a seekable file; stdin is not seekable. \
             Either save your stream to disk and re-invoke, or use --shuffle=buffer:K."
                .into(),
        ));
    }

    // Reader rejects non-seekable inputs with a clear error.
    let reader = shuflr::io::zstd_seekable::SeekableReader::open(path).map_err(|e| {
        match e {
            // Wrap unhelpful Io errors with a user-friendly message.
            shuflr::Error::Io(io_err) => shuflr::Error::Input(format!(
                "'{}' is not a seekable-zstd file ({io_err}). \
                 Run `shuflr convert {}` first, or use --shuffle=buffer:K.",
                path.display(),
                path.display(),
            )),
            other => other,
        }
    })?;

    let seed = args.seed.unwrap_or(0);
    if args.seed.is_none() {
        tracing::info!(seed, "no --seed given; using default");
    }
    tracing::info!(
        path = %path.display(),
        frames = reader.num_frames(),
        total_decompressed = reader.total_decompressed(),
        seed,
        epochs = args.epochs,
        "opened seekable input (chunk-shuffled)",
    );

    let stdout = io::stdout();
    let mut sink = stdout.lock();
    let mut total = shuflr::Stats::default();
    let epochs_cap = if args.epochs == 0 {
        u64::MAX
    } else {
        args.epochs
    };
    let total_start = Instant::now();

    for epoch in 0..epochs_cap {
        // Reopen reader each epoch so we get a fresh file-position state.
        // (The reader can be reused; the CLI keeps it simple.)
        let reader = shuflr::io::zstd_seekable::SeekableReader::open(path).map_err(|e| {
            shuflr::Error::Input(format!(
                "reopening '{}' for epoch {epoch}: {e}",
                path.display()
            ))
        })?;
        let cfg = shuflr::pipeline::ChunkShuffledConfig {
            seed,
            epoch,
            max_line: args.max_line,
            on_error: args.on_error.into(),
            sample: remaining_sample(args.sample, &total),
            ensure_trailing_newline: true,
        };
        let started = Instant::now();
        let stats = shuflr::pipeline::chunk_shuffled(reader, &mut sink, &cfg)?;
        let elapsed = started.elapsed();
        tracing::info!(
            epoch,
            records_in = stats.records_in,
            records_out = stats.records_out,
            bytes_in = stats.bytes_in,
            bytes_out = stats.bytes_out,
            throughput_mb_s = mbs(stats.bytes_in, elapsed),
            elapsed_ms = elapsed.as_millis() as u64,
            "chunk-shuffled epoch done",
        );
        accumulate(&mut total, &stats);
        if let Some(cap) = args.sample
            && total.records_out >= cap
        {
            break;
        }
    }

    let elapsed = total_start.elapsed();
    tracing::info!(
        records_out = total.records_out,
        throughput_mb_s = mbs(total.bytes_in, elapsed),
        elapsed_ms = elapsed.as_millis() as u64,
        "chunk-shuffled done",
    );
    sink.flush().map_err(shuflr::Error::Io)?;
    Ok(())
}

fn stream_buffer_inner(args: cli::StreamArgs) -> shuflr::Result<()> {
    if args.input.inputs.len() > 1 {
        tracing::warn!(
            "PR-5 concatenates multiple inputs for --shuffle=buffer; chunked modes land in PR-7"
        );
    }
    let total_start = Instant::now();
    let stdout = io::stdout();
    let mut sink = stdout.lock();
    let mut total = shuflr::Stats::default();

    let buffer_size = usize::try_from(args.buffer_size).map_err(|_| {
        shuflr::Error::Input(format!(
            "--buffer-size {} too large for this build",
            args.buffer_size
        ))
    })?;
    if buffer_size == 0 {
        return Err(shuflr::Error::Input(
            "--buffer-size must be at least 1".into(),
        ));
    }

    // Default seed (if the user didn't pass --seed) is 0. Log it so the run
    // is reproducible after the fact.
    let seed = args.seed.unwrap_or(0);
    if args.seed.is_none() {
        tracing::info!(seed, "no --seed given; using default");
    }

    for path in &args.input.inputs {
        let input = shuflr::io::Input::open(path)?;
        tracing::info!(
            path = %path.display(),
            bytes = input.size_hint().unwrap_or(0),
            raw_format = ?input.raw_format(),
            buffer_size,
            seed,
            "opened input (buffer-shuffle)",
        );

        let cfg = shuflr::pipeline::BufferConfig {
            buffer_size,
            seed,
            max_line: args.max_line,
            on_error: args.on_error.into(),
            sample: remaining_sample(args.sample, &total),
            ensure_trailing_newline: true,
        };

        let started = Instant::now();
        let stats = shuflr::pipeline::buffer(input, &mut sink, &cfg)?;
        let elapsed = started.elapsed();
        tracing::info!(
            path = %path.display(),
            records_in = stats.records_in,
            records_out = stats.records_out,
            bytes_in = stats.bytes_in,
            bytes_out = stats.bytes_out,
            throughput_mb_s = mbs(stats.bytes_in, elapsed),
            elapsed_ms = elapsed.as_millis() as u64,
            "buffer-shuffle finished input",
        );
        accumulate(&mut total, &stats);
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
        throughput_mb_s = mbs(total.bytes_in, elapsed),
        elapsed_ms = elapsed.as_millis() as u64,
        "buffer-shuffle done",
    );

    sink.flush().map_err(shuflr::Error::Io)?;
    Ok(())
}

#[cfg(feature = "grpc")]
pub fn serve(_args: cli::ServeArgs) -> exit::Code {
    stub(
        "serve",
        "002 §7 (shuflr.v1 proto); deferred to a post-PR-7 milestone".into(),
    )
}

#[cfg(feature = "zstd")]
pub fn convert(args: cli::ConvertArgs) -> exit::Code {
    match convert_inner(args) {
        Ok(()) => exit::Code::Ok,
        Err(e) => report_library_error(&e),
    }
}

#[cfg(feature = "zstd")]
fn convert_inner(args: cli::ConvertArgs) -> shuflr::Result<()> {
    use shuflr::io::zstd_seekable::{Writer, WriterConfig};
    use std::io::{BufReader, Read};
    use std::time::Instant;

    if args.input.inputs.len() != 1 {
        return Err(shuflr::Error::Input(
            "PR-4 `convert` accepts exactly one input; multi-input merge \
             lands in PR-5. Use '-' to read from stdin."
                .to_string(),
        ));
    }
    let in_path = &args.input.inputs[0];
    let frame_size = usize::try_from(args.frame_size).map_err(|_| {
        shuflr::Error::Input(format!(
            "--frame-size {} too large for this build",
            args.frame_size
        ))
    })?;

    let input = shuflr::io::Input::open(in_path)?;
    tracing::info!(
        path = %in_path.display(),
        raw_format = ?input.raw_format(),
        frame_size,
        level = args.level,
        record_aligned = !args.no_record_align,
        checksums = !args.no_checksum,
        "opened input for convert",
    );

    // Open the output sink. Not Send-bounded because convert is
    // single-threaded in PR-4; multithreaded compress lands in PR-5.
    let output: Box<dyn std::io::Write> = if args.output == std::path::Path::new("-") {
        Box::new(std::io::stdout().lock())
    } else {
        let file = std::fs::File::create(&args.output).map_err(shuflr::Error::Io)?;
        Box::new(file)
    };

    let cfg = WriterConfig {
        level: args.level as i32,
        frame_size,
        checksums: !args.no_checksum,
        record_aligned: !args.no_record_align,
    };
    let mut writer = Writer::new(output, cfg);

    let started = Instant::now();
    let mut reader = BufReader::with_capacity(2 * 1024 * 1024, input);
    let mut buf = vec![0u8; 2 * 1024 * 1024];
    loop {
        let n = reader.read(&mut buf).map_err(shuflr::Error::Io)?;
        if n == 0 {
            break;
        }
        writer.write_block(&buf[..n])?;
    }
    let stats = writer.finish()?;
    let elapsed = started.elapsed();

    let ratio = if stats.uncompressed_bytes > 0 {
        stats.compressed_bytes as f64 / stats.uncompressed_bytes as f64
    } else {
        0.0
    };
    tracing::info!(
        output = %args.output.display(),
        frames = stats.frames,
        records = stats.records,
        uncompressed_bytes = stats.uncompressed_bytes,
        compressed_bytes = stats.compressed_bytes,
        seek_table_bytes = stats.seek_table_bytes,
        ratio = ratio,
        throughput_mb_s = (stats.uncompressed_bytes as f64) / elapsed.as_secs_f64() / 1_048_576.0,
        elapsed_ms = elapsed.as_millis() as u64,
        "convert done",
    );

    if args.verify {
        tracing::warn!("--verify is a PR-5 feature; skipping post-write verification");
    }

    Ok(())
}

#[cfg(feature = "zstd")]
pub fn info(args: cli::InfoArgs) -> exit::Code {
    match info_inner(args) {
        Ok(()) => exit::Code::Ok,
        Err(e) => report_library_error(&e),
    }
}

#[cfg(feature = "zstd")]
fn info_inner(args: cli::InfoArgs) -> shuflr::Result<()> {
    use shuflr::io::zstd_seekable::SeekTable;
    use std::io::Write as _;

    let mut file = std::fs::File::open(&args.input).map_err(|e| match e.kind() {
        std::io::ErrorKind::NotFound => shuflr::Error::NotFound {
            path: args.input.display().to_string(),
        },
        std::io::ErrorKind::PermissionDenied => shuflr::Error::PermissionDenied {
            path: args.input.display().to_string(),
        },
        _ => shuflr::Error::Io(e),
    })?;
    let total_size = file.metadata().map(|m| m.len()).unwrap_or(0);

    let table = SeekTable::read_from_tail(&mut file).map_err(shuflr::Error::Io)?;
    let total_decompressed = table.total_decompressed();
    let total_compressed_payload = table.total_compressed();
    let ratio = if total_compressed_payload > 0 {
        total_decompressed as f64 / total_compressed_payload as f64
    } else {
        0.0
    };

    let mut stdout = std::io::stdout().lock();
    if args.json {
        let mut sizes: Vec<u32> = table.entries.iter().map(|e| e.decompressed_size).collect();
        let min_frame = sizes.iter().min().copied().unwrap_or(0);
        let max_frame = sizes.iter().max().copied().unwrap_or(0);
        let med_frame = median(&mut sizes);
        writeln!(
            stdout,
            "{{\
\"file\":\"{path}\",\
\"format\":\"zstd-seekable\",\
\"frames\":{frames},\
\"compressed_bytes\":{compressed},\
\"decompressed_bytes\":{decompressed},\
\"ratio\":{ratio:.3},\
\"checksums\":{checksums},\
\"frame_size_min\":{min_frame},\
\"frame_size_max\":{max_frame},\
\"frame_size_median\":{med_frame}\
}}",
            path = args.input.display(),
            frames = table.num_frames(),
            compressed = total_size,
            decompressed = total_decompressed,
            ratio = ratio,
            checksums = table.with_checksums,
        )
        .map_err(shuflr::Error::Io)?;
    } else {
        writeln!(stdout, "file:           {}", args.input.display()).map_err(shuflr::Error::Io)?;
        writeln!(stdout, "format:         zstd-seekable").map_err(shuflr::Error::Io)?;
        writeln!(stdout, "frames:         {}", table.num_frames()).map_err(shuflr::Error::Io)?;
        writeln!(stdout, "compressed:     {}", humanize_bytes(total_size))
            .map_err(shuflr::Error::Io)?;
        writeln!(
            stdout,
            "decompressed:   {} (ratio {ratio:.2})",
            humanize_bytes(total_decompressed),
        )
        .map_err(shuflr::Error::Io)?;
        if !table.entries.is_empty() {
            let mut sizes: Vec<u32> = table.entries.iter().map(|e| e.decompressed_size).collect();
            let min_frame = *sizes.iter().min().unwrap_or(&0);
            let max_frame = *sizes.iter().max().unwrap_or(&0);
            let med_frame = median(&mut sizes);
            writeln!(
                stdout,
                "frame size:     min {}, median {}, max {}",
                humanize_bytes(min_frame as u64),
                humanize_bytes(med_frame as u64),
                humanize_bytes(max_frame as u64),
            )
            .map_err(shuflr::Error::Io)?;
        }
        writeln!(
            stdout,
            "checksum:       {}",
            if table.with_checksums {
                "XXH64 per frame"
            } else {
                "none"
            }
        )
        .map_err(shuflr::Error::Io)?;
        writeln!(
            stdout,
            "seekable:       yes (direct chunk-shuffled / index-perm compatible after PR-7)"
        )
        .map_err(shuflr::Error::Io)?;
    }
    Ok(())
}

fn median(sizes: &mut [u32]) -> u32 {
    if sizes.is_empty() {
        return 0;
    }
    sizes.sort_unstable();
    sizes[sizes.len() / 2]
}

fn humanize_bytes(n: u64) -> String {
    const UNITS: &[(&str, u64)] = &[("GiB", 1 << 30), ("MiB", 1 << 20), ("KiB", 1 << 10)];
    for (name, size) in UNITS {
        if n >= *size {
            return format!("{:.2} {}", n as f64 / *size as f64, name);
        }
    }
    format!("{n} B")
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
