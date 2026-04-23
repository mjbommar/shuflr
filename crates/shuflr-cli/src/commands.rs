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
            partition: partition_from_args(&args),
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

fn stream_reservoir_inner(args: cli::StreamArgs) -> shuflr::Result<()> {
    if args.input.inputs.len() > 1 {
        tracing::warn!("PR-11 reservoir concatenates multi-input in order");
    }
    let total_start = Instant::now();
    let stdout = io::stdout();
    let mut sink = stdout.lock();
    let mut total = shuflr::Stats::default();

    let k = usize::try_from(args.reservoir_size).map_err(|_| {
        shuflr::Error::Input(format!(
            "--reservoir-size {} too large for this build",
            args.reservoir_size
        ))
    })?;
    if k == 0 {
        return Err(shuflr::Error::Input(
            "--reservoir-size must be at least 1".into(),
        ));
    }
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
            reservoir_size = k,
            seed,
            "opened input (reservoir)",
        );
        let cfg = shuflr::pipeline::ReservoirConfig {
            k,
            seed,
            max_line: args.max_line,
            on_error: args.on_error.into(),
            ensure_trailing_newline: true,
            partition: partition_from_args(&args),
        };
        let started = Instant::now();
        let stats = shuflr::pipeline::reservoir(input, &mut sink, &cfg)?;
        let elapsed = started.elapsed();
        tracing::info!(
            records_in = stats.records_in,
            records_out = stats.records_out,
            throughput_mb_s = mbs(stats.bytes_in, elapsed),
            elapsed_ms = elapsed.as_millis() as u64,
            "reservoir finished input",
        );
        accumulate(&mut total, &stats);
    }
    let elapsed = total_start.elapsed();
    tracing::info!(
        records_out = total.records_out,
        elapsed_ms = elapsed.as_millis() as u64,
        "reservoir done",
    );
    sink.flush().map_err(shuflr::Error::Io)?;
    Ok(())
}

/// Extract the `(rank, world_size)` tuple from CLI args, if both are set.
fn partition_from_args(args: &cli::StreamArgs) -> Option<(u32, u32)> {
    match (args.rank, args.world_size) {
        (Some(r), Some(w)) if w > 1 && r < w => Some((r, w)),
        (Some(_), Some(1)) => None, // world_size=1 is effectively no partition
        _ => None,
    }
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
        cli::ShuffleMode::IndexPerm => match stream_index_perm_inner(args) {
            Ok(()) => exit::Code::Ok,
            Err(e) => report_library_error(&e),
        },
        cli::ShuffleMode::Reservoir => match stream_reservoir_inner(args) {
            Ok(()) => exit::Code::Ok,
            Err(e) => report_library_error(&e),
        },
        other => stub(
            "stream",
            format!(
                "--shuffle={other:?} is not yet implemented. Supported modes so far: \
                 none, buffer, chunk-shuffled, index-perm. See docs/design/002 §2 and \
                 004 §9 for the roadmap."
            ),
        ),
    }
}

fn stream_index_perm_inner(args: cli::StreamArgs) -> shuflr::Result<()> {
    if args.input.inputs.len() != 1 {
        return Err(shuflr::Error::Input(
            "--shuffle=index-perm accepts exactly one input file".into(),
        ));
    }
    let path = &args.input.inputs[0];
    if path == std::path::Path::new("-") {
        return Err(shuflr::Error::Input(
            "--shuffle=index-perm requires a seekable file; stdin is not seekable. \
             Use --shuffle=buffer:K or save the stream to disk first."
                .into(),
        ));
    }
    // Reject compressed inputs with a clear message.
    let probe = shuflr::io::Input::open(path)?;
    if probe.raw_format() != shuflr::io::magic::Format::Plain {
        return Err(shuflr::Error::Input(format!(
            "--shuffle=index-perm needs byte-offset random access, which is not possible \
             on {} input. Decompress to '.jsonl' first, or run `shuflr convert {}` and \
             use --shuffle=chunk-shuffled instead.",
            probe.raw_format().name(),
            path.display(),
        )));
    }
    drop(probe);

    let seed = args.seed.unwrap_or(0);
    if args.seed.is_none() {
        tracing::info!(seed, "no --seed given; using default");
    }

    let fingerprint = shuflr::Fingerprint::from_metadata(path)?;
    let sidecar = shuflr::index::sidecar_path(path);

    // Load index if sidecar exists and its fingerprint matches; otherwise
    // build, then save.
    let index = match shuflr::IndexFile::load(&sidecar) {
        Ok(loaded) if loaded.fingerprint == fingerprint => {
            tracing::info!(index = %sidecar.display(), records = loaded.count(), "loaded existing index");
            loaded
        }
        Ok(loaded) => {
            tracing::warn!(
                index = %sidecar.display(),
                recorded_fp = ?loaded.fingerprint.0,
                current_fp = ?fingerprint.0,
                "existing index fingerprint mismatches file metadata; rebuilding",
            );
            let built_start = Instant::now();
            let file = std::fs::File::open(path).map_err(shuflr::Error::Io)?;
            let idx = shuflr::IndexFile::build(file, fingerprint)?;
            let build_ms = built_start.elapsed().as_millis() as u64;
            match idx.save(&sidecar) {
                Ok(()) => {
                    tracing::info!(index = %sidecar.display(), records = idx.count(), build_ms, "index rebuilt")
                }
                Err(e) => {
                    tracing::warn!(err = %e, "failed to persist rebuilt index; continuing with in-memory copy")
                }
            }
            idx
        }
        Err(_) => {
            let built_start = Instant::now();
            let file = std::fs::File::open(path).map_err(shuflr::Error::Io)?;
            let idx = shuflr::IndexFile::build(file, fingerprint)?;
            let build_ms = built_start.elapsed().as_millis() as u64;
            match idx.save(&sidecar) {
                Ok(()) => {
                    tracing::info!(index = %sidecar.display(), records = idx.count(), build_ms, "index built + saved")
                }
                Err(e) => {
                    tracing::warn!(err = %e, "failed to persist index; continuing with in-memory copy")
                }
            }
            idx
        }
    };

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
        let cfg = shuflr::pipeline::IndexPermConfig {
            seed,
            epoch,
            sample: remaining_sample(args.sample, &total),
            ensure_trailing_newline: true,
            partition: partition_from_args(&args),
        };
        let started = Instant::now();
        let stats = shuflr::pipeline::index_perm(path, &index, &mut sink, &cfg)?;
        let elapsed = started.elapsed();
        tracing::info!(
            epoch,
            records_out = stats.records_out,
            bytes_out = stats.bytes_out,
            throughput_mb_s = mbs(stats.bytes_in, elapsed),
            elapsed_ms = elapsed.as_millis() as u64,
            "index-perm epoch done",
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
        "index-perm done",
    );
    sink.flush().map_err(shuflr::Error::Io)?;
    Ok(())
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
            partition: partition_from_args(&args),
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
            partition: partition_from_args(&args),
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
    use crate::progress;
    use shuflr::io::zstd_seekable::{ParallelConfig, Writer, WriterConfig, convert_parallel};
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
    let effective_threads = resolve_threads(args.threads as usize);
    let input_size = input.size_hint();
    tracing::info!(
        path = %in_path.display(),
        raw_format = ?input.raw_format(),
        frame_size,
        level = args.level,
        threads = effective_threads,
        record_aligned = !args.no_record_align,
        checksums = !args.no_checksum,
        "opened input for convert",
    );

    let output: Box<dyn std::io::Write> = if args.output == std::path::Path::new("-") {
        Box::new(std::io::stdout().lock())
    } else {
        let file = std::fs::File::create(&args.output).map_err(shuflr::Error::Io)?;
        Box::new(file)
    };

    // Build the progress bar if --progress allows it. For compressed inputs the
    // decompressed size is unknown; we fall back to a spinner by passing `None`.
    let show_progress = progress::should_show(args.progress);
    let bar = if show_progress {
        let total = if input.raw_format() == shuflr::io::magic::Format::Plain
            && args.sample_rate.is_none()
            && args.limit.is_none()
        {
            input_size
        } else {
            // Under filtering, output size is unknown; use a spinner.
            None
        };
        Some(progress::new_bar(total, "convert"))
    } else {
        None
    };

    // If --limit or --sample-rate is set, insert a record-level sampling
    // filter between the (decompressed) input stream and the writer. The
    // filter counts and filters records; the writer sees a plain stream of
    // accepted records only.
    let sampling_active = args.limit.is_some() || args.sample_rate.is_some();
    if sampling_active {
        tracing::info!(
            limit = ?args.limit,
            sample_rate = ?args.sample_rate,
            seed = args.seed.unwrap_or(0),
            "record sampling active",
        );
    }
    let seed = args.seed.unwrap_or(0);

    // Compose the reader stack from bottom to top:
    //   Input  →  ProgressReader  →  SamplingReader  →  (writer input)
    // The progress bar sees INPUT bytes (so it reflects decompressed-stream
    // progress, which is the knob users care about), while the sampling
    // filter trims records before they reach the writer.
    let progress_bar = bar.clone();
    let with_progress: Box<dyn Read + Send> = match progress_bar {
        Some(pb) => Box::new(progress::ProgressReader::new(input, pb)),
        None => Box::new(input),
    };
    let source: Box<dyn Read + Send> = if sampling_active {
        Box::new(shuflr::SamplingReader::new(
            with_progress,
            args.sample_rate,
            args.limit,
            seed,
        ))
    } else {
        with_progress
    };

    let started = Instant::now();
    let stats = if effective_threads <= 1 {
        // Single-threaded path: simple Writer API.
        let cfg = WriterConfig {
            level: args.level as i32,
            frame_size,
            checksums: !args.no_checksum,
            record_aligned: !args.no_record_align,
        };
        let mut writer = Writer::new(output, cfg);
        let mut reader = BufReader::with_capacity(2 * 1024 * 1024, source);
        let mut buf = vec![0u8; 2 * 1024 * 1024];
        loop {
            let n = reader.read(&mut buf).map_err(shuflr::Error::Io)?;
            if n == 0 {
                break;
            }
            writer.write_block(&buf[..n])?;
        }
        writer.finish()?
    } else {
        // Multi-threaded path: scoped worker pool, ordered writer.
        let cfg = ParallelConfig {
            level: args.level as i32,
            frame_size,
            checksums: !args.no_checksum,
            record_aligned: !args.no_record_align,
            threads: effective_threads,
        };
        convert_parallel(source, output, &cfg)?
    };
    let elapsed = started.elapsed();
    if let Some(pb) = &bar {
        pb.finish_and_clear();
    }

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
        // Only verify file outputs; stdout would need a buffered round-trip which
        // is out of scope here. Most real use cases write to a file anyway.
        if args.output == std::path::Path::new("-") {
            tracing::warn!("--verify skipped: output is stdout; re-run with -o FILE to verify");
        } else {
            let vstart = Instant::now();
            let report =
                shuflr::io::zstd_seekable::verify_strict(std::path::Path::new(&args.output))?;
            tracing::info!(
                output = %args.output.display(),
                frames = report.frames,
                records = report.records,
                decompressed_bytes = report.total_decompressed,
                elapsed_ms = vstart.elapsed().as_millis() as u64,
                "verify OK",
            );
        }
    }

    Ok(())
}

fn resolve_threads(requested: usize) -> usize {
    if requested == 0 {
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4)
    } else {
        requested
    }
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

pub fn analyze(args: cli::AnalyzeArgs) -> exit::Code {
    match analyze_inner(args) {
        Ok(verdict) => match verdict {
            shuflr::analyze::Verdict::Safe => exit::Code::Ok,
            shuflr::analyze::Verdict::Unsafe => exit::Code::AnalyzeUnsafe,
        },
        Err(e) => report_library_error(&e),
    }
}

fn analyze_inner(args: cli::AnalyzeArgs) -> shuflr::Result<shuflr::analyze::Verdict> {
    #[cfg(feature = "zstd")]
    {
        if args.input.inputs.len() != 1 {
            return Err(shuflr::Error::Input(
                "`shuflr analyze` accepts exactly one seekable-zstd input".into(),
            ));
        }
        let path = &args.input.inputs[0];
        if path == std::path::Path::new("-") {
            return Err(shuflr::Error::Input(
                "`shuflr analyze` requires a seekable file, not stdin".into(),
            ));
        }

        // Fail politely if the input isn't a seekable zstd file.
        let mut reader =
            shuflr::io::zstd_seekable::SeekableReader::open(path).map_err(|e| match e {
                shuflr::Error::Io(io) => shuflr::Error::Input(format!(
                    "'{}' is not a seekable-zstd file ({io}). Run `shuflr convert` first.",
                    path.display(),
                )),
                other => other,
            })?;

        let report = shuflr::analyze::run(
            &mut reader,
            args.sample_chunks as usize,
            args.strict as u64, // deterministic but not user-exposed
        )?;

        print_report(path, &report)?;

        if args.strict && report.verdict == shuflr::analyze::Verdict::Unsafe {
            // `--strict` turns the warning into a non-zero exit so scripts
            // can gate. The pretty-print already explained why.
            return Ok(shuflr::analyze::Verdict::Unsafe);
        }
        // Without --strict, always exit Ok but the verdict in the stdout
        // lets callers inspect.
        Ok(if args.strict {
            report.verdict
        } else {
            shuflr::analyze::Verdict::Safe
        })
    }
    #[cfg(not(feature = "zstd"))]
    {
        Err(shuflr::Error::Input(
            "`shuflr analyze` requires the zstd feature; rebuild with `--features zstd`".into(),
        ))
    }
}

#[cfg(feature = "zstd")]
fn print_report(
    path: &std::path::Path,
    report: &shuflr::analyze::AnalysisReport,
) -> shuflr::Result<()> {
    use std::io::Write as _;
    let mut out = std::io::stdout().lock();
    writeln!(out, "analyze:       {}", path.display()).map_err(shuflr::Error::Io)?;
    writeln!(out, "total frames:  {}", report.total_frames).map_err(shuflr::Error::Io)?;
    writeln!(out, "sampled:       {}", report.sampled_frames).map_err(shuflr::Error::Io)?;
    writeln!(
        out,
        "records seen:  {} (mean {:.1} B/record)",
        report.total_records_sampled, report.mean_record_len_bytes,
    )
    .map_err(shuflr::Error::Io)?;
    writeln!(
        out,
        "byte-KL max:   {:.4} nats ({})",
        report.byte_kl_max,
        qualify(
            report.byte_kl_max,
            shuflr::analyze::BYTE_KL_THRESHOLD_UNSAFE
        )
    )
    .map_err(shuflr::Error::Io)?;
    writeln!(
        out,
        "reclen CV:     {:.3}      ({})",
        report.reclen_cv,
        qualify(
            report.reclen_cv,
            shuflr::analyze::RECLEN_CV_THRESHOLD_UNSAFE
        )
    )
    .map_err(shuflr::Error::Io)?;
    match report.verdict {
        shuflr::analyze::Verdict::Safe => {
            writeln!(out, "verdict:       SAFE — chunk-shuffled is fine")
                .map_err(shuflr::Error::Io)?;
        }
        shuflr::analyze::Verdict::Unsafe => {
            writeln!(
                out,
                "verdict:       UNSAFE — source-order locality detected.\n\
                 recommendation: use `shuflr index {}` + `--shuffle=index-perm`\n\
                 for a provably uniform shuffle.",
                path.display()
            )
            .map_err(shuflr::Error::Io)?;
        }
    }
    Ok(())
}

#[cfg(feature = "zstd")]
fn qualify(value: f64, threshold: f64) -> &'static str {
    if value < threshold * 0.5 {
        "uniform"
    } else if value < threshold {
        "mild drift"
    } else {
        "SKEWED"
    }
}

pub fn index(args: cli::IndexArgs) -> exit::Code {
    match index_inner(args) {
        Ok(()) => exit::Code::Ok,
        Err(e) => report_library_error(&e),
    }
}

fn index_inner(args: cli::IndexArgs) -> shuflr::Result<()> {
    if args.input.inputs.len() != 1 {
        return Err(shuflr::Error::Input(
            "`shuflr index` accepts exactly one input file".into(),
        ));
    }
    let in_path = &args.input.inputs[0];
    if in_path == std::path::Path::new("-") {
        return Err(shuflr::Error::Input(
            "`shuflr index` requires a seekable file (stdin has no persistent offsets)".into(),
        ));
    }
    // Reject compressed inputs; index-perm needs byte-offset random access.
    let probe = shuflr::io::Input::open(in_path)?;
    if probe.raw_format() != shuflr::io::magic::Format::Plain {
        return Err(shuflr::Error::Input(format!(
            "`shuflr index` currently only supports plain JSONL. \
             '{}' is {}; decompress to '.jsonl' first, or convert to zstd-seekable \
             and use --shuffle=chunk-shuffled instead.",
            in_path.display(),
            probe.raw_format().name(),
        )));
    }
    drop(probe);

    let out_path = args
        .output
        .unwrap_or_else(|| shuflr::index::sidecar_path(in_path));

    let started = Instant::now();
    let fingerprint = shuflr::Fingerprint::from_metadata(in_path)?;
    let file = std::fs::File::open(in_path).map_err(shuflr::Error::Io)?;
    let idx = shuflr::IndexFile::build(file, fingerprint)?;
    let build_ms = started.elapsed().as_millis() as u64;

    let save_start = Instant::now();
    idx.save(&out_path)?;
    let save_ms = save_start.elapsed().as_millis() as u64;

    tracing::info!(
        input = %in_path.display(),
        index = %out_path.display(),
        records = idx.count(),
        build_ms,
        save_ms,
        "index built",
    );
    Ok(())
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
