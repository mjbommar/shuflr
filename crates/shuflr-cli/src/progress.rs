//! Progress bar plumbing (002 §6.2 `--progress`).
//!
//! We wrap the caller's `Read` in a thin counter that updates an
//! `indicatif::ProgressBar` on every `read`. For plain-file inputs we
//! know the decompressed == file size so we can show a real bar with a
//! percentage and ETA; for compressed inputs we fall back to a spinner
//! plus byte-rate display because the decompressed total is unknown at
//! open time.

use std::io::{self, Read};
use std::time::Duration;

use indicatif::{ProgressBar, ProgressStyle};

use crate::cli;

/// Decide whether to draw a progress bar given the user's flag and the
/// runtime TTY. `indicatif` defaults to drawing on stderr; we honour
/// --progress=never always and --progress=always to force-on.
pub fn should_show(when: cli::When) -> bool {
    match when {
        cli::When::Never => false,
        cli::When::Always => true,
        cli::When::Auto => is_stderr_tty(),
    }
}

fn is_stderr_tty() -> bool {
    use std::io::IsTerminal;
    io::stderr().is_terminal()
}

/// Build a progress bar sized for the given known-total bytes (or `None`
/// for an indeterminate spinner).
pub fn new_bar(total_bytes: Option<u64>, prefix: &str) -> ProgressBar {
    match total_bytes {
        Some(n) => {
            let pb = ProgressBar::new(n);
            pb.set_style(
                ProgressStyle::with_template(
                    "{prefix:.bold} {bar:30.cyan/blue} {bytes}/{total_bytes} ({bytes_per_sec}, ETA {eta})",
                )
                .unwrap_or_else(|_| ProgressStyle::default_bar())
                .progress_chars("=> "),
            );
            pb.set_prefix(prefix.to_string());
            pb.enable_steady_tick(Duration::from_millis(120));
            pb
        }
        None => {
            let pb = ProgressBar::new_spinner();
            pb.set_style(
                ProgressStyle::with_template("{prefix:.bold} {spinner} {bytes} ({bytes_per_sec})")
                    .unwrap_or_else(|_| ProgressStyle::default_spinner()),
            );
            pb.set_prefix(prefix.to_string());
            pb.enable_steady_tick(Duration::from_millis(120));
            pb
        }
    }
}

/// Build a progress bar counting discrete units (frames, records, …) with
/// a known total. Used by long-running jobs that don't have a neat "bytes
/// consumed" number but do have a frame count — e.g. the seekable-zstd
/// record-index cold build.
pub fn new_count_bar(total: u64, prefix: &str, unit: &str) -> ProgressBar {
    let pb = ProgressBar::new(total);
    let template = format!(
        "{{prefix:.bold}} {{bar:30.cyan/blue}} {{pos}}/{{len}} {unit} ({{per_sec}}, ETA {{eta}})"
    );
    pb.set_style(
        ProgressStyle::with_template(&template)
            .unwrap_or_else(|_| ProgressStyle::default_bar())
            .progress_chars("=> "),
    );
    pb.set_prefix(prefix.to_string());
    pb.enable_steady_tick(Duration::from_millis(120));
    pb
}

/// Wraps a `Read` and bumps the attached progress bar on every successful read.
pub struct ProgressReader<R: Read> {
    inner: R,
    bar: ProgressBar,
}

impl<R: Read> ProgressReader<R> {
    pub fn new(inner: R, bar: ProgressBar) -> Self {
        Self { inner, bar }
    }
}

impl<R: Read> Read for ProgressReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let n = self.inner.read(buf)?;
        if n > 0 {
            self.bar.inc(n as u64);
        }
        Ok(n)
    }
}
