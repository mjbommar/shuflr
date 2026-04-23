//! `--shuffle=none` — serial scan + emit.
//!
//! Reads records from a [`crate::io::Input`] and writes them to any
//! `impl Write` in file order. No shuffle, no index, no memory overhead
//! beyond a 2 MiB reused buffer. Serves as the baseline throughput
//! target and the skeleton the actual shuffle modes plug into later.
//!
//! The loop is deliberately simple: `BufRead::read_until(b'\n', ...)`
//! reuses its `Vec<u8>` buffer across iterations, so line parsing is a
//! single `memchr` call plus a copy per record. The `BufWriter` on the
//! sink amortizes syscalls to one per ~2 MiB flushed.

use std::io::{BufRead, BufWriter, Write};

use crate::error::{Error, Result};
use crate::framing::{OnError, Stats};
use crate::io::Input;

/// Configuration for a passthrough run.
#[derive(Debug, Clone)]
pub struct Config {
    /// Per-record byte cap. Records longer than this are subject to `on_error`.
    pub max_line: u64,
    /// Policy for oversized records.
    pub on_error: OnError,
    /// Emit at most this many records (after `on_error` skips). `None` = unbounded.
    pub sample: Option<u64>,
    /// If true, ensure the last emitted record ends with `\n` even if the input didn't.
    pub ensure_trailing_newline: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            max_line: 16 * 1024 * 1024,
            on_error: OnError::Skip,
            sample: None,
            ensure_trailing_newline: true,
        }
    }
}

/// Default write-buffer capacity on the sink (2 MiB).
///
/// Matches the reader capacity; at this size stdout flushes are ~one
/// syscall per 2 MiB regardless of record length distribution.
pub const DEFAULT_WRITER_CAPACITY: usize = 2 * 1024 * 1024;

/// Run a passthrough pipeline. Returns aggregated [`Stats`].
///
/// `input` is consumed; caller passes the sink as `impl Write`. The sink
/// is wrapped in a `BufWriter` internally — do not wrap it yourself.
pub fn run(mut input: Input, sink: impl Write, cfg: &Config) -> Result<Stats> {
    input.reject_if_compressed()?;

    let mut writer = BufWriter::with_capacity(DEFAULT_WRITER_CAPACITY, sink);
    let mut stats = Stats::default();

    // Reused across iterations; capacity grows as needed and stays.
    let mut line: Vec<u8> = Vec::with_capacity(8 * 1024);
    let mut byte_offset: u64 = 0;

    loop {
        line.clear();
        let n = input.read_until(b'\n', &mut line).map_err(Error::Io)?;
        if n == 0 {
            // EOF with no pending bytes.
            break;
        }
        stats.records_in += 1;
        stats.bytes_in += n as u64;

        let this_offset = byte_offset;
        byte_offset += n as u64;
        let has_newline = line.last() == Some(&b'\n');
        if !has_newline {
            stats.had_trailing_partial = true;
        }

        // --max-line policy. The byte count we compare is wire size (including the
        // trailing '\n' when present), matching what a downstream pipe would see.
        if (n as u64) > cfg.max_line {
            let keep =
                stats.apply_oversize_policy(cfg.on_error, this_offset, n as u64, cfg.max_line)?;
            if !keep {
                continue;
            }
        }

        // Emit.
        writer.write_all(&line).map_err(Error::Io)?;
        if !has_newline && cfg.ensure_trailing_newline {
            writer.write_all(b"\n").map_err(Error::Io)?;
            stats.bytes_out += 1;
        }
        stats.bytes_out += line.len() as u64;
        stats.records_out += 1;

        if let Some(cap) = cfg.sample
            && stats.records_out >= cap
        {
            break;
        }
    }

    writer.flush().map_err(Error::Io)?;
    Ok(stats)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::Input;

    fn input_of(bytes: &'static [u8]) -> Input {
        Input::from_reader(Box::new(bytes), Some(bytes.len() as u64), None).unwrap()
    }

    #[test]
    fn trivial_three_lines() {
        let inp = input_of(b"{\"a\":1}\n{\"a\":2}\n{\"a\":3}\n");
        let mut out = Vec::new();
        let stats = run(inp, &mut out, &Config::default()).unwrap();
        assert_eq!(stats.records_in, 3);
        assert_eq!(stats.records_out, 3);
        assert_eq!(out, b"{\"a\":1}\n{\"a\":2}\n{\"a\":3}\n");
    }

    #[test]
    fn missing_trailing_newline_is_patched() {
        let inp = input_of(b"first\nsecond");
        let mut out = Vec::new();
        let stats = run(inp, &mut out, &Config::default()).unwrap();
        assert_eq!(stats.records_in, 2);
        assert_eq!(stats.records_out, 2);
        assert!(stats.had_trailing_partial);
        assert_eq!(out, b"first\nsecond\n");
    }

    #[test]
    fn empty_input_emits_nothing() {
        let inp = input_of(b"");
        let mut out = Vec::new();
        let stats = run(inp, &mut out, &Config::default()).unwrap();
        assert_eq!(stats.records_in, 0);
        assert_eq!(stats.records_out, 0);
        assert_eq!(stats.bytes_out, 0);
        assert!(out.is_empty());
    }

    #[test]
    fn sample_caps_output() {
        let inp = input_of(b"a\nb\nc\nd\ne\n");
        let mut out = Vec::new();
        let cfg = Config {
            sample: Some(3),
            ..Config::default()
        };
        let stats = run(inp, &mut out, &cfg).unwrap();
        assert_eq!(stats.records_out, 3);
        assert_eq!(out, b"a\nb\nc\n");
    }

    #[test]
    fn oversized_skip_drops_and_continues() {
        // max_line=10: "ok\n" (3) and "also_ok\n" (8) fit; "WAY_TOO_LONG\n" (13) doesn't.
        let inp = input_of(b"ok\nWAY_TOO_LONG\nalso_ok\n");
        let mut out = Vec::new();
        let cfg = Config {
            max_line: 10,
            on_error: OnError::Skip,
            ..Config::default()
        };
        let stats = run(inp, &mut out, &cfg).unwrap();
        assert_eq!(stats.records_in, 3);
        assert_eq!(stats.records_out, 2);
        assert_eq!(stats.oversized_skipped, 1);
        assert_eq!(out, b"ok\nalso_ok\n");
    }

    #[test]
    fn oversized_fail_errors() {
        let inp = input_of(b"ok\nWAY_TOO_LONG\n");
        let mut out = Vec::new();
        let cfg = Config {
            max_line: 5,
            on_error: OnError::Fail,
            ..Config::default()
        };
        let err = run(inp, &mut out, &cfg).unwrap_err();
        assert!(matches!(err, Error::OversizedRecord { .. }));
    }

    #[test]
    fn oversized_passthrough_emits_anyway() {
        let inp = input_of(b"ok\nWAY_TOO_LONG\nfin\n");
        let mut out = Vec::new();
        let cfg = Config {
            max_line: 4, // < "WAY_TOO_LONG\n" (13) and < "ok\n" (3)? "ok\n"=3 <=4 ok; fin\n=4 ok.
            on_error: OnError::Passthrough,
            ..Config::default()
        };
        let stats = run(inp, &mut out, &cfg).unwrap();
        assert_eq!(stats.records_out, 3);
        assert_eq!(stats.oversized_passthrough, 1);
        assert_eq!(out, b"ok\nWAY_TOO_LONG\nfin\n");
    }

    #[test]
    fn rejects_gzip_input() {
        let inp = input_of(&[0x1f, 0x8b, 0x08, 0x00, 0, 0, 0, 0]);
        let mut out = Vec::new();
        let err = run(inp, &mut out, &Config::default()).unwrap_err();
        assert!(matches!(err, Error::CompressedInputUnsupported { .. }));
    }

    #[test]
    fn handles_crlf_by_default_preserving_cr() {
        // LF framing preserves `\r` in the record; normalization is a later feature.
        let inp = input_of(b"one\r\ntwo\r\n");
        let mut out = Vec::new();
        let stats = run(inp, &mut out, &Config::default()).unwrap();
        assert_eq!(stats.records_in, 2);
        assert_eq!(out, b"one\r\ntwo\r\n");
    }

    #[test]
    fn embedded_nuls_pass_through() {
        let inp = input_of(b"before\0after\nnext\n");
        let mut out = Vec::new();
        let stats = run(inp, &mut out, &Config::default()).unwrap();
        assert_eq!(stats.records_in, 2);
        assert_eq!(out, b"before\0after\nnext\n");
    }

    #[test]
    fn zero_byte_file_is_empty_output() {
        let inp = input_of(b"");
        let mut out = Vec::new();
        run(inp, &mut out, &Config::default()).unwrap();
        assert_eq!(out, b"");
    }

    #[test]
    fn single_line_no_trailing_newline() {
        let inp = input_of(b"solo");
        let mut out = Vec::new();
        let stats = run(inp, &mut out, &Config::default()).unwrap();
        assert_eq!(stats.records_in, 1);
        assert_eq!(out, b"solo\n");
    }
}
