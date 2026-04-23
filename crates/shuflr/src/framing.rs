//! Record-framing primitives: how bytes become lines, and what to do on mishaps.
//!
//! v1 framing is newline-delimited JSON (`\n`). Other framings (length-prefixed
//! varint, u32le) are reserved for later PRs and live here when they arrive.

use crate::error::Error;

/// Policy for records that exceed `--max-line` or fail parsing.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub enum OnError {
    /// Drop the record and count it in stats; continue.
    #[default]
    Skip,
    /// Abort immediately with a typed error.
    Fail,
    /// Emit the record as-is despite the violation.
    Passthrough,
}

/// Aggregate counters emitted at end-of-run and surfaced via `tracing::info!`.
#[derive(Default, Debug, Clone)]
pub struct Stats {
    /// Complete records read from the input (including any skipped).
    pub records_in: u64,
    /// Records written to the sink (subject to `--sample`).
    pub records_out: u64,
    /// Raw bytes pulled from the input source.
    pub bytes_in: u64,
    /// Raw bytes written to the sink (including any appended trailing `\n`).
    pub bytes_out: u64,
    /// Records that exceeded `--max-line` and were dropped.
    pub oversized_skipped: u64,
    /// Records that exceeded `--max-line` and were emitted under `on-error=passthrough`.
    pub oversized_passthrough: u64,
    /// Final partial line (no trailing `\n`) encountered at EOF.
    pub had_trailing_partial: bool,
}

impl Stats {
    /// Apply an oversized-record policy to the given byte length; returns `Ok(true)`
    /// if the record should be emitted, `Ok(false)` if it should be dropped.
    pub(crate) fn apply_oversize_policy(
        &mut self,
        policy: OnError,
        offset: u64,
        len: u64,
        cap: u64,
    ) -> Result<bool, Error> {
        match policy {
            OnError::Skip => {
                self.oversized_skipped += 1;
                Ok(false)
            }
            OnError::Fail => Err(Error::OversizedRecord { offset, len, cap }),
            OnError::Passthrough => {
                self.oversized_passthrough += 1;
                Ok(true)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn skip_policy_drops_and_counts() {
        let mut s = Stats::default();
        let keep = s.apply_oversize_policy(OnError::Skip, 0, 100, 50).unwrap();
        assert!(!keep);
        assert_eq!(s.oversized_skipped, 1);
    }

    #[test]
    fn fail_policy_errors() {
        let mut s = Stats::default();
        let err = s
            .apply_oversize_policy(OnError::Fail, 42, 100, 50)
            .unwrap_err();
        assert!(matches!(
            err,
            Error::OversizedRecord {
                offset: 42,
                len: 100,
                cap: 50
            }
        ));
    }

    #[test]
    fn passthrough_policy_emits_and_counts() {
        let mut s = Stats::default();
        let keep = s
            .apply_oversize_policy(OnError::Passthrough, 0, 100, 50)
            .unwrap();
        assert!(keep);
        assert_eq!(s.oversized_passthrough, 1);
    }
}
