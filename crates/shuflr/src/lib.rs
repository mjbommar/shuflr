//! shuflr — streaming shuffled JSONL.
//!
//! See `docs/design/002-revised-plan.md` for the authoritative v1 specification,
//! plus amendments `003-compression-formats.md`, `004-convert-subcommand.md`,
//! and `005-serve-multi-transport.md`.

#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

pub mod analyze;
pub mod error;
pub mod framing;
pub mod index;
pub mod io;
pub mod json_validate;
pub mod pipeline;
pub mod sampling;
pub mod seed;

pub use error::{Error, Result};
pub use framing::{OnError, Stats};
pub use index::{Fingerprint, IndexFile};
pub use sampling::SamplingReader;
pub use seed::Seed;

/// Physical CPU core count (not logical/SMT). Defaults to 1 on systems
/// where detection fails. Preferred over `std::thread::available_parallelism`
/// for compute-heavy workloads like zstd compression; see
/// `docs/bench/001-edgar-31gb-gzip.md` §thread-scaling.
pub fn physical_cores() -> usize {
    num_cpus::get_physical().max(1)
}
