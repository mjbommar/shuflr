//! shuflr — streaming shuffled JSONL.
//!
//! See `docs/design/002-revised-plan.md` for the authoritative v1 specification,
//! plus amendments `003-compression-formats.md` and `004-convert-subcommand.md`.

#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

pub mod analyze;
pub mod error;
pub mod framing;
pub mod index;
pub mod io;
pub mod pipeline;
pub mod seed;

pub use error::{Error, Result};
pub use framing::{OnError, Stats};
pub use index::{Fingerprint, IndexFile};
pub use seed::Seed;
