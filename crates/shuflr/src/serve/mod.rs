//! Network service surface — the `serve` subcommand.
//!
//! See `docs/design/005-serve-multi-transport.md` for the spec that
//! supersedes 002 §7.
//!
//! PR-30 ships the HTTP/1.1 chunked NDJSON transport (loopback only,
//! no auth, no TLS). PR-31 layers TLS + auth. PR-33 adds the custom
//! `shuflr-wire/1` binary transport. PR-35 adds gRPC.
//!
//! All transports share one sync core (the `shuflr::pipeline` modules)
//! and one dataset catalog ([`catalog::Catalog`]). Each transport owns
//! only its framing / wire adapter.

pub mod catalog;
pub mod http;

pub use catalog::{Catalog, DatasetEntry};
pub use http::{HttpConfig, run as run_http};
