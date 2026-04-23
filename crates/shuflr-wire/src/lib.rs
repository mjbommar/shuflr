//! `shuflr-wire/1` protocol codec — pure framing / parsing with no I/O.
//!
//! Specced in `docs/design/005-serve-multi-transport.md` §3. This
//! crate knows nothing about sockets, TLS, pyo3, or tokio. It turns
//! [`Message`] values into bytes and back, with an xxh3 checksum on
//! every non-handshake frame.
//!
//! The transport (PR-33) drives this codec from a
//! `tokio::io::{AsyncReadExt, AsyncWriteExt}` loop; the `shuflr-
//! client` Python wheel (PR-34b) uses it from a blocking `std::io`
//! loop. Both sides use the same encode/decode pair — byte-for-byte
//! compat is pinned by the proptest round-trip harness in tests/.

#![forbid(unsafe_code)]
#![deny(missing_debug_implementations)]

pub mod codec;
pub mod error;
pub mod message;

pub use codec::{
    DecodeOptions, Decoder, HandshakeRole, MIN_FRAME_BYTES, encode, encode_handshake, encode_into,
};
pub use error::WireError;
pub use message::{
    AuthKind, BatchPayload, ChosenMode, ClientHello, HandshakeStatus, Kind, Message, ServerHello,
    StreamErrorCode,
};

pub const MAGIC: [u8; 8] = *b"SHUFLRW1";
pub const VERSION: u8 = 1;
/// Server-side hard cap on any single message body. 005 §3.1. Chosen
/// to exceed EDGAR's largest single-record frame (427 MiB) with
/// headroom without leaving the door open for multi-GB pathological
/// messages.
pub const MAX_MESSAGE_BYTES: u32 = 512 * 1024 * 1024;
