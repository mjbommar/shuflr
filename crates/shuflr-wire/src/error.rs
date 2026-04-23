//! Typed errors for the wire codec.
//!
//! Decode errors fall into two classes: *framing* errors (bad magic,
//! bad xxh3, unknown kind) that indicate an incompatible / corrupt
//! peer and warrant closing the connection; *insufficient* (which is
//! not actually an Err, represented as Ok(None) by the decoder) for
//! when we simply haven't read enough bytes yet.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum WireError {
    #[error("unsupported magic: got {got:?}, expected SHUFLRW1")]
    BadMagic { got: [u8; 8] },

    #[error("unsupported protocol version {got}; this codec speaks version 1")]
    BadVersion { got: u8 },

    #[error("message kind {got} is not recognized in v1")]
    UnknownKind { got: u8 },

    #[error("xxh3 mismatch on kind={kind}: wire={wire:016x}, computed={computed:016x}")]
    Xxh3Mismatch { kind: u8, wire: u64, computed: u64 },

    #[error("message payload length {got} exceeds MAX_MESSAGE_BYTES ({max})")]
    PayloadTooLarge { got: u32, max: u32 },

    #[error("truncated payload for kind={kind}: expected {expected} bytes, got {got}")]
    TruncatedPayload {
        kind: u8,
        expected: usize,
        got: usize,
    },

    #[error("malformed field in kind={kind}: {detail}")]
    Malformed { kind: u8, detail: String },
}
