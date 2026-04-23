//! Typed message model for `shuflr-wire/1`. Mirrors 005 §3.2-3.4.
//!
//! Enum variants correspond 1:1 to message kinds on the wire. Fields
//! that are opaque bytes on the wire (the protobuf-encoded
//! `OpenStream` / `StreamOpened`, bearer-token bytes, error
//! detail bytes) are `Vec<u8>` here so the codec never has to
//! understand anything more than length-prefixed framing. The
//! transport layer is free to decode those as protobuf / utf8.

/// Message kinds on the wire. The numeric values are the bytes that
/// appear in the first octet of every frame.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Handshake = 0,
    RawFrame = 1,
    ZstdBatch = 2,
    PlainBatch = 3,
    EpochBoundary = 4,
    StreamError = 5,
    StreamClosed = 6,
    Heartbeat = 7,
    AddCredit = 10,
    Cancel = 11,
    Pong = 12,
}

impl Kind {
    pub fn from_u8(v: u8) -> Option<Self> {
        Some(match v {
            0 => Kind::Handshake,
            1 => Kind::RawFrame,
            2 => Kind::ZstdBatch,
            3 => Kind::PlainBatch,
            4 => Kind::EpochBoundary,
            5 => Kind::StreamError,
            6 => Kind::StreamClosed,
            7 => Kind::Heartbeat,
            10 => Kind::AddCredit,
            11 => Kind::Cancel,
            12 => Kind::Pong,
            _ => return None,
        })
    }

    /// Handshake frames skip xxh3 (see 005 §3.2). Every other frame
    /// carries a trailing u64 xxh3 checksum.
    pub fn has_checksum(self) -> bool {
        !matches!(self, Kind::Handshake)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum AuthKind {
    None = 0,
    Bearer = 1,
    Mtls = 2,
}

impl AuthKind {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(AuthKind::None),
            1 => Some(AuthKind::Bearer),
            2 => Some(AuthKind::Mtls),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ChosenMode {
    RawFrame = 1,
    ZstdBatch = 2,
    PlainBatch = 3,
}

impl ChosenMode {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            1 => Some(ChosenMode::RawFrame),
            2 => Some(ChosenMode::ZstdBatch),
            3 => Some(ChosenMode::PlainBatch),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum HandshakeStatus {
    Ok = 0,
    /// Any non-zero status. The `stream_opened` payload on a non-Ok
    /// ServerHello carries a human detail string the transport can log.
    Error = 1,
}

impl HandshakeStatus {
    pub fn from_u8(v: u8) -> Self {
        match v {
            0 => HandshakeStatus::Ok,
            _ => HandshakeStatus::Error,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum StreamErrorCode {
    Unspecified = 0,
    MalformedRecord = 1,
    OversizedRecord = 2,
    IoError = 3,
    SlowConsumer = 4,
    ServerShutdown = 5,
    AuthDenied = 6,
    ResourceExhausted = 7,
    Internal = 8,
}

impl StreamErrorCode {
    pub fn from_u8(v: u8) -> Self {
        match v {
            1 => StreamErrorCode::MalformedRecord,
            2 => StreamErrorCode::OversizedRecord,
            3 => StreamErrorCode::IoError,
            4 => StreamErrorCode::SlowConsumer,
            5 => StreamErrorCode::ServerShutdown,
            6 => StreamErrorCode::AuthDenied,
            7 => StreamErrorCode::ResourceExhausted,
            8 => StreamErrorCode::Internal,
            _ => StreamErrorCode::Unspecified,
        }
    }
}

/// Decoded payload for a `PlainBatch` (kind=3).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchPayload {
    pub batch_id: u64,
    pub epoch: u32,
    pub records: Vec<Vec<u8>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientHello {
    /// bit 0 raw-frame, bit 1 zstd-batch, bit 2 xxh3 on control msgs.
    /// bits 3-7 reserved (must be 0).
    pub capability_flags: u8,
    pub auth_kind: AuthKind,
    /// Max 65535 bytes (u16 length on wire).
    pub auth: Vec<u8>,
    /// Opaque `shuflr.v1.OpenStream` protobuf bytes. Max
    /// `MAX_MESSAGE_BYTES` minus the fixed-size fields.
    pub open_stream: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerHello {
    pub status: HandshakeStatus,
    pub chosen_mode: Option<ChosenMode>,
    pub initial_credit: u64,
    pub server_version: u8,
    pub max_message_bytes: u32,
    /// Opaque `shuflr.v1.StreamOpened` protobuf bytes, or a UTF-8
    /// error detail when `status != Ok`.
    pub stream_opened: Vec<u8>,
}

/// A decoded `shuflr-wire/1` message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Message {
    ClientHello(ClientHello),
    ServerHello(ServerHello),
    RawFrame {
        frame_id: u32,
        perm_seed: u64,
        zstd_bytes: Vec<u8>,
    },
    ZstdBatch {
        batch_id: u64,
        epoch: u32,
        n_records: u32,
        zstd_bytes: Vec<u8>,
    },
    PlainBatch(BatchPayload),
    EpochBoundary {
        completed_epoch: u32,
        records_in_epoch: u64,
    },
    StreamError {
        code: StreamErrorCode,
        fatal: bool,
        detail: Vec<u8>,
    },
    StreamClosed {
        total_records: u64,
        epochs_completed: u32,
    },
    Heartbeat {
        now_unix_nanos: u64,
    },
    AddCredit {
        add_bytes: u64,
    },
    Cancel {
        reason: Vec<u8>,
    },
    Pong {
        now_unix_nanos: u64,
    },
}

impl Message {
    pub fn kind(&self) -> Kind {
        match self {
            Message::ClientHello(_) | Message::ServerHello(_) => Kind::Handshake,
            Message::RawFrame { .. } => Kind::RawFrame,
            Message::ZstdBatch { .. } => Kind::ZstdBatch,
            Message::PlainBatch(_) => Kind::PlainBatch,
            Message::EpochBoundary { .. } => Kind::EpochBoundary,
            Message::StreamError { .. } => Kind::StreamError,
            Message::StreamClosed { .. } => Kind::StreamClosed,
            Message::Heartbeat { .. } => Kind::Heartbeat,
            Message::AddCredit { .. } => Kind::AddCredit,
            Message::Cancel { .. } => Kind::Cancel,
            Message::Pong { .. } => Kind::Pong,
        }
    }
}
