//! Encoder / decoder for `shuflr-wire/1`.
//!
//! **Frame layout** (non-handshake; 005 §3.1):
//!
//! ```text
//! +---------+----------------+---------------------+---------+
//! | kind u8 | payload_len u32| payload             | xxh3 u64|
//! |         |    little-end  | (payload_len bytes) |         |
//! +---------+----------------+---------------------+---------+
//! ```
//!
//! Handshake frames (kind=0) use the same framing but skip the
//! trailing xxh3 (005 §3.2 — chicken-and-egg with versioning).
//!
//! xxh3 covers `kind || payload_len_le || payload`. The client MAY
//! skip verification for throughput; see [`DecodeOptions::verify_xxh3`].

use xxhash_rust::xxh3::xxh3_64;

use crate::error::WireError;
use crate::message::{
    AuthKind, BatchPayload, ChosenMode, ClientHello, HandshakeStatus, Kind, Message, ServerHello,
    StreamErrorCode,
};
use crate::{MAGIC, MAX_MESSAGE_BYTES, VERSION};

/// Bytes of fixed framing overhead on a non-handshake message.
pub const MIN_FRAME_BYTES: usize = 1 /* kind */ + 4 /* len */ + 8 /* xxh3 */;

/// Encode `msg` into a fresh `Vec<u8>`. Convenience wrapper over
/// [`encode_into`].
pub fn encode(msg: &Message) -> Vec<u8> {
    let mut out = Vec::new();
    encode_into(msg, &mut out);
    out
}

/// Encode `msg`, appending to `out`. Preferred form in hot paths —
/// the transport layer reuses one buffer across writes.
pub fn encode_into(msg: &Message, out: &mut Vec<u8>) {
    let kind = msg.kind();
    let start = out.len();
    // Reserve the 5-byte header now; we'll backfill the length once
    // we've written the payload.
    out.push(kind as u8);
    out.extend_from_slice(&[0u8; 4]);
    let payload_start = out.len();
    write_payload(msg, out);
    let payload_len = out.len() - payload_start;
    debug_assert!(payload_len <= u32::MAX as usize);
    out[start + 1..start + 5].copy_from_slice(&(payload_len as u32).to_le_bytes());
    if kind.has_checksum() {
        let h = xxh3_64(&out[start..payload_start + payload_len]);
        out.extend_from_slice(&h.to_le_bytes());
    }
}

/// Convenience: encode a handshake message (ClientHello or
/// ServerHello). Fails at compile-time if the caller passes anything
/// else via a debug_assert.
pub fn encode_handshake(msg: &Message) -> Vec<u8> {
    debug_assert!(matches!(
        msg,
        Message::ClientHello(_) | Message::ServerHello(_)
    ));
    encode(msg)
}

fn write_payload(msg: &Message, out: &mut Vec<u8>) {
    match msg {
        Message::ClientHello(h) => {
            out.extend_from_slice(&MAGIC);
            out.push(VERSION);
            out.push(h.capability_flags);
            out.push(h.auth_kind as u8);
            let auth_len: u16 = u16::try_from(h.auth.len()).unwrap_or(u16::MAX);
            out.extend_from_slice(&auth_len.to_le_bytes());
            out.extend_from_slice(&h.auth[..auth_len as usize]);
            let os_len: u32 = u32::try_from(h.open_stream.len()).unwrap_or(u32::MAX);
            out.extend_from_slice(&os_len.to_le_bytes());
            out.extend_from_slice(&h.open_stream[..os_len as usize]);
        }
        Message::ServerHello(s) => {
            out.push(s.status as u8);
            out.push(match s.chosen_mode {
                Some(m) => m as u8,
                None => 0,
            });
            out.extend_from_slice(&s.initial_credit.to_le_bytes());
            out.push(s.server_version);
            out.extend_from_slice(&s.max_message_bytes.to_le_bytes());
            let so_len: u32 = u32::try_from(s.stream_opened.len()).unwrap_or(u32::MAX);
            out.extend_from_slice(&so_len.to_le_bytes());
            out.extend_from_slice(&s.stream_opened[..so_len as usize]);
        }
        Message::RawFrame {
            frame_id,
            perm_seed,
            zstd_bytes,
        } => {
            out.extend_from_slice(&frame_id.to_le_bytes());
            out.extend_from_slice(perm_seed);
            out.extend_from_slice(zstd_bytes);
        }
        Message::ZstdBatch {
            batch_id,
            epoch,
            n_records,
            zstd_bytes,
        } => {
            out.extend_from_slice(&batch_id.to_le_bytes());
            out.extend_from_slice(&epoch.to_le_bytes());
            out.extend_from_slice(&n_records.to_le_bytes());
            out.extend_from_slice(zstd_bytes);
        }
        Message::PlainBatch(b) => {
            out.extend_from_slice(&b.batch_id.to_le_bytes());
            out.extend_from_slice(&b.epoch.to_le_bytes());
            let n: u32 = u32::try_from(b.records.len()).unwrap_or(u32::MAX);
            out.extend_from_slice(&n.to_le_bytes());
            for rec in &b.records {
                let len: u32 = u32::try_from(rec.len()).unwrap_or(u32::MAX);
                out.extend_from_slice(&len.to_le_bytes());
                out.extend_from_slice(&rec[..len as usize]);
            }
        }
        Message::EpochBoundary {
            completed_epoch,
            records_in_epoch,
        } => {
            out.extend_from_slice(&completed_epoch.to_le_bytes());
            out.extend_from_slice(&records_in_epoch.to_le_bytes());
        }
        Message::StreamError {
            code,
            fatal,
            detail,
        } => {
            out.push(*code as u8);
            out.push(if *fatal { 1 } else { 0 });
            out.extend_from_slice(detail);
        }
        Message::StreamClosed {
            total_records,
            epochs_completed,
        } => {
            out.extend_from_slice(&total_records.to_le_bytes());
            out.extend_from_slice(&epochs_completed.to_le_bytes());
        }
        Message::Heartbeat { now_unix_nanos } | Message::Pong { now_unix_nanos } => {
            out.extend_from_slice(&now_unix_nanos.to_le_bytes());
        }
        Message::AddCredit { add_bytes } => {
            out.extend_from_slice(&add_bytes.to_le_bytes());
        }
        Message::Cancel { reason } => {
            out.extend_from_slice(reason);
        }
    }
}

/// Which handshake form the decoder should expect. ClientHello
/// begins with the 8-byte magic; ServerHello does not. The transport
/// knows its role; callers that don't (e.g. protocol smoke tests)
/// can use `Either`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HandshakeRole {
    /// This decoder is on the server side — the next kind-0 frame is
    /// a ClientHello.
    ExpectClientHello,
    /// This decoder is on the client side — the next kind-0 frame is
    /// a ServerHello.
    ExpectServerHello,
    /// Try ClientHello first (match magic); fall back to ServerHello.
    Either,
}

/// Options that govern how the decoder behaves.
#[derive(Debug, Clone, Copy)]
pub struct DecodeOptions {
    /// If true, verify the trailing xxh3 on every non-handshake frame
    /// and reject mismatches. Default `true`.
    pub verify_xxh3: bool,
    /// Payload-size cap. Defaults to [`MAX_MESSAGE_BYTES`]; a server
    /// can tighten this on the accept path.
    pub max_payload: u32,
    /// Which handshake form to parse. Default `Either`.
    pub role: HandshakeRole,
}

impl Default for DecodeOptions {
    fn default() -> Self {
        Self {
            verify_xxh3: true,
            max_payload: MAX_MESSAGE_BYTES,
            role: HandshakeRole::Either,
        }
    }
}

/// Stateful streaming decoder. Feed it bytes as they arrive, call
/// [`Decoder::try_next`] to pop off framed messages.
#[derive(Debug, Default)]
pub struct Decoder {
    buf: Vec<u8>,
    opts: DecodeOptions,
}

impl Decoder {
    pub fn new(opts: DecodeOptions) -> Self {
        Self {
            buf: Vec::with_capacity(8 * 1024),
            opts,
        }
    }

    /// Append bytes to the internal buffer. The decoder does not copy
    /// again during [`try_next`]; ownership moves in.
    pub fn feed(&mut self, bytes: &[u8]) {
        self.buf.extend_from_slice(bytes);
    }

    /// Pop the next complete message off the buffer. Returns:
    /// - `Ok(Some(msg))` if one full frame was parsed
    /// - `Ok(None)` if we need more bytes
    /// - `Err(...)` on a protocol violation (malformed / bad xxh3 / …)
    pub fn try_next(&mut self) -> Result<Option<Message>, WireError> {
        if self.buf.is_empty() {
            return Ok(None);
        }
        let kind_byte = self.buf[0];
        let kind = Kind::from_u8(kind_byte).ok_or(WireError::UnknownKind { got: kind_byte })?;
        if self.buf.len() < 5 {
            return Ok(None);
        }
        // Safe: slice is bounded [1..5] so exactly 4 bytes.
        let len_bytes: [u8; 4] = self.buf[1..5].try_into().unwrap_or([0u8; 4]);
        let payload_len = u32::from_le_bytes(len_bytes);
        if payload_len > self.opts.max_payload {
            return Err(WireError::PayloadTooLarge {
                got: payload_len,
                max: self.opts.max_payload,
            });
        }
        let need = 5 + payload_len as usize + if kind.has_checksum() { 8 } else { 0 };
        if self.buf.len() < need {
            return Ok(None);
        }
        // We have a full frame. Verify xxh3 if configured.
        if kind.has_checksum() {
            let checked = 5 + payload_len as usize;
            // Safe: slice is [checked..checked + 8] so exactly 8 bytes.
            let xxh_bytes: [u8; 8] = self.buf[checked..checked + 8]
                .try_into()
                .unwrap_or([0u8; 8]);
            let wire_h = u64::from_le_bytes(xxh_bytes);
            if self.opts.verify_xxh3 {
                let computed = xxh3_64(&self.buf[..checked]);
                if computed != wire_h {
                    return Err(WireError::Xxh3Mismatch {
                        kind: kind_byte,
                        wire: wire_h,
                        computed,
                    });
                }
            }
        }
        let payload = &self.buf[5..5 + payload_len as usize];
        let msg = parse_payload(kind, payload, self.opts.role)?;
        self.buf.drain(..need);
        Ok(Some(msg))
    }

    /// Number of bytes buffered but not yet parsed into a message.
    pub fn buffered_bytes(&self) -> usize {
        self.buf.len()
    }
}

fn parse_payload(kind: Kind, payload: &[u8], role: HandshakeRole) -> Result<Message, WireError> {
    match kind {
        Kind::Handshake => parse_handshake(payload, role),
        Kind::RawFrame => parse_raw_frame(payload),
        Kind::ZstdBatch => parse_zstd_batch(payload),
        Kind::PlainBatch => parse_plain_batch(payload),
        Kind::EpochBoundary => parse_epoch_boundary(payload),
        Kind::StreamError => parse_stream_error(payload),
        Kind::StreamClosed => parse_stream_closed(payload),
        Kind::Heartbeat => parse_u64_kind(payload, kind, |ns| Message::Heartbeat {
            now_unix_nanos: ns,
        }),
        Kind::AddCredit => parse_u64_kind(payload, kind, |n| Message::AddCredit { add_bytes: n }),
        Kind::Cancel => Ok(Message::Cancel {
            reason: payload.to_vec(),
        }),
        Kind::Pong => parse_u64_kind(payload, kind, |ns| Message::Pong { now_unix_nanos: ns }),
    }
}

fn parse_handshake(p: &[u8], role: HandshakeRole) -> Result<Message, WireError> {
    match role {
        HandshakeRole::ExpectClientHello => parse_client_hello(p),
        HandshakeRole::ExpectServerHello => parse_server_hello(p),
        HandshakeRole::Either => {
            if p.len() >= 8 && p[..8] == MAGIC {
                parse_client_hello(p)
            } else {
                parse_server_hello(p)
            }
        }
    }
}

fn parse_client_hello(p: &[u8]) -> Result<Message, WireError> {
    // magic(8) | version(1) | caps(1) | auth_kind(1) | auth_len(2) | auth | os_len(4) | os
    let min = 8 + 1 + 1 + 1 + 2 + 4;
    if p.len() < min {
        return Err(WireError::TruncatedPayload {
            kind: 0,
            expected: min,
            got: p.len(),
        });
    }
    if p[..8] != MAGIC {
        return Err(WireError::BadMagic {
            got: p[..8].try_into().unwrap_or([0u8; 8]),
        });
    }
    let version = p[8];
    if version != VERSION {
        return Err(WireError::BadVersion { got: version });
    }
    let caps = p[9];
    let auth_kind_raw = p[10];
    let auth_kind = AuthKind::from_u8(auth_kind_raw).ok_or(WireError::Malformed {
        kind: 0,
        detail: format!("bad auth_kind={auth_kind_raw}"),
    })?;
    let auth_len = u16::from_le_bytes(p[11..13].try_into().unwrap_or([0; 2])) as usize;
    let auth_end = 13 + auth_len;
    if p.len() < auth_end + 4 {
        return Err(WireError::TruncatedPayload {
            kind: 0,
            expected: auth_end + 4,
            got: p.len(),
        });
    }
    let auth = p[13..auth_end].to_vec();
    let os_len =
        u32::from_le_bytes(p[auth_end..auth_end + 4].try_into().unwrap_or([0; 4])) as usize;
    let os_start = auth_end + 4;
    if p.len() < os_start + os_len {
        return Err(WireError::TruncatedPayload {
            kind: 0,
            expected: os_start + os_len,
            got: p.len(),
        });
    }
    let open_stream = p[os_start..os_start + os_len].to_vec();
    Ok(Message::ClientHello(ClientHello {
        capability_flags: caps,
        auth_kind,
        auth,
        open_stream,
    }))
}

fn parse_server_hello(p: &[u8]) -> Result<Message, WireError> {
    // status(1) | chosen_mode(1) | initial_credit(8) | server_version(1) | max_msg(4) | so_len(4) | so
    let min = 1 + 1 + 8 + 1 + 4 + 4;
    if p.len() < min {
        return Err(WireError::TruncatedPayload {
            kind: 0,
            expected: min,
            got: p.len(),
        });
    }
    let status = HandshakeStatus::from_u8(p[0]);
    let chosen_mode = if p[1] == 0 {
        None
    } else {
        ChosenMode::from_u8(p[1])
    };
    let initial_credit = u64::from_le_bytes(p[2..10].try_into().unwrap_or([0; 8]));
    let server_version = p[10];
    if server_version != VERSION {
        return Err(WireError::BadVersion {
            got: server_version,
        });
    }
    let max_msg = u32::from_le_bytes(p[11..15].try_into().unwrap_or([0; 4]));
    let so_len = u32::from_le_bytes(p[15..19].try_into().unwrap_or([0; 4])) as usize;
    if p.len() < 19 + so_len {
        return Err(WireError::TruncatedPayload {
            kind: 0,
            expected: 19 + so_len,
            got: p.len(),
        });
    }
    let stream_opened = p[19..19 + so_len].to_vec();
    Ok(Message::ServerHello(ServerHello {
        status,
        chosen_mode,
        initial_credit,
        server_version,
        max_message_bytes: max_msg,
        stream_opened,
    }))
}

fn parse_raw_frame(p: &[u8]) -> Result<Message, WireError> {
    // frame_id u32 (4) + perm_seed [u8;32] (32) = 36-byte fixed header.
    const HDR: usize = 4 + 32;
    if p.len() < HDR {
        return Err(WireError::TruncatedPayload {
            kind: Kind::RawFrame as u8,
            expected: HDR,
            got: p.len(),
        });
    }
    let frame_id = u32::from_le_bytes(p[0..4].try_into().unwrap_or([0; 4]));
    let mut perm_seed = [0u8; 32];
    perm_seed.copy_from_slice(&p[4..36]);
    let zstd_bytes = p[36..].to_vec();
    Ok(Message::RawFrame {
        frame_id,
        perm_seed,
        zstd_bytes,
    })
}

fn parse_zstd_batch(p: &[u8]) -> Result<Message, WireError> {
    if p.len() < 16 {
        return Err(WireError::TruncatedPayload {
            kind: Kind::ZstdBatch as u8,
            expected: 16,
            got: p.len(),
        });
    }
    let batch_id = u64::from_le_bytes(p[0..8].try_into().unwrap_or([0; 8]));
    let epoch = u32::from_le_bytes(p[8..12].try_into().unwrap_or([0; 4]));
    let n_records = u32::from_le_bytes(p[12..16].try_into().unwrap_or([0; 4]));
    let zstd_bytes = p[16..].to_vec();
    Ok(Message::ZstdBatch {
        batch_id,
        epoch,
        n_records,
        zstd_bytes,
    })
}

fn parse_plain_batch(p: &[u8]) -> Result<Message, WireError> {
    let min = 8 + 4 + 4;
    if p.len() < min {
        return Err(WireError::TruncatedPayload {
            kind: Kind::PlainBatch as u8,
            expected: min,
            got: p.len(),
        });
    }
    let batch_id = u64::from_le_bytes(p[0..8].try_into().unwrap_or([0; 8]));
    let epoch = u32::from_le_bytes(p[8..12].try_into().unwrap_or([0; 4]));
    let n = u32::from_le_bytes(p[12..16].try_into().unwrap_or([0; 4])) as usize;

    let mut records = Vec::with_capacity(n);
    let mut cursor = 16usize;
    for _ in 0..n {
        if p.len() < cursor + 4 {
            return Err(WireError::TruncatedPayload {
                kind: Kind::PlainBatch as u8,
                expected: cursor + 4,
                got: p.len(),
            });
        }
        let len = u32::from_le_bytes(p[cursor..cursor + 4].try_into().unwrap_or([0; 4])) as usize;
        cursor += 4;
        if p.len() < cursor + len {
            return Err(WireError::TruncatedPayload {
                kind: Kind::PlainBatch as u8,
                expected: cursor + len,
                got: p.len(),
            });
        }
        records.push(p[cursor..cursor + len].to_vec());
        cursor += len;
    }
    Ok(Message::PlainBatch(BatchPayload {
        batch_id,
        epoch,
        records,
    }))
}

fn parse_epoch_boundary(p: &[u8]) -> Result<Message, WireError> {
    if p.len() != 12 {
        return Err(WireError::TruncatedPayload {
            kind: Kind::EpochBoundary as u8,
            expected: 12,
            got: p.len(),
        });
    }
    Ok(Message::EpochBoundary {
        completed_epoch: u32::from_le_bytes(p[0..4].try_into().unwrap_or([0; 4])),
        records_in_epoch: u64::from_le_bytes(p[4..12].try_into().unwrap_or([0; 8])),
    })
}

fn parse_stream_error(p: &[u8]) -> Result<Message, WireError> {
    if p.len() < 2 {
        return Err(WireError::TruncatedPayload {
            kind: Kind::StreamError as u8,
            expected: 2,
            got: p.len(),
        });
    }
    Ok(Message::StreamError {
        code: StreamErrorCode::from_u8(p[0]),
        fatal: p[1] != 0,
        detail: p[2..].to_vec(),
    })
}

fn parse_stream_closed(p: &[u8]) -> Result<Message, WireError> {
    if p.len() != 12 {
        return Err(WireError::TruncatedPayload {
            kind: Kind::StreamClosed as u8,
            expected: 12,
            got: p.len(),
        });
    }
    Ok(Message::StreamClosed {
        total_records: u64::from_le_bytes(p[0..8].try_into().unwrap_or([0; 8])),
        epochs_completed: u32::from_le_bytes(p[8..12].try_into().unwrap_or([0; 4])),
    })
}

fn parse_u64_kind(
    p: &[u8],
    kind: Kind,
    build: impl FnOnce(u64) -> Message,
) -> Result<Message, WireError> {
    if p.len() != 8 {
        return Err(WireError::TruncatedPayload {
            kind: kind as u8,
            expected: 8,
            got: p.len(),
        });
    }
    let v = u64::from_le_bytes(p[0..8].try_into().unwrap_or([0; 8]));
    Ok(build(v))
}
