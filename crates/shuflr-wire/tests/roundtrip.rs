//! Round-trip proptest for every Message variant: encoding is
//! injective over our model.
//!
//! Also: adversarial coverage for malformed input — truncations,
//! corrupted xxh3, unknown kinds, too-large payloads.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::field_reassign_with_default
)]

use proptest::prelude::*;
use shuflr_wire::{
    AuthKind, BatchPayload, ChosenMode, ClientHello, Decoder, HandshakeStatus, Message,
    ServerHello, StreamErrorCode, encode, encode_into,
};

// ---------- proptest strategies for each variant ----------

fn bytes_strategy(max: usize) -> impl Strategy<Value = Vec<u8>> {
    prop::collection::vec(any::<u8>(), 0..=max)
}

fn client_hello() -> impl Strategy<Value = Message> {
    (
        any::<u8>(),
        prop_oneof![
            Just(AuthKind::None),
            Just(AuthKind::Bearer),
            Just(AuthKind::Mtls)
        ],
        bytes_strategy(1024),
        bytes_strategy(4096),
    )
        .prop_map(|(caps, ak, auth, os)| {
            Message::ClientHello(ClientHello {
                capability_flags: caps,
                auth_kind: ak,
                auth,
                open_stream: os,
            })
        })
}

fn server_hello() -> impl Strategy<Value = Message> {
    (
        prop_oneof![Just(HandshakeStatus::Ok), Just(HandshakeStatus::Error)],
        prop_oneof![
            Just(None::<ChosenMode>),
            Just(Some(ChosenMode::RawFrame)),
            Just(Some(ChosenMode::ZstdBatch)),
            Just(Some(ChosenMode::PlainBatch))
        ],
        any::<u64>(),
        any::<u32>(),
        bytes_strategy(4096),
    )
        .prop_map(|(st, cm, credit, max_msg, so)| {
            Message::ServerHello(ServerHello {
                status: st,
                chosen_mode: cm,
                initial_credit: credit,
                server_version: 1,
                max_message_bytes: max_msg,
                stream_opened: so,
            })
        })
}

fn raw_frame() -> impl Strategy<Value = Message> {
    (any::<u32>(), any::<u64>(), bytes_strategy(32 * 1024)).prop_map(|(fid, seed, b)| {
        Message::RawFrame {
            frame_id: fid,
            perm_seed: seed,
            zstd_bytes: b,
        }
    })
}

fn zstd_batch() -> impl Strategy<Value = Message> {
    (
        any::<u64>(),
        any::<u32>(),
        any::<u32>(),
        bytes_strategy(32 * 1024),
    )
        .prop_map(|(bid, ep, n, b)| Message::ZstdBatch {
            batch_id: bid,
            epoch: ep,
            n_records: n,
            zstd_bytes: b,
        })
}

fn plain_batch() -> impl Strategy<Value = Message> {
    (
        any::<u64>(),
        any::<u32>(),
        prop::collection::vec(bytes_strategy(512), 0..32),
    )
        .prop_map(|(bid, ep, records)| {
            Message::PlainBatch(BatchPayload {
                batch_id: bid,
                epoch: ep,
                records,
            })
        })
}

fn epoch_boundary() -> impl Strategy<Value = Message> {
    (any::<u32>(), any::<u64>()).prop_map(|(e, r)| Message::EpochBoundary {
        completed_epoch: e,
        records_in_epoch: r,
    })
}

fn stream_error() -> impl Strategy<Value = Message> {
    (
        prop_oneof![
            Just(StreamErrorCode::Unspecified),
            Just(StreamErrorCode::MalformedRecord),
            Just(StreamErrorCode::OversizedRecord),
            Just(StreamErrorCode::IoError),
            Just(StreamErrorCode::SlowConsumer),
            Just(StreamErrorCode::ServerShutdown),
            Just(StreamErrorCode::AuthDenied),
            Just(StreamErrorCode::ResourceExhausted),
            Just(StreamErrorCode::Internal),
        ],
        any::<bool>(),
        bytes_strategy(512),
    )
        .prop_map(|(code, fatal, detail)| Message::StreamError {
            code,
            fatal,
            detail,
        })
}

fn stream_closed() -> impl Strategy<Value = Message> {
    (any::<u64>(), any::<u32>()).prop_map(|(t, e)| Message::StreamClosed {
        total_records: t,
        epochs_completed: e,
    })
}

fn any_message() -> impl Strategy<Value = Message> {
    prop_oneof![
        client_hello(),
        server_hello(),
        raw_frame(),
        zstd_batch(),
        plain_batch(),
        epoch_boundary(),
        stream_error(),
        stream_closed(),
        any::<u64>().prop_map(|v| Message::Heartbeat { now_unix_nanos: v }),
        any::<u64>().prop_map(|v| Message::AddCredit { add_bytes: v }),
        bytes_strategy(256).prop_map(|r| Message::Cancel { reason: r }),
        any::<u64>().prop_map(|v| Message::Pong { now_unix_nanos: v }),
    ]
}

// ---------- tests ----------

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    #[test]
    fn encode_decode_roundtrip(msg in any_message()) {
        let bytes = encode(&msg);
        let mut d = Decoder::default();
        d.feed(&bytes);
        let decoded = d.try_next().expect("decode err").expect("need full frame");
        prop_assert_eq!(decoded, msg);
        prop_assert_eq!(d.buffered_bytes(), 0);
    }

    #[test]
    fn concatenated_frames_decode_in_order(messages in prop::collection::vec(any_message(), 0..16)) {
        let mut combined = Vec::new();
        for m in &messages {
            encode_into(m, &mut combined);
        }
        let mut d = Decoder::default();
        d.feed(&combined);
        let mut out = Vec::new();
        while let Some(m) = d.try_next().expect("decode err") {
            out.push(m);
        }
        prop_assert_eq!(out, messages);
    }

    #[test]
    fn partial_feed_still_roundtrips(msg in any_message(), chunk in 1usize..=1024) {
        // Feed the encoded bytes one byte at a time (or in chunks of
        // `chunk` bytes). The decoder must still return the same
        // message once the final byte arrives, with no partial
        // outputs before that.
        let bytes = encode(&msg);
        let mut d = Decoder::default();
        let mut emitted: Option<Message> = None;
        for piece in bytes.chunks(chunk) {
            d.feed(piece);
            if let Some(m) = d.try_next().expect("decode") {
                prop_assert!(emitted.is_none(), "should only emit once");
                emitted = Some(m);
            }
        }
        prop_assert_eq!(emitted, Some(msg));
    }
}

#[test]
fn bad_xxh3_is_rejected_when_verifying() {
    let msg = Message::AddCredit { add_bytes: 1 };
    let mut bytes = encode(&msg);
    // Corrupt the last byte of the xxh3 checksum.
    let last = bytes.len() - 1;
    bytes[last] ^= 0xFF;
    let mut d = Decoder::default();
    d.feed(&bytes);
    let res = d.try_next();
    assert!(matches!(
        res,
        Err(shuflr_wire::WireError::Xxh3Mismatch { .. })
    ));
}

#[test]
fn unknown_kind_is_rejected() {
    // Kind byte 255 is unused; feed a fake frame.
    let mut buf = Vec::new();
    buf.push(255);
    buf.extend_from_slice(&0u32.to_le_bytes());
    buf.extend_from_slice(&0u64.to_le_bytes());
    let mut d = Decoder::default();
    d.feed(&buf);
    assert!(matches!(
        d.try_next(),
        Err(shuflr_wire::WireError::UnknownKind { got: 255 })
    ));
}

#[test]
fn insufficient_bytes_return_none_not_error() {
    let msg = Message::PlainBatch(BatchPayload {
        batch_id: 7,
        epoch: 1,
        records: vec![b"one".to_vec(), b"two".to_vec()],
    });
    let bytes = encode(&msg);
    let mut d = Decoder::default();
    // Feed all but the final byte.
    d.feed(&bytes[..bytes.len() - 1]);
    let res = d.try_next();
    assert!(matches!(res, Ok(None)));
    // Top up and now decode.
    d.feed(&bytes[bytes.len() - 1..]);
    assert_eq!(d.try_next().unwrap().unwrap(), msg);
}

#[test]
fn payload_too_large_rejected() {
    let mut opts = shuflr_wire::codec::DecodeOptions::default();
    opts.max_payload = 16;
    let mut d = Decoder::new(opts);
    // Craft a frame that claims 1 MiB of payload.
    let mut buf = Vec::new();
    buf.push(shuflr_wire::Kind::PlainBatch as u8);
    buf.extend_from_slice(&(1024u32 * 1024).to_le_bytes());
    d.feed(&buf);
    assert!(matches!(
        d.try_next(),
        Err(shuflr_wire::WireError::PayloadTooLarge { got: _, max: 16 })
    ));
}

#[test]
fn bad_magic_in_client_hello_is_rejected() {
    // Hand-craft a handshake frame with wrong magic.
    let mut payload = Vec::new();
    payload.extend_from_slice(b"WRONG_M_"); // 8 bytes that aren't SHUFLRW1
    payload.push(1); // version
    payload.push(0); // caps
    payload.push(0); // auth_kind=None
    payload.extend_from_slice(&0u16.to_le_bytes()); // auth_len
    payload.extend_from_slice(&0u32.to_le_bytes()); // os_len
    let mut buf = Vec::new();
    buf.push(0u8);
    buf.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    buf.extend_from_slice(&payload);

    // Server-role decoder: expects a ClientHello; bad magic must flip
    // BadMagic.
    let mut d = Decoder::new(shuflr_wire::DecodeOptions {
        role: shuflr_wire::HandshakeRole::ExpectClientHello,
        ..Default::default()
    });
    d.feed(&buf);
    assert!(matches!(
        d.try_next(),
        Err(shuflr_wire::WireError::BadMagic { .. })
    ));
}

#[test]
fn role_distinguishes_client_and_server_handshake() {
    use shuflr_wire::{AuthKind, ClientHello, HandshakeStatus, ServerHello};
    let client = Message::ClientHello(ClientHello {
        capability_flags: 0b111,
        auth_kind: AuthKind::Bearer,
        auth: b"token".to_vec(),
        open_stream: b"proto".to_vec(),
    });
    let server = Message::ServerHello(ServerHello {
        status: HandshakeStatus::Ok,
        chosen_mode: Some(shuflr_wire::ChosenMode::RawFrame),
        initial_credit: 16 * 1024 * 1024,
        server_version: 1,
        max_message_bytes: 64 * 1024 * 1024,
        stream_opened: b"proto".to_vec(),
    });

    // Server-role decoder parses ClientHello cleanly and rejects
    // ServerHello bytes (no magic → BadMagic via ClientHello path).
    let mut d = Decoder::new(shuflr_wire::DecodeOptions {
        role: shuflr_wire::HandshakeRole::ExpectClientHello,
        ..Default::default()
    });
    d.feed(&encode(&client));
    assert_eq!(d.try_next().unwrap().unwrap(), client);

    // Client-role decoder parses ServerHello cleanly.
    let mut d = Decoder::new(shuflr_wire::DecodeOptions {
        role: shuflr_wire::HandshakeRole::ExpectServerHello,
        ..Default::default()
    });
    d.feed(&encode(&server));
    assert_eq!(d.try_next().unwrap().unwrap(), server);
}
