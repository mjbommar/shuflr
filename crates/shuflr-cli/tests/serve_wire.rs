//! PR-33 integration tests for the `shuflr-wire/1` transport.
//!
//! Spawn the real shuflr binary with `--wire 127.0.0.1:0 ...` and
//! drive the handshake / stream protocol from a tokio-based test
//! client using the `shuflr-wire` codec directly. Verifies bytes over
//! the socket match the PlainBatch-mode pipeline output record-for-
//! record.

#![cfg(feature = "serve")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::io::{BufRead, BufReader};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use shuflr_wire::{
    AuthKind, BatchPayload, ChosenMode, ClientHello, DecodeOptions, Decoder, HandshakeRole,
    HandshakeStatus, Message, encode,
};

fn shuflr_bin() -> PathBuf {
    assert_cmd::cargo::cargo_bin("shuflr")
}

fn pick_port() -> u16 {
    let lst = TcpListener::bind("127.0.0.1:0").unwrap();
    let p = lst.local_addr().unwrap().port();
    drop(lst);
    p
}

struct ServeGuard {
    child: Child,
    port: u16,
}
impl Drop for ServeGuard {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn spawn_wire_server(datasets: &[(&str, &Path)], extra: &[&str]) -> ServeGuard {
    let port = pick_port();
    let mut cmd = Command::new(shuflr_bin());
    cmd.arg("serve")
        .arg("--wire")
        .arg(format!("127.0.0.1:{port}"))
        .arg("--log-level")
        .arg("info");
    for (id, p) in datasets {
        cmd.arg("--dataset").arg(format!("{id}={}", p.display()));
    }
    for a in extra {
        cmd.arg(a);
    }
    let mut child = cmd
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn shuflr");
    let err = child.stderr.take().unwrap();
    let reader = BufReader::new(err);
    let (tx, rx) = std::sync::mpsc::channel();
    // Keep streaming stderr to the host test process so any handler
    // errors from the server are visible when a test fails.
    std::thread::spawn(move || {
        let mut ready = false;
        for line in reader.lines().map_while(Result::ok) {
            if !ready && line.contains("serve(wire) bound") {
                let _ = tx.send(());
                ready = true;
            }
            eprintln!("[serve] {line}");
        }
    });
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if Instant::now() > deadline {
            panic!("wire server never bound");
        }
        match rx.recv_timeout(Duration::from_millis(50)) {
            Ok(()) => break,
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                if let Ok(Some(s)) = child.try_wait() {
                    panic!("server exited early: {s:?}");
                }
            }
            Err(_) => panic!("stderr disconnected"),
        }
    }
    ServeGuard { child, port }
}

/// Drive one handshake + stream from `127.0.0.1:port` using tokio,
/// collect all records, return them as a sorted list.
fn fetch_records(
    port: u16,
    open_stream_json: &str,
    auth: &[u8],
    auth_kind: AuthKind,
) -> Vec<Vec<u8>> {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut s = tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .unwrap();
        // ClientHello.
        let hello = Message::ClientHello(ClientHello {
            capability_flags: 0b111,
            auth_kind,
            auth: auth.to_vec(),
            open_stream: open_stream_json.as_bytes().to_vec(),
        });
        s.write_all(&encode(&hello)).await.unwrap();
        s.flush().await.unwrap();

        // Read until StreamClosed or StreamError.
        let mut d = Decoder::new(DecodeOptions {
            role: HandshakeRole::ExpectServerHello,
            ..Default::default()
        });
        let mut scratch = vec![0u8; 64 * 1024];
        let mut out = Vec::new();
        let mut got_hello = false;
        loop {
            let n = s.read(&mut scratch).await.unwrap();
            if n == 0 {
                break;
            }
            d.feed(&scratch[..n]);
            loop {
                let msg = match d.try_next() {
                    Ok(Some(m)) => m,
                    Ok(None) => break,
                    Err(e) => panic!("decode: {e}"),
                };
                match msg {
                    Message::ServerHello(h) => {
                        got_hello = true;
                        if h.status != HandshakeStatus::Ok {
                            let detail = String::from_utf8_lossy(&h.stream_opened);
                            panic!("server rejected: {detail}");
                        }
                        // fetch_records callers use plain-JSONL inputs
                        // so RawFrame can't be selected. Pin that here.
                        assert_eq!(h.chosen_mode, Some(ChosenMode::PlainBatch));
                    }
                    Message::PlainBatch(batch) => {
                        out.extend(batch.records);
                    }
                    Message::EpochBoundary { .. } => {}
                    Message::StreamClosed { .. } => {
                        return out;
                    }
                    Message::StreamError {
                        code,
                        fatal,
                        detail,
                    } => {
                        panic!(
                            "server StreamError {code:?} fatal={fatal} detail={}",
                            String::from_utf8_lossy(&detail)
                        );
                    }
                    other => panic!("unexpected server message {other:?}"),
                }
            }
        }
        assert!(got_hello, "server never sent hello");
        out
    })
}

fn fetch_records_with_meta(
    port: u16,
    open_stream_json: &str,
) -> (Vec<Vec<u8>>, Vec<(u32, u64)>, (u64, u32), Vec<u32>) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut s = tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .unwrap();
        let hello = Message::ClientHello(ClientHello {
            capability_flags: 0,
            auth_kind: AuthKind::None,
            auth: vec![],
            open_stream: open_stream_json.as_bytes().to_vec(),
        });
        s.write_all(&encode(&hello)).await.unwrap();
        s.flush().await.unwrap();

        let mut d = Decoder::new(DecodeOptions {
            role: HandshakeRole::ExpectServerHello,
            ..Default::default()
        });
        let mut scratch = vec![0u8; 64 * 1024];
        let mut out = Vec::new();
        let mut batch_epochs = Vec::new();
        let mut boundaries = Vec::new();
        loop {
            let n = s.read(&mut scratch).await.unwrap();
            if n == 0 {
                panic!("eof before StreamClosed");
            }
            d.feed(&scratch[..n]);
            while let Some(msg) = d.try_next().unwrap() {
                match msg {
                    Message::ServerHello(h) => {
                        assert_eq!(h.status, HandshakeStatus::Ok);
                        assert_eq!(h.chosen_mode, Some(ChosenMode::PlainBatch));
                    }
                    Message::PlainBatch(BatchPayload { epoch, records, .. }) => {
                        batch_epochs.push(epoch);
                        out.extend(records);
                    }
                    Message::EpochBoundary {
                        completed_epoch,
                        records_in_epoch,
                    } => {
                        boundaries.push((completed_epoch, records_in_epoch));
                    }
                    Message::StreamClosed {
                        total_records,
                        epochs_completed,
                    } => {
                        return (
                            out,
                            boundaries,
                            (total_records, epochs_completed),
                            batch_epochs,
                        );
                    }
                    Message::StreamError {
                        code,
                        fatal,
                        detail,
                    } => {
                        panic!(
                            "server StreamError {code:?} fatal={fatal} detail={}",
                            String::from_utf8_lossy(&detail)
                        );
                    }
                    other => panic!("unexpected server message {other:?}"),
                }
            }
        }
    })
}

#[test]
fn wire_roundtrip_plain_batch_with_none_shuffle() {
    let tmp = tempfile::tempdir().unwrap();
    let ds = tmp.path().join("c.jsonl");
    let records: Vec<String> = (0..25).map(|i| format!("{{\"i\":{i:02}}}")).collect();
    std::fs::write(&ds, records.join("\n") + "\n").unwrap();
    let g = spawn_wire_server(&[("corpus", &ds)], &[]);

    let got = fetch_records(
        g.port,
        r#"{"dataset_id":"corpus","seed":0,"shuffle":"none"}"#,
        b"",
        AuthKind::None,
    );
    let mut got_str: Vec<String> = got
        .into_iter()
        .map(|r| String::from_utf8(r).unwrap())
        .collect();
    got_str.sort();
    let mut want = records;
    want.sort();
    assert_eq!(got_str, want);
}

#[test]
fn wire_roundtrip_buffer_preserves_multiset() {
    use std::collections::BTreeSet;
    let tmp = tempfile::tempdir().unwrap();
    let ds = tmp.path().join("c.jsonl");
    let records: Vec<String> = (0..200).map(|i| format!("{{\"i\":{i:03}}}")).collect();
    std::fs::write(&ds, records.join("\n") + "\n").unwrap();
    let g = spawn_wire_server(&[("corpus", &ds)], &[]);

    let got = fetch_records(
        g.port,
        r#"{"dataset_id":"corpus","seed":7,"shuffle":"buffer","max_batch_records":32}"#,
        b"",
        AuthKind::None,
    );
    let got_set: BTreeSet<String> = got
        .into_iter()
        .map(|r| String::from_utf8(r).unwrap())
        .collect();
    let want_set: BTreeSet<String> = records.into_iter().collect();
    assert_eq!(got_set, want_set);
}

#[test]
fn wire_sample_caps_record_count() {
    let tmp = tempfile::tempdir().unwrap();
    let ds = tmp.path().join("c.jsonl");
    std::fs::write(&ds, (0..500).map(|i| format!("r{i}\n")).collect::<String>()).unwrap();
    let g = spawn_wire_server(&[("corpus", &ds)], &[]);
    let got = fetch_records(
        g.port,
        r#"{"dataset_id":"corpus","seed":0,"shuffle":"none","sample":17}"#,
        b"",
        AuthKind::None,
    );
    assert_eq!(got.len(), 17);
}

#[test]
fn wire_epochs_emit_boundaries_and_close_count() {
    let tmp = tempfile::tempdir().unwrap();
    let ds = tmp.path().join("c.jsonl");
    std::fs::write(&ds, "a\nb\nc\n").unwrap();
    let g = spawn_wire_server(&[("corpus", &ds)], &[]);

    let (got, boundaries, closed, batch_epochs) = fetch_records_with_meta(
        g.port,
        r#"{"dataset_id":"corpus","seed":0,"shuffle":"none","epochs":2,"max_batch_records":2}"#,
    );
    let got_str: Vec<String> = got
        .into_iter()
        .map(|r| String::from_utf8(r).unwrap())
        .collect();
    assert_eq!(got_str, vec!["a", "b", "c", "a", "b", "c"]);
    assert_eq!(boundaries, vec![(0, 3), (1, 3)]);
    assert_eq!(closed, (6, 2));
    assert!(batch_epochs.contains(&0), "batch epochs: {batch_epochs:?}");
    assert!(batch_epochs.contains(&1), "batch epochs: {batch_epochs:?}");
}

#[test]
fn wire_unknown_dataset_is_rejected_in_handshake() {
    let tmp = tempfile::tempdir().unwrap();
    let ds = tmp.path().join("c.jsonl");
    std::fs::write(&ds, "x\n").unwrap();
    let g = spawn_wire_server(&[("real", &ds)], &[]);

    let rt = tokio::runtime::Runtime::new().unwrap();
    let detail = rt.block_on(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut s = tokio::net::TcpStream::connect(("127.0.0.1", g.port))
            .await
            .unwrap();
        let hello = Message::ClientHello(ClientHello {
            capability_flags: 0,
            auth_kind: AuthKind::None,
            auth: vec![],
            open_stream: br#"{"dataset_id":"missing"}"#.to_vec(),
        });
        s.write_all(&encode(&hello)).await.unwrap();
        s.flush().await.unwrap();
        let mut d = Decoder::new(DecodeOptions {
            role: HandshakeRole::ExpectServerHello,
            ..Default::default()
        });
        let mut scratch = vec![0u8; 8192];
        loop {
            let n = s.read(&mut scratch).await.unwrap();
            if n == 0 {
                panic!("server closed before sending hello");
            }
            d.feed(&scratch[..n]);
            if let Some(Message::ServerHello(h)) = d.try_next().unwrap() {
                assert_eq!(h.status, HandshakeStatus::Error);
                return String::from_utf8_lossy(&h.stream_opened).into_owned();
            }
        }
    });
    assert!(
        detail.contains("unknown dataset"),
        "unexpected detail: {detail}"
    );
}

#[test]
fn wire_bearer_auth_accepts_right_token() {
    let tmp = tempfile::tempdir().unwrap();
    let ds = tmp.path().join("c.jsonl");
    std::fs::write(&ds, "a\nb\n").unwrap();
    let tok = tmp.path().join("tokens.txt");
    std::fs::write(&tok, "secret\n").unwrap();
    let g = spawn_wire_server(
        &[("c", &ds)],
        &["--auth", "bearer", "--auth-tokens", tok.to_str().unwrap()],
    );

    // Right token → Ok.
    let got = fetch_records(
        g.port,
        r#"{"dataset_id":"c","seed":0,"shuffle":"none"}"#,
        b"secret",
        AuthKind::Bearer,
    );
    let mut got_str: Vec<_> = got
        .into_iter()
        .map(|b| String::from_utf8(b).unwrap())
        .collect();
    got_str.sort();
    assert_eq!(got_str, vec!["a".to_string(), "b".to_string()]);
}

#[test]
fn wire_bearer_auth_rejects_wrong_token() {
    let tmp = tempfile::tempdir().unwrap();
    let ds = tmp.path().join("c.jsonl");
    std::fs::write(&ds, "a\nb\n").unwrap();
    let tok = tmp.path().join("tokens.txt");
    std::fs::write(&tok, "secret\n").unwrap();
    let g = spawn_wire_server(
        &[("c", &ds)],
        &["--auth", "bearer", "--auth-tokens", tok.to_str().unwrap()],
    );

    let rt = tokio::runtime::Runtime::new().unwrap();
    let detail = rt.block_on(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut s = tokio::net::TcpStream::connect(("127.0.0.1", g.port))
            .await
            .unwrap();
        let hello = Message::ClientHello(ClientHello {
            capability_flags: 0,
            auth_kind: AuthKind::Bearer,
            auth: b"wrong".to_vec(),
            open_stream: br#"{"dataset_id":"c","shuffle":"none"}"#.to_vec(),
        });
        s.write_all(&encode(&hello)).await.unwrap();
        let mut d = Decoder::new(DecodeOptions {
            role: HandshakeRole::ExpectServerHello,
            ..Default::default()
        });
        let mut scratch = vec![0u8; 8192];
        loop {
            let n = s.read(&mut scratch).await.unwrap();
            if n == 0 {
                panic!("eof before hello");
            }
            d.feed(&scratch[..n]);
            if let Some(Message::ServerHello(h)) = d.try_next().unwrap() {
                assert_eq!(h.status, HandshakeStatus::Error);
                return String::from_utf8_lossy(&h.stream_opened).into_owned();
            }
        }
    });
    assert!(detail.contains("auth"), "expected auth error: {detail}");
}

// ---- PR-33b raw-frame tests ----

fn convert_to_seekable_zstd(plain: &std::path::Path, out: &std::path::Path) {
    let status = Command::new(shuflr_bin())
        .args(["convert", "--log-level", "warn", "-o"])
        .arg(out)
        .arg(plain)
        .status()
        .unwrap();
    assert!(status.success(), "convert failed");
}

#[test]
fn wire_negotiates_raw_frame_on_chunk_shuffled_seekable() {
    // When the client advertises raw-frame capability AND the dataset
    // is seekable-zstd AND shuffle=chunk-shuffled, the server must
    // pick ChosenMode::RawFrame.
    let tmp = tempfile::tempdir().unwrap();
    let plain = tmp.path().join("c.jsonl");
    let seekable = tmp.path().join("c.jsonl.zst");
    let records: Vec<String> = (0..200).map(|i| format!("r{i:03}")).collect();
    std::fs::write(&plain, records.join("\n") + "\n").unwrap();
    convert_to_seekable_zstd(&plain, &seekable);
    let g = spawn_wire_server(&[("corpus", &seekable)], &[]);

    let rt = tokio::runtime::Runtime::new().unwrap();
    let (chosen, collected) = rt.block_on(async move {
        use rand::SeedableRng;
        use rand::seq::SliceRandom;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut s = tokio::net::TcpStream::connect(("127.0.0.1", g.port))
            .await
            .unwrap();
        let open = r#"{"dataset_id":"corpus","seed":7,"shuffle":"chunk-shuffled"}"#;
        let hello = Message::ClientHello(ClientHello {
            capability_flags: 0b111, // raw-frame capable
            auth_kind: AuthKind::None,
            auth: vec![],
            open_stream: open.as_bytes().to_vec(),
        });
        s.write_all(&encode(&hello)).await.unwrap();

        let mut d = Decoder::new(DecodeOptions {
            role: HandshakeRole::ExpectServerHello,
            ..Default::default()
        });
        let mut scratch = vec![0u8; 64 * 1024];
        let mut chosen = None;
        let mut collected: Vec<Vec<u8>> = Vec::new();
        loop {
            let n = s.read(&mut scratch).await.unwrap();
            if n == 0 {
                break;
            }
            d.feed(&scratch[..n]);
            while let Some(msg) = d.try_next().unwrap() {
                match msg {
                    Message::ServerHello(h) => {
                        assert_eq!(h.status, HandshakeStatus::Ok);
                        chosen = h.chosen_mode;
                    }
                    Message::RawFrame {
                        zstd_bytes,
                        perm_seed,
                        ..
                    } => {
                        let decoded = zstd::stream::decode_all(&zstd_bytes[..]).unwrap();
                        let mut recs = Vec::new();
                        let mut start = 0usize;
                        for nl in memchr::memchr_iter(b'\n', &decoded) {
                            recs.push(decoded[start..nl].to_vec());
                            start = nl + 1;
                        }
                        if start < decoded.len() {
                            recs.push(decoded[start..].to_vec());
                        }
                        let mut rng = rand_chacha::ChaCha20Rng::from_seed(perm_seed);
                        recs.shuffle(&mut rng);
                        collected.extend(recs);
                    }
                    Message::PlainBatch(BatchPayload { records, .. }) => {
                        collected.extend(records);
                    }
                    Message::EpochBoundary { .. } => {}
                    Message::StreamClosed { .. } => return (chosen, collected),
                    Message::StreamError {
                        code,
                        fatal,
                        detail,
                    } => {
                        panic!(
                            "server error {code:?} fatal={fatal} detail={}",
                            String::from_utf8_lossy(&detail)
                        );
                    }
                    other => panic!("unexpected: {other:?}"),
                }
            }
        }
        (chosen, collected)
    });
    assert_eq!(chosen, Some(ChosenMode::RawFrame));
    let mut got: Vec<_> = collected
        .iter()
        .map(|b| std::str::from_utf8(b).unwrap().to_string())
        .collect();
    got.sort();
    let mut want = records;
    want.sort();
    assert_eq!(got, want);
}

#[test]
fn wire_falls_back_to_plain_batch_when_shuffle_is_not_chunk_shuffled() {
    // Raw-frame requires shuffle=chunk-shuffled. For buffer/none/
    // index-perm the server must stay on plain-batch.
    let tmp = tempfile::tempdir().unwrap();
    let plain = tmp.path().join("c.jsonl");
    let seekable = tmp.path().join("c.jsonl.zst");
    std::fs::write(&plain, "a\nb\nc\nd\n").unwrap();
    convert_to_seekable_zstd(&plain, &seekable);
    let g = spawn_wire_server(&[("corpus", &seekable)], &[]);

    let rt = tokio::runtime::Runtime::new().unwrap();
    let chosen = rt.block_on(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut s = tokio::net::TcpStream::connect(("127.0.0.1", g.port))
            .await
            .unwrap();
        let open = r#"{"dataset_id":"corpus","seed":0,"shuffle":"buffer"}"#;
        let hello = Message::ClientHello(ClientHello {
            capability_flags: 0b111, // raw-frame capable
            auth_kind: AuthKind::None,
            auth: vec![],
            open_stream: open.as_bytes().to_vec(),
        });
        s.write_all(&encode(&hello)).await.unwrap();
        let mut d = Decoder::new(DecodeOptions {
            role: HandshakeRole::ExpectServerHello,
            ..Default::default()
        });
        let mut scratch = vec![0u8; 8192];
        loop {
            let n = s.read(&mut scratch).await.unwrap();
            if n == 0 {
                panic!("eof before hello");
            }
            d.feed(&scratch[..n]);
            if let Some(Message::ServerHello(h)) = d.try_next().unwrap() {
                return h.chosen_mode;
            }
        }
    });
    assert_eq!(chosen, Some(ChosenMode::PlainBatch));
}
