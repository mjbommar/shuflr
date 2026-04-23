//! End-to-end tests for the HTTP transport (PR-30).
//!
//! Each test spawns the real `shuflr` binary with `serve --http 127.0.0.1:0`
//! and hits it with a blocking HTTP client. The tests are gated on the
//! `serve` feature — if you `cargo test` without features, they skip.

#![cfg(feature = "serve")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::io::{BufRead, BufReader};
use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

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

impl ServeGuard {
    fn base(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }
}

fn pick_port() -> u16 {
    // Grab a free port, then release it; race with other processes is
    // possible but unlikely in a short-lived test.
    let lst = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = lst.local_addr().unwrap().port();
    drop(lst);
    port
}

fn shuflr_bin() -> std::path::PathBuf {
    // assert_cmd's Command::cargo_bin would work too, but we want to
    // spawn with Stdio::piped to wait for "bound" on stderr.
    assert_cmd::cargo::cargo_bin("shuflr")
}

fn spawn_serve(datasets: &[(&str, &std::path::Path)]) -> ServeGuard {
    let port = pick_port();
    let mut cmd = Command::new(shuflr_bin());
    // info-level so the "serve(http) bound" readiness line appears.
    cmd.arg("serve")
        .arg("--http")
        .arg(format!("127.0.0.1:{port}"))
        .arg("--log-level")
        .arg("info");
    for (id, path) in datasets {
        cmd.arg("--dataset").arg(format!("{id}={}", path.display()));
    }
    let mut child = cmd
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn shuflr serve");

    // Wait until the "bound" log line appears — that's our ready signal.
    // Timebox at 10 s so CI failures surface fast.
    let stderr = child.stderr.take().expect("stderr");
    let reader = BufReader::new(stderr);
    let deadline = Instant::now() + Duration::from_secs(10);
    let (found_tx, found_rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        for line in reader.lines().map_while(Result::ok) {
            if line.contains("serve(http) bound") {
                let _ = found_tx.send(());
                break;
            }
        }
    });
    loop {
        if Instant::now() >= deadline {
            panic!("serve never logged 'bound' within 10s");
        }
        match found_rx.recv_timeout(Duration::from_millis(50)) {
            Ok(()) => break,
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                if let Ok(Some(status)) = child.try_wait() {
                    panic!("serve died before ready: {status:?}");
                }
            }
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                panic!("stderr reader disconnected before serve was ready");
            }
        }
    }

    ServeGuard { child, port }
}

#[test]
fn health_returns_serving() {
    let tmp = tempfile::tempdir().unwrap();
    let ds = tmp.path().join("d.jsonl");
    std::fs::write(&ds, "a\nb\nc\n").unwrap();
    let g = spawn_serve(&[("main", &ds)]);

    let body: String = ureq::get(&format!("{}/v1/health", g.base()))
        .call()
        .unwrap()
        .into_string()
        .unwrap();
    assert!(body.contains("SERVING"), "body: {body}");
}

#[test]
fn datasets_list_includes_registered_ids() {
    let tmp = tempfile::tempdir().unwrap();
    let a = tmp.path().join("a.jsonl");
    let b = tmp.path().join("b.jsonl");
    std::fs::write(&a, "x\n").unwrap();
    std::fs::write(&b, "y\n").unwrap();
    let g = spawn_serve(&[("first", &a), ("second", &b)]);

    let body: String = ureq::get(&format!("{}/v1/datasets", g.base()))
        .call()
        .unwrap()
        .into_string()
        .unwrap();
    assert!(body.contains("first"), "body: {body}");
    assert!(body.contains("second"), "body: {body}");
    assert!(body.contains("fingerprint"), "body: {body}");
}

#[test]
fn stream_ndjson_preserves_records_and_headers() {
    use std::collections::BTreeSet;

    let tmp = tempfile::tempdir().unwrap();
    let ds = tmp.path().join("s.jsonl");
    let records: Vec<String> = (0..200).map(|i| format!("{{\"i\":{i:03}}}\n")).collect();
    std::fs::write(&ds, records.concat()).unwrap();

    let g = spawn_serve(&[("corpus", &ds)]);

    let resp = ureq::get(&format!(
        "{}/v1/streams/corpus?shuffle=buffer&seed=7",
        g.base()
    ))
    .call()
    .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.header("shuflr-effective-seed"), Some("7"));
    assert!(
        resp.header("shuflr-fingerprint")
            .unwrap()
            .starts_with("blake3:")
    );
    assert_eq!(
        resp.header("content-type"),
        Some("application/x-ndjson; charset=utf-8")
    );

    let body = resp.into_string().unwrap();
    let lines: BTreeSet<&str> = body.lines().collect();
    let expected: BTreeSet<&str> = records.iter().map(|s| s.trim_end()).collect();
    assert_eq!(lines, expected, "multiset preserved under shuffle=buffer");
}

#[test]
fn stream_sample_caps_records() {
    let tmp = tempfile::tempdir().unwrap();
    let ds = tmp.path().join("s.jsonl");
    let records: String = (0..500).map(|i| format!("{{\"i\":{i:03}}}\n")).collect();
    std::fs::write(&ds, &records).unwrap();

    let g = spawn_serve(&[("c", &ds)]);

    let resp = ureq::get(&format!("{}/v1/streams/c?shuffle=none&sample=25", g.base()))
        .call()
        .unwrap();
    let body = resp.into_string().unwrap();
    assert_eq!(body.lines().count(), 25, "body={body:?}");
}

#[test]
fn unknown_dataset_returns_404_json() {
    let tmp = tempfile::tempdir().unwrap();
    let ds = tmp.path().join("x.jsonl");
    std::fs::write(&ds, "a\n").unwrap();
    let g = spawn_serve(&[("real", &ds)]);

    let resp = ureq::get(&format!("{}/v1/streams/nope?shuffle=none", g.base())).call();
    match resp {
        Err(ureq::Error::Status(404, r)) => {
            let body = r.into_string().unwrap_or_default();
            assert!(body.contains("unknown_dataset"), "body={body}");
        }
        other => panic!("expected 404 Status, got {other:?}"),
    }
}

#[test]
fn bad_query_returns_400() {
    let tmp = tempfile::tempdir().unwrap();
    let ds = tmp.path().join("x.jsonl");
    std::fs::write(&ds, "a\n").unwrap();
    let g = spawn_serve(&[("c", &ds)]);

    let resp = ureq::get(&format!("{}/v1/streams/c?seed=notanumber", g.base())).call();
    match resp {
        Err(ureq::Error::Status(400, _)) => {}
        other => panic!("expected 400, got {other:?}"),
    }
}

#[test]
fn non_loopback_http_rejected() {
    // Dispatches in the CLI before we reach the async runtime.
    let tmp = tempfile::tempdir().unwrap();
    let ds = tmp.path().join("x.jsonl");
    std::fs::write(&ds, "a\n").unwrap();

    let out = Command::new(shuflr_bin())
        .args(["serve", "--http", "0.0.0.0:0", "--dataset"])
        .arg(format!("x={}", ds.display()))
        .arg("--log-level")
        .arg("warn")
        .stderr(Stdio::piped())
        .stdout(Stdio::piped())
        .output()
        .unwrap();
    assert!(!out.status.success(), "non-loopback must be rejected");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("non-loopback") || stderr.contains("loopback"),
        "stderr: {stderr}"
    );
}

#[test]
fn serve_requires_at_least_one_listener() {
    let out = Command::new(shuflr_bin())
        .args(["serve", "--log-level", "warn"])
        .stderr(Stdio::piped())
        .stdout(Stdio::piped())
        .output()
        .unwrap();
    assert!(!out.status.success());
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("listener"), "stderr: {stderr}");
}
