//! PR-31: TLS + auth integration tests for the `serve` HTTP transport.
//!
//! Each test spawns the real `shuflr` binary with `--features serve`
//! and talks to it via ureq's rustls backend. Self-signed certs are
//! generated in-process via rcgen; the test installs the generated
//! server CA as the only trust root on the client side so we don't
//! have to fiddle with system trust stores.

#![cfg(feature = "serve")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::io::{BufRead, BufReader};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

fn shuflr_bin() -> PathBuf {
    assert_cmd::cargo::cargo_bin("shuflr")
}

fn pick_port() -> u16 {
    let lst = TcpListener::bind("127.0.0.1:0").unwrap();
    let p = lst.local_addr().unwrap().port();
    drop(lst);
    p
}

struct ServerCerts {
    cert_pem_path: PathBuf,
    key_pem_path: PathBuf,
    ca_der: Vec<u8>,
    _guard: tempfile::TempDir,
}

fn gen_self_signed(common_name: &str) -> ServerCerts {
    let tmp = tempfile::tempdir().unwrap();
    let mut params = rcgen::CertificateParams::new(vec![common_name.to_string()]).unwrap();
    params.distinguished_name = rcgen::DistinguishedName::new();
    params
        .distinguished_name
        .push(rcgen::DnType::CommonName, common_name);
    let key_pair = rcgen::KeyPair::generate().unwrap();
    let cert = params.self_signed(&key_pair).unwrap();
    let cert_pem = cert.pem();
    let key_pem = key_pair.serialize_pem();

    let cert_path = tmp.path().join("cert.pem");
    let key_path = tmp.path().join("key.pem");
    std::fs::write(&cert_path, &cert_pem).unwrap();
    std::fs::write(&key_path, &key_pem).unwrap();

    ServerCerts {
        cert_pem_path: cert_path,
        key_pem_path: key_path,
        ca_der: cert.der().to_vec(),
        _guard: tmp,
    }
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

fn spawn_tls_server(dataset_path: &Path, certs: &ServerCerts, auth: Option<&Path>) -> ServeGuard {
    let port = pick_port();
    let mut cmd = Command::new(shuflr_bin());
    cmd.arg("serve")
        .arg("--http")
        .arg(format!("127.0.0.1:{port}"))
        .arg("--log-level")
        .arg("info")
        .arg("--tls-cert")
        .arg(&certs.cert_pem_path)
        .arg("--tls-key")
        .arg(&certs.key_pem_path)
        .arg("--dataset")
        .arg(format!("corpus={}", dataset_path.display()));
    if let Some(tokens) = auth {
        cmd.arg("--auth")
            .arg("bearer")
            .arg("--auth-tokens")
            .arg(tokens);
    }
    let mut child = cmd
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn shuflr serve");
    let stderr = child.stderr.take().unwrap();
    let reader = BufReader::new(stderr);
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        for line in reader.lines().map_while(Result::ok) {
            if line.contains("serve(http) bound") {
                let _ = tx.send(());
                break;
            }
        }
    });
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if Instant::now() > deadline {
            panic!("server never bound");
        }
        match rx.recv_timeout(Duration::from_millis(50)) {
            Ok(()) => break,
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                if let Ok(Some(status)) = child.try_wait() {
                    panic!("server exited before ready: {status:?}");
                }
            }
            Err(_) => panic!("stderr channel disconnected"),
        }
    }
    ServeGuard { child, port }
}

fn ureq_agent_trusting(cert_der: &[u8]) -> ureq::Agent {
    // Bring up a rustls client config that trusts only the given CA.
    let _ = rustls::crypto::ring::default_provider().install_default();
    let mut roots = rustls::RootCertStore::empty();
    roots
        .add(rustls::pki_types::CertificateDer::from(cert_der.to_vec()))
        .unwrap();
    let config = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    ureq::AgentBuilder::new()
        .tls_config(Arc::new(config))
        .build()
}

#[test]
fn https_roundtrip_returns_ndjson() {
    let tmp = tempfile::tempdir().unwrap();
    let ds = tmp.path().join("a.jsonl");
    std::fs::write(&ds, b"a\nb\nc\n").unwrap();
    let certs = gen_self_signed("localhost");
    let g = spawn_tls_server(&ds, &certs, None);

    let agent = ureq_agent_trusting(&certs.ca_der);
    let resp = agent
        .get(&format!(
            "https://localhost:{}/v1/streams/corpus?shuffle=none",
            g.port
        ))
        .call()
        .expect("https GET");
    assert_eq!(resp.status(), 200);
    let body = resp.into_string().unwrap();
    let mut lines: Vec<_> = body.lines().collect();
    lines.sort();
    assert_eq!(lines, vec!["a", "b", "c"]);
}

#[test]
fn bearer_auth_allows_known_token() {
    let tmp = tempfile::tempdir().unwrap();
    let ds = tmp.path().join("d.jsonl");
    std::fs::write(&ds, b"x\ny\n").unwrap();
    let tokens = tmp.path().join("tokens.txt");
    std::fs::write(&tokens, b"goodsecret\n").unwrap();
    let certs = gen_self_signed("localhost");
    let g = spawn_tls_server(&ds, &certs, Some(&tokens));

    let agent = ureq_agent_trusting(&certs.ca_der);
    let url = format!(
        "https://localhost:{}/v1/streams/corpus?shuffle=none",
        g.port
    );

    // Missing token → 401.
    match agent.get(&url).call() {
        Err(ureq::Error::Status(401, _)) => {}
        other => panic!("expected 401 on missing token, got {other:?}"),
    }

    // Wrong token → 401.
    match agent
        .get(&url)
        .set("Authorization", "Bearer notmytoken")
        .call()
    {
        Err(ureq::Error::Status(401, _)) => {}
        other => panic!("expected 401 on wrong token, got {other:?}"),
    }

    // Correct token → 200 + records.
    let resp = agent
        .get(&url)
        .set("Authorization", "Bearer goodsecret")
        .call()
        .expect("auth'd GET");
    assert_eq!(resp.status(), 200);
    let body = resp.into_string().unwrap();
    assert_eq!(body.lines().count(), 2);
}

#[test]
fn health_is_unauthed_even_with_bearer_policy() {
    let tmp = tempfile::tempdir().unwrap();
    let ds = tmp.path().join("d.jsonl");
    std::fs::write(&ds, b"x\n").unwrap();
    let tokens = tmp.path().join("tokens.txt");
    std::fs::write(&tokens, b"required\n").unwrap();
    let certs = gen_self_signed("localhost");
    let g = spawn_tls_server(&ds, &certs, Some(&tokens));

    let agent = ureq_agent_trusting(&certs.ca_der);
    let resp = agent
        .get(&format!("https://localhost:{}/v1/health", g.port))
        .call()
        .expect("health GET");
    assert_eq!(resp.status(), 200);
    assert!(resp.into_string().unwrap().contains("SERVING"));
}

#[test]
fn non_loopback_without_tls_needs_insecure_public_flag() {
    let tmp = tempfile::tempdir().unwrap();
    let ds = tmp.path().join("x.jsonl");
    std::fs::write(&ds, b"x\n").unwrap();

    let out = Command::new(shuflr_bin())
        .args(["serve", "--http", "0.0.0.0:0", "--bind-public", "--dataset"])
        .arg(format!("x={}", ds.display()))
        .arg("--log-level")
        .arg("warn")
        .output()
        .unwrap();
    assert!(!out.status.success(), "expected failure");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("insecure-public") || stderr.contains("TLS"),
        "stderr must describe the gate:\n{stderr}"
    );
}

#[test]
fn bearer_without_tokens_file_is_startup_error() {
    let tmp = tempfile::tempdir().unwrap();
    let ds = tmp.path().join("x.jsonl");
    std::fs::write(&ds, b"x\n").unwrap();

    let out = Command::new(shuflr_bin())
        .args([
            "serve",
            "--http",
            "127.0.0.1:0",
            "--auth",
            "bearer",
            "--dataset",
        ])
        .arg(format!("x={}", ds.display()))
        .arg("--log-level")
        .arg("warn")
        .output()
        .unwrap();
    assert!(!out.status.success());
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("auth-tokens") || stderr.contains("bearer"),
        "stderr: {stderr}"
    );
}

#[test]
fn mtls_without_client_ca_is_startup_error() {
    let tmp = tempfile::tempdir().unwrap();
    let ds = tmp.path().join("x.jsonl");
    std::fs::write(&ds, b"x\n").unwrap();
    let certs = gen_self_signed("localhost");

    let out = Command::new(shuflr_bin())
        .args(["serve", "--http", "127.0.0.1:0"])
        .arg("--tls-cert")
        .arg(&certs.cert_pem_path)
        .arg("--tls-key")
        .arg(&certs.key_pem_path)
        .args(["--auth", "mtls", "--dataset"])
        .arg(format!("x={}", ds.display()))
        .arg("--log-level")
        .arg("warn")
        .output()
        .unwrap();
    assert!(!out.status.success());
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("client-ca") || stderr.contains("mtls"),
        "stderr: {stderr}"
    );
}

#[test]
fn tls_cert_without_key_is_startup_error() {
    let tmp = tempfile::tempdir().unwrap();
    let ds = tmp.path().join("x.jsonl");
    std::fs::write(&ds, b"x\n").unwrap();
    let certs = gen_self_signed("localhost");

    let out = Command::new(shuflr_bin())
        .args(["serve", "--http", "127.0.0.1:0"])
        .arg("--tls-cert")
        .arg(&certs.cert_pem_path)
        .args(["--dataset"])
        .arg(format!("x={}", ds.display()))
        .arg("--log-level")
        .arg("warn")
        .output()
        .unwrap();
    assert!(!out.status.success());
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("tls-key") || stderr.contains("together"),
        "stderr: {stderr}"
    );
}
