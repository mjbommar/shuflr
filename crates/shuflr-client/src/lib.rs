//! Rust core for the `shuflr-client` Python package.
//!
//! Transports as of PR-34b:
//!
//!   http://host:port/v1/streams/{id}    — HTTP/1.1 NDJSON (ureq)
//!   https:// …                          — same, TLS (ureq's TLS feature)
//!   shuflr://host:port/{dataset_id}     — shuflr-wire/1 over plain TCP
//!
//! `shuflrs://` (wire + TLS) and `shuflr+unix://` (wire + UDS) parse
//! successfully for forward compat but currently raise
//! NotImplementedError from `__iter__` — follow-up PRs add rustls and
//! AF_UNIX support.
//!
//! Everything is blocking — we deliberately avoid tokio in the Python
//! wheel. PyTorch's multiprocessing DataLoader fork-spawns workers,
//! and carrying an async runtime across fork is a nightmare.

#![allow(clippy::unwrap_used)]

use std::io::{BufRead, BufReader, Read, Write};
use std::net::ToSocketAddrs;
use std::sync::Mutex;
use std::time::Duration;

use pyo3::exceptions::{PyIOError, PyNotImplementedError, PyStopIteration, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyBytes;

/// Supported URL schemes as of PR-34a.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Transport {
    Http,
    Https,
    /// `shuflr://` — custom binary (PR-36).
    WireTcp,
    /// `shuflrs://` — wire + TLS (PR-36).
    WireTls,
    /// `shuflr+unix://` — wire over UDS (PR-36).
    WireUnix,
}

fn transport_from_scheme(scheme: &str) -> Option<Transport> {
    match scheme {
        "http" => Some(Transport::Http),
        "https" => Some(Transport::Https),
        "shuflr" => Some(Transport::WireTcp),
        "shuflrs" => Some(Transport::WireTls),
        "shuflr+unix" => Some(Transport::WireUnix),
        _ => None,
    }
}

/// Parsed connection target. Produced from the URL; passed to the
/// appropriate transport's `open` function.
#[derive(Debug, Clone)]
struct Target {
    transport: Transport,
    /// For HTTP: the full URL to GET /v1/streams/{id} against.
    /// For wire: the scheme-less host:port + dataset_id.
    raw: String,
    dataset_id: String,
}

fn parse_target(url_str: &str) -> PyResult<Target> {
    let parsed = url::Url::parse(url_str)
        .map_err(|e| PyValueError::new_err(format!("invalid URL '{url_str}': {e}")))?;
    let transport = transport_from_scheme(parsed.scheme()).ok_or_else(|| {
        PyValueError::new_err(format!("unsupported URL scheme '{}'", parsed.scheme()))
    })?;

    // Dataset id is the last non-empty path segment.
    let dataset_id = parsed
        .path_segments()
        .and_then(|mut it| it.next_back())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            PyValueError::new_err(format!(
                "URL '{url_str}' has no dataset_id (expected .../{{dataset_id}})"
            ))
        })?
        .to_string();

    Ok(Target {
        transport,
        raw: url_str.to_string(),
        dataset_id,
    })
}

/// Iterator over records. Each variant handles one transport's
/// framing / decoding; the Python-facing `Dataset::__next__` dispatches
/// on the active variant.
enum Stream {
    Http(HttpStream),
    Wire(WireStream),
}

struct HttpStream {
    reader: Mutex<BufReader<Box<dyn Read + Send>>>,
    exhausted: bool,
}

impl HttpStream {
    fn next_line(&mut self) -> Option<PyResult<Vec<u8>>> {
        if self.exhausted {
            return None;
        }
        let mut line = Vec::new();
        let n = {
            let mut guard = self.reader.lock().ok()?;
            match guard.read_until(b'\n', &mut line) {
                Ok(n) => n,
                Err(e) => {
                    self.exhausted = true;
                    return Some(Err(PyIOError::new_err(format!("read error: {e}"))));
                }
            }
        };
        if n == 0 {
            self.exhausted = true;
            return None;
        }
        if line.ends_with(b"\n") {
            line.pop();
            if line.ends_with(b"\r") {
                line.pop();
            }
        }
        // Empty trailing newline at the end of a stream is benign; skip it.
        if line.is_empty() {
            return self.next_line();
        }
        // Server sentinel: surface as an error for the Python caller.
        if line.starts_with(b"{\"_shuflr_error\":") {
            self.exhausted = true;
            let msg = String::from_utf8_lossy(&line).into_owned();
            return Some(Err(PyIOError::new_err(format!("server error: {msg}"))));
        }
        Some(Ok(line))
    }
}

/// Blocking `shuflr-wire/1` client state. Owns the TCP socket, the
/// decoder buffer, and a queue of already-decoded records waiting to
/// be handed to Python.
struct WireStream {
    /// Underlying connection. Boxed so we can switch between plain
    /// TCP (and, later, TLS) without leaking that shape through the
    /// enum.
    conn: Box<dyn ReadWrite + Send>,
    decoder: shuflr_wire::Decoder,
    pending: std::collections::VecDeque<Vec<u8>>,
    scratch: Vec<u8>,
    exhausted: bool,
}

trait ReadWrite: Read + std::io::Write {}
impl<T: Read + std::io::Write> ReadWrite for T {}

impl WireStream {
    fn next_record(&mut self) -> Option<PyResult<Vec<u8>>> {
        loop {
            if let Some(rec) = self.pending.pop_front() {
                return Some(Ok(rec));
            }
            if self.exhausted {
                return None;
            }
            // Drain any already-buffered frames first.
            match self.decoder.try_next() {
                Ok(Some(msg)) => {
                    if let Some(err) = self.handle_server_message(msg) {
                        return Some(Err(err));
                    }
                    continue;
                }
                Ok(None) => {}
                Err(e) => {
                    self.exhausted = true;
                    return Some(Err(PyIOError::new_err(format!("wire decode: {e}"))));
                }
            }
            // Need more bytes.
            match self.conn.read(&mut self.scratch) {
                Ok(0) => {
                    self.exhausted = true;
                    if self.pending.is_empty() {
                        return None;
                    }
                    continue;
                }
                Ok(n) => self.decoder.feed(&self.scratch[..n]),
                Err(e) => {
                    self.exhausted = true;
                    return Some(Err(PyIOError::new_err(format!("wire read: {e}"))));
                }
            }
        }
    }

    /// Handle one decoded server-side message. Returns `Some(err)` if
    /// the caller should surface a Python error; `None` to keep
    /// looping.
    fn handle_server_message(&mut self, msg: shuflr_wire::Message) -> Option<pyo3::PyErr> {
        use shuflr_wire::Message;
        let kind = msg.kind();
        match msg {
            Message::PlainBatch(batch) => {
                self.pending.extend(batch.records);
                None
            }
            Message::RawFrame {
                frame_id,
                perm_seed,
                zstd_bytes,
            } => match decompress_and_shuffle(&zstd_bytes, perm_seed) {
                Ok(records) => {
                    self.pending.extend(records);
                    None
                }
                Err(e) => {
                    self.exhausted = true;
                    Some(PyIOError::new_err(format!(
                        "raw-frame decode (frame_id={frame_id}): {e}"
                    )))
                }
            },
            Message::EpochBoundary { .. } | Message::Heartbeat { .. } => None,
            Message::StreamClosed { .. } => {
                self.exhausted = true;
                None
            }
            Message::StreamError {
                code,
                fatal,
                detail,
            } => {
                self.exhausted = true;
                let detail = String::from_utf8_lossy(&detail).into_owned();
                Some(PyIOError::new_err(format!(
                    "server StreamError code={code:?} fatal={fatal} detail={detail}"
                )))
            }
            // The server shouldn't send these to a client; treat as a
            // protocol violation but drain cleanly rather than panic.
            _ => {
                self.exhausted = true;
                Some(PyIOError::new_err(format!(
                    "unexpected server message kind={kind:?}"
                )))
            }
        }
    }
}

/// `shuflr_client.Dataset(url, ...)` — see module docstring.
#[pyclass(module = "shuflr_client._shuflr_client_native", unsendable)]
struct Dataset {
    target: Target,
    // Populated on __iter__; None before first iteration.
    stream: Option<Stream>,
    // Query-string settings.
    seed: u64,
    shuffle: String,
    sample: Option<u64>,
    rank: Option<u32>,
    world_size: Option<u32>,
    timeout_secs: f64,
}

#[pymethods]
impl Dataset {
    #[new]
    #[pyo3(signature = (
        url,
        *,
        seed = 0,
        shuffle = "chunk-shuffled".to_string(),
        sample = None,
        rank = None,
        world_size = None,
        timeout = 30.0,
    ))]
    fn new(
        url: String,
        seed: u64,
        shuffle: String,
        sample: Option<u64>,
        rank: Option<u32>,
        world_size: Option<u32>,
        timeout: f64,
    ) -> PyResult<Self> {
        let target = parse_target(&url)?;
        if rank.is_some() && world_size.is_none() {
            return Err(PyValueError::new_err("rank requires world_size to be set"));
        }
        Ok(Self {
            target,
            stream: None,
            seed,
            shuffle,
            sample,
            rank,
            world_size,
            timeout_secs: timeout,
        })
    }

    /// Open the HTTP response (if not already open) and return self.
    fn __iter__(slf: PyRefMut<'_, Self>) -> PyResult<Py<Self>> {
        let py = slf.py();
        let owner: Py<Self> = slf.into();
        {
            let mut b = owner.borrow_mut(py);
            if b.stream.is_none() {
                b.open_stream()?;
            }
        }
        Ok(owner)
    }

    fn __next__(mut slf: PyRefMut<'_, Self>) -> PyResult<Py<PyBytes>> {
        if slf.stream.is_none() {
            slf.open_stream()?;
        }
        let py = slf.py();
        let rec = match &mut slf.stream {
            Some(Stream::Http(s)) => s.next_line(),
            Some(Stream::Wire(s)) => s.next_record(),
            None => None,
        };
        match rec {
            Some(Ok(bytes)) => Ok(PyBytes::new(py, &bytes).into()),
            Some(Err(e)) => Err(e),
            None => Err(PyStopIteration::new_err(())),
        }
    }

    #[getter]
    fn dataset_id(&self) -> &str {
        &self.target.dataset_id
    }

    #[getter]
    fn url(&self) -> &str {
        &self.target.raw
    }

    #[getter]
    fn transport(&self) -> &'static str {
        match self.target.transport {
            Transport::Http => "http",
            Transport::Https => "https",
            Transport::WireTcp => "shuflr-wire/1",
            Transport::WireTls => "shuflr-wire/1+tls",
            Transport::WireUnix => "shuflr-wire/1+unix",
        }
    }
}

impl Dataset {
    fn open_stream(&mut self) -> PyResult<()> {
        match self.target.transport {
            Transport::Http | Transport::Https => {
                let stream = open_http(
                    &self.target.raw,
                    self.seed,
                    &self.shuffle,
                    self.sample,
                    self.rank,
                    self.world_size,
                    self.timeout_secs,
                )?;
                self.stream = Some(Stream::Http(stream));
                Ok(())
            }
            Transport::WireTcp => {
                let stream = open_wire_tcp(
                    &self.target.raw,
                    &self.target.dataset_id,
                    self.seed,
                    &self.shuffle,
                    self.sample,
                    self.rank,
                    self.world_size,
                    self.timeout_secs,
                )?;
                self.stream = Some(Stream::Wire(stream));
                Ok(())
            }
            Transport::WireTls | Transport::WireUnix => Err(PyNotImplementedError::new_err(
                "shuflrs:// and shuflr+unix:// ship in a follow-up; use shuflr:// for now",
            )),
        }
    }
}

fn open_http(
    base: &str,
    seed: u64,
    shuffle: &str,
    sample: Option<u64>,
    rank: Option<u32>,
    world_size: Option<u32>,
    timeout_secs: f64,
) -> PyResult<HttpStream> {
    let parsed = url::Url::parse(base)
        .map_err(|e| PyValueError::new_err(format!("invalid URL '{base}': {e}")))?;
    let dataset_id = parsed
        .path_segments()
        .and_then(|mut it| it.next_back())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| PyValueError::new_err("URL has no dataset_id"))?;

    let mut stream_url = parsed.clone();
    stream_url.set_path(&format!("/v1/streams/{dataset_id}"));
    {
        let mut q = stream_url.query_pairs_mut();
        q.clear();
        if seed != 0 {
            q.append_pair("seed", &seed.to_string());
        }
        q.append_pair("shuffle", shuffle);
        if let Some(n) = sample {
            q.append_pair("sample", &n.to_string());
        }
        if let (Some(r), Some(w)) = (rank, world_size) {
            q.append_pair("rank", &r.to_string());
            q.append_pair("world_size", &w.to_string());
        }
    }

    let agent = ureq::AgentBuilder::new()
        .timeout_connect(Duration::from_secs_f64(timeout_secs))
        // No per-read timeout: we want to hold the stream open for
        // long-running shuffles. If the user wants a deadline they
        // can wrap with signal.alarm / threading.Timer.
        .build();
    let resp = agent
        .get(stream_url.as_str())
        .call()
        .map_err(|e| PyIOError::new_err(format!("HTTP GET {stream_url}: {e}")))?;

    if resp.status() != 200 {
        return Err(PyIOError::new_err(format!(
            "HTTP {} from {stream_url}",
            resp.status()
        )));
    }
    let reader: Box<dyn Read + Send> = Box::new(resp.into_reader());
    Ok(HttpStream {
        reader: Mutex::new(BufReader::with_capacity(256 * 1024, reader)),
        exhausted: false,
    })
}

/// OpenStream payload — mirrors the server's `serve::wire::OpenStreamJson`.
/// Kept in sync by shared field names; PR-35 will swap both for proto.
#[derive(serde::Serialize)]
struct OpenStreamJson<'a> {
    dataset_id: &'a str,
    seed: u64,
    shuffle: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    sample: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    rank: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    world_size: Option<u32>,
    max_batch_records: u32,
}

/// Open a `shuflr://host:port/{dataset}` connection: plain TCP, no
/// auth, no TLS. `shuflrs://` (TLS) and `shuflr+unix://` (UDS) come
/// in follow-up PRs — the URL parser already accepts them but
/// `open_stream` rejects them with `NotImplementedError`.
#[allow(clippy::too_many_arguments)]
fn open_wire_tcp(
    url_str: &str,
    dataset_id: &str,
    seed: u64,
    shuffle: &str,
    sample: Option<u64>,
    rank: Option<u32>,
    world_size: Option<u32>,
    timeout_secs: f64,
) -> PyResult<WireStream> {
    use shuflr_wire::{
        AuthKind, ClientHello, DecodeOptions, Decoder, HandshakeRole, HandshakeStatus, Message,
        encode,
    };
    use std::net::TcpStream;
    use std::time::Duration;

    let parsed = url::Url::parse(url_str)
        .map_err(|e| PyValueError::new_err(format!("invalid URL '{url_str}': {e}")))?;
    let host = parsed
        .host_str()
        .ok_or_else(|| PyValueError::new_err(format!("URL '{url_str}' has no host")))?;
    let port = parsed
        .port()
        .ok_or_else(|| PyValueError::new_err(format!("URL '{url_str}' has no port")))?;

    // Connect with the requested handshake timeout.
    let connect_timeout = Duration::from_secs_f64(timeout_secs.max(0.1));
    let socket_addrs: Vec<std::net::SocketAddr> = (host, port)
        .to_socket_addrs()
        .map_err(|e| PyIOError::new_err(format!("resolve {host}:{port}: {e}")))?
        .collect();
    let first = socket_addrs
        .first()
        .copied()
        .ok_or_else(|| PyIOError::new_err(format!("no addresses for {host}:{port}")))?;
    let mut tcp = TcpStream::connect_timeout(&first, connect_timeout)
        .map_err(|e| PyIOError::new_err(format!("connect {first}: {e}")))?;
    // Per-read/write timeout is separate; leave it None so a long
    // stream doesn't artificially time out mid-fetch.
    tcp.set_nodelay(true).ok();

    // Build and send the ClientHello.
    let open = OpenStreamJson {
        dataset_id,
        seed,
        shuffle,
        sample,
        rank,
        world_size,
        max_batch_records: 64,
    };
    let open_json = serde_json::to_vec(&open)
        .map_err(|e| PyIOError::new_err(format!("encode OpenStream: {e}")))?;
    let hello = Message::ClientHello(ClientHello {
        capability_flags: 0b111,
        auth_kind: AuthKind::None,
        auth: Vec::new(),
        open_stream: open_json,
    });
    let encoded = encode(&hello);
    tcp.write_all(&encoded)
        .map_err(|e| PyIOError::new_err(format!("wire send hello: {e}")))?;

    // Read the ServerHello.
    let mut decoder = Decoder::new(DecodeOptions {
        role: HandshakeRole::ExpectServerHello,
        ..Default::default()
    });
    let mut scratch = vec![0u8; 64 * 1024];
    let server_hello = loop {
        match decoder.try_next() {
            Ok(Some(Message::ServerHello(h))) => break h,
            Ok(Some(other)) => {
                return Err(PyIOError::new_err(format!(
                    "expected ServerHello first, got {:?}",
                    other.kind()
                )));
            }
            Ok(None) => {}
            Err(e) => {
                return Err(PyIOError::new_err(format!("wire decode hello: {e}")));
            }
        }
        let n = tcp
            .read(&mut scratch)
            .map_err(|e| PyIOError::new_err(format!("wire read hello: {e}")))?;
        if n == 0 {
            return Err(PyIOError::new_err("connection closed before ServerHello"));
        }
        decoder.feed(&scratch[..n]);
    };
    if server_hello.status != HandshakeStatus::Ok {
        let detail = String::from_utf8_lossy(&server_hello.stream_opened).into_owned();
        return Err(PyIOError::new_err(format!(
            "server rejected handshake: {detail}"
        )));
    }

    Ok(WireStream {
        conn: Box::new(tcp),
        decoder,
        pending: std::collections::VecDeque::new(),
        scratch,
        exhausted: false,
    })
}

/// Client-side half of the raw-frame mode (005 §3.5): take a
/// zstd-compressed frame's bytes + the 32-byte ChaCha20 seed, decode
/// and Fisher-Yates-shuffle the records inside, return them in shuffle
/// order. Byte-identical to what the in-process `chunk-shuffled`
/// pipeline emits for the same seed.
fn decompress_and_shuffle(zstd_bytes: &[u8], perm_seed: [u8; 32]) -> Result<Vec<Vec<u8>>, String> {
    use rand::SeedableRng;
    use rand::seq::SliceRandom;

    let bytes = zstd::stream::decode_all(zstd_bytes).map_err(|e| format!("zstd decode: {e}"))?;
    // Split on '\n'; the engine guarantees a trailing newline on
    // every record, so memchr gives us (start_inclusive,
    // end_exclusive) positions cleanly.
    let mut records: Vec<Vec<u8>> = Vec::new();
    let mut start = 0usize;
    for nl in memchr::memchr_iter(b'\n', &bytes) {
        records.push(bytes[start..nl].to_vec());
        start = nl + 1;
    }
    if start < bytes.len() {
        records.push(bytes[start..].to_vec());
    }

    let mut rng = rand_chacha::ChaCha20Rng::from_seed(perm_seed);
    records.shuffle(&mut rng);
    Ok(records)
}

/// Python module entry point. Exposes `Dataset` to the Python side.
#[pymodule]
fn _shuflr_client_native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Dataset>()?;
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}

// Unit tests for URL parsing live on the Python side (`tests/test_http.py`)
// — pyo3's `extension-module` feature skips libpython linking, so Rust
// test binaries inside this crate can't link. Any pure-logic tests that
// don't need pyo3 should move to their own crate if we want Rust-side
// coverage.
