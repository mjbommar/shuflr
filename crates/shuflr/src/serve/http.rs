//! HTTP/1.1 chunked NDJSON transport for `serve` (005 §2.1).
//!
//! PR-30 scope: loopback only, no auth, no TLS. PR-31 adds
//! `--tls-cert/--tls-key`, `--auth=bearer`, and `--insecure-public`
//! for non-loopback binds.
//!
//! Design: one shared sync core (the pipelines in [`crate::pipeline`])
//! and one thin async edge. Every stream is handled on an async
//! request task that spawns a `tokio::task::spawn_blocking` hosting
//! the sync pipeline. The pipeline writes into a `tokio::sync::mpsc`
//! channel via a blocking `io::Write` adapter; the response body is a
//! stream over that channel. Backpressure is TCP-native — if the
//! client stops reading, the socket buffer fills, the mpsc fills, and
//! the blocking `Write::write` call blocks until the client consumes.
//! The `shuflr-wire/1` transport adds explicit credit-based flow
//! control (005 §3.6); HTTP rides TCP's.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use bytes::Bytes;
use http_body_util::{Either, Full, StreamBody};
use hyper::body::Frame;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use tokio_stream::wrappers::ReceiverStream;

use crate::error::{Error, Result};
use crate::framing::OnError;
use crate::serve::auth::{Auth, AuthOutcome};
use crate::serve::catalog::Catalog;

/// Paths to a TLS cert/key pair (and an optional client-CA bundle for
/// mTLS). Parsed and handed to rustls at serve-up time.
#[derive(Debug, Clone)]
pub struct TlsPaths {
    pub cert: PathBuf,
    pub key: PathBuf,
    pub client_ca: Option<PathBuf>,
}

/// Configuration for [`run`]. Built via [`HttpConfig::builder`] so we
/// can enforce the cross-field invariants (`bind_public` gates
/// non-loopback, TLS without `client_ca` is incompatible with
/// `Auth::Mtls`, and so on) in one place.
#[derive(Debug, Clone)]
pub struct HttpConfig {
    pub addr: SocketAddr,
    pub catalog: Catalog,
    pub tls: Option<TlsPaths>,
    pub auth: Auth,
    pub bind_public: bool,
    pub insecure_public: bool,
}

impl HttpConfig {
    /// Simple constructor for tests and loopback no-auth servers.
    pub fn new(addr: SocketAddr, catalog: Catalog) -> Result<Self> {
        Self::builder(addr, catalog).build()
    }

    pub fn builder(addr: SocketAddr, catalog: Catalog) -> HttpConfigBuilder {
        HttpConfigBuilder {
            addr,
            catalog,
            tls: None,
            auth: Auth::None,
            bind_public: false,
            insecure_public: false,
        }
    }
}

pub struct HttpConfigBuilder {
    addr: SocketAddr,
    catalog: Catalog,
    tls: Option<TlsPaths>,
    auth: Auth,
    bind_public: bool,
    insecure_public: bool,
}

impl HttpConfigBuilder {
    pub fn tls(mut self, paths: TlsPaths) -> Self {
        self.tls = Some(paths);
        self
    }
    pub fn auth(mut self, auth: Auth) -> Self {
        self.auth = auth;
        self
    }
    pub fn bind_public(mut self, v: bool) -> Self {
        self.bind_public = v;
        self
    }
    pub fn insecure_public(mut self, v: bool) -> Self {
        self.insecure_public = v;
        self
    }

    pub fn build(self) -> Result<HttpConfig> {
        let is_public = !self.addr.ip().is_loopback();
        if is_public && !self.bind_public {
            return Err(Error::Input(format!(
                "HTTP listener on {} is non-loopback — pass --bind-public \
                 to opt in (005 §4.1)",
                self.addr
            )));
        }
        if is_public && self.tls.is_none() && !self.insecure_public {
            return Err(Error::Input(format!(
                "HTTP listener on {} is non-loopback without TLS — pass \
                 --tls-cert/--tls-key, or --insecure-public to accept \
                 plaintext (005 §4.2)",
                self.addr
            )));
        }
        if matches!(self.auth, Auth::Mtls) {
            let Some(tls) = &self.tls else {
                return Err(Error::Input(
                    "--auth=mtls requires --tls-cert/--tls-key".into(),
                ));
            };
            if tls.client_ca.is_none() {
                return Err(Error::Input("--auth=mtls requires --tls-client-ca".into()));
            }
        }
        Ok(HttpConfig {
            addr: self.addr,
            catalog: self.catalog,
            tls: self.tls,
            auth: self.auth,
            bind_public: self.bind_public,
            insecure_public: self.insecure_public,
        })
    }
}

/// Run the HTTP server until `shutdown` resolves. Returns after the
/// listener has closed and every in-flight response has completed.
pub async fn run(cfg: HttpConfig, shutdown: impl std::future::Future<Output = ()>) -> Result<()> {
    use tokio::net::TcpListener;

    let is_public = !cfg.addr.ip().is_loopback();
    let tls_acceptor = if let Some(tls_paths) = &cfg.tls {
        let server_cfg = crate::serve::tls::build_server_config(
            &tls_paths.cert,
            &tls_paths.key,
            tls_paths.client_ca.as_deref(),
        )?;
        Some(tokio_rustls::TlsAcceptor::from(server_cfg))
    } else {
        None
    };
    let insecure_plaintext_public = is_public && tls_acceptor.is_none();
    if insecure_plaintext_public {
        tracing::warn!(
            addr = %cfg.addr,
            "serve(http): running UNENCRYPTED on public interface; all traffic is plaintext"
        );
    }

    let listener = TcpListener::bind(cfg.addr).await.map_err(Error::Io)?;
    tracing::info!(
        addr = %cfg.addr,
        datasets = cfg.catalog.len(),
        tls = tls_acceptor.is_some(),
        auth = ?cfg.auth,
        "serve(http) bound",
    );

    let catalog = Arc::new(cfg.catalog);
    let auth = Arc::new(cfg.auth);

    // Register a SIGHUP reload handler for bearer-token rotation on unix.
    #[cfg(unix)]
    {
        let auth_for_hup = Arc::clone(&auth);
        tokio::spawn(async move {
            use tokio::signal::unix::{SignalKind, signal};
            let mut s = match signal(SignalKind::hangup()) {
                Ok(s) => s,
                Err(e) => {
                    tracing::warn!(err = %e, "could not install SIGHUP handler");
                    return;
                }
            };
            while s.recv().await.is_some() {
                tracing::info!("SIGHUP received; reloading bearer tokens");
                auth_for_hup.reload_if_bearer();
            }
        });
    }

    tokio::pin!(shutdown);

    loop {
        tokio::select! {
            accept = listener.accept() => {
                let (tcp, peer) = match accept {
                    Ok(p) => p,
                    Err(e) => {
                        tracing::warn!(err = %e, "accept failed; continuing");
                        continue;
                    }
                };
                if insecure_plaintext_public {
                    tracing::warn!(peer = %peer, "serve(http): plaintext connection from public peer");
                }
                let catalog = Arc::clone(&catalog);
                let auth = Arc::clone(&auth);
                let tls_acceptor = tls_acceptor.clone();
                tokio::spawn(async move {
                    match tls_acceptor {
                        Some(acceptor) => match acceptor.accept(tcp).await {
                            Ok(tls_stream) => {
                                serve_one(TokioIo::new(tls_stream), catalog, auth).await;
                            }
                            Err(e) => {
                                tracing::debug!(peer = %peer, err = %e, "TLS handshake failed");
                            }
                        },
                        None => {
                            serve_one(TokioIo::new(tcp), catalog, auth).await;
                        }
                    }
                });
            }
            _ = &mut shutdown => {
                tracing::info!("serve(http) shutdown signal received");
                break;
            }
        }
    }
    Ok(())
}

/// Drive one client connection: hyper HTTP/1.1 with our service.
async fn serve_one<S>(io: TokioIo<S>, catalog: Arc<Catalog>, auth: Arc<Auth>)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let svc = service_fn(move |req| {
        let catalog = Arc::clone(&catalog);
        let auth = Arc::clone(&auth);
        async move { handle(req, catalog, auth).await }
    });
    if let Err(e) = hyper::server::conn::http1::Builder::new()
        .serve_connection(io, svc)
        .await
    {
        tracing::debug!(err = %e, "http: conn closed with error");
    }
}

type BoxBody = Either<
    Full<Bytes>,
    StreamBody<ReceiverStream<std::result::Result<Frame<Bytes>, std::io::Error>>>,
>;

async fn handle(
    req: Request<hyper::body::Incoming>,
    catalog: Arc<Catalog>,
    auth: Arc<Auth>,
) -> std::result::Result<Response<BoxBody>, std::convert::Infallible> {
    let method = req.method().clone();
    let path = req.uri().path().to_string();
    let query = req.uri().query().unwrap_or("").to_string();

    // /v1/health is left unauthenticated — operators / load balancers
    // frequently probe it without credentials. Everything else goes
    // through the configured policy.
    let unauthed_path = path == "/v1/health";

    if !unauthed_path {
        let auth_header = req
            .headers()
            .get(hyper::header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok());
        match auth.verify_http_header(auth_header) {
            AuthOutcome::Ok => {}
            outcome => return Ok(auth_fail_response(outcome)),
        }
    }

    // Route table. Handlers return `Result<Response, HttpError>`; map to
    // Response uniformly below.
    let result: std::result::Result<Response<BoxBody>, HttpError> = match (&method, path.as_str()) {
        (&Method::GET, "/v1/health") => Ok(health_response()),
        (&Method::GET, "/v1/datasets") => Ok(datasets_list_response(&catalog)),
        (&Method::GET, p) if p.starts_with("/v1/datasets/") => {
            let id = &p["/v1/datasets/".len()..];
            dataset_get_response(&catalog, id)
        }
        (&Method::GET, p) if p.starts_with("/v1/streams/") => {
            let id = p["/v1/streams/".len()..].to_string();
            stream_response(&catalog, &id, &query).await
        }
        _ => Err(HttpError::NotFound(path.clone())),
    };

    Ok(match result {
        Ok(resp) => resp,
        Err(e) => e.into_response(),
    })
}

fn auth_fail_response(outcome: AuthOutcome) -> Response<BoxBody> {
    let (status, detail) = match outcome {
        AuthOutcome::Ok => unreachable!("auth_fail_response called with Ok"),
        AuthOutcome::Missing => (StatusCode::UNAUTHORIZED, "missing Authorization header"),
        AuthOutcome::Malformed => (StatusCode::UNAUTHORIZED, "malformed Authorization header"),
        AuthOutcome::Mismatch => (StatusCode::UNAUTHORIZED, "bearer token not accepted"),
        AuthOutcome::Internal => (StatusCode::INTERNAL_SERVER_ERROR, "auth check failed"),
    };
    let body = format!("{{\"error\":\"unauthorized\",\"detail\":\"{}\"}}\n", detail);
    let mut builder = Response::builder()
        .status(status)
        .header("content-type", "application/json; charset=utf-8");
    if status == StatusCode::UNAUTHORIZED {
        builder = builder.header("www-authenticate", "Bearer");
    }
    builder
        .body(Either::Left(Full::new(Bytes::from(body))))
        .unwrap_or_else(|e| {
            tracing::error!(err = %e, "auth_fail_response build failed");
            Response::new(Either::Left(Full::new(Bytes::from("internal\n"))))
        })
}

#[derive(Debug)]
enum HttpError {
    NotFound(String),
    BadQuery(String),
    UnknownDataset(String),
    Internal(String),
}

impl HttpError {
    fn into_response(self) -> Response<BoxBody> {
        let (status, body) = match &self {
            HttpError::NotFound(p) => (
                StatusCode::NOT_FOUND,
                format!("{{\"error\":\"not_found\",\"path\":\"{}\"}}\n", json_esc(p)),
            ),
            HttpError::BadQuery(m) => (
                StatusCode::BAD_REQUEST,
                format!(
                    "{{\"error\":\"bad_query\",\"detail\":\"{}\"}}\n",
                    json_esc(m)
                ),
            ),
            HttpError::UnknownDataset(id) => (
                StatusCode::NOT_FOUND,
                format!(
                    "{{\"error\":\"unknown_dataset\",\"dataset_id\":\"{}\"}}\n",
                    json_esc(id)
                ),
            ),
            HttpError::Internal(m) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!(
                    "{{\"error\":\"internal\",\"detail\":\"{}\"}}\n",
                    json_esc(m)
                ),
            ),
        };
        Response::builder()
            .status(status)
            .header("content-type", "application/json; charset=utf-8")
            .body(Either::Left(Full::new(Bytes::from(body))))
            .unwrap_or_else(|e| {
                // Response::builder() only fails on invalid headers/status;
                // our set is static so this is effectively unreachable.
                tracing::error!(err = %e, "static error response build failed");
                Response::new(Either::Left(Full::new(Bytes::from("internal\n"))))
            })
    }
}

fn json_ok_response(body: String) -> Response<BoxBody> {
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/json; charset=utf-8")
        .body(Either::Left(Full::new(Bytes::from(body))))
        .unwrap_or_else(|e| {
            tracing::error!(err = %e, "static JSON response build failed");
            Response::new(Either::Left(Full::new(Bytes::from("internal\n"))))
        })
}

fn health_response() -> Response<BoxBody> {
    json_ok_response(String::from("{\"status\":\"SERVING\"}\n"))
}

fn datasets_list_response(cat: &Catalog) -> Response<BoxBody> {
    let mut s = String::from("[");
    let mut first = true;
    for entry in cat.iter() {
        if !first {
            s.push(',');
        }
        first = false;
        dataset_json(&mut s, entry);
    }
    s.push(']');
    s.push('\n');
    json_ok_response(s)
}

fn dataset_get_response(
    cat: &Catalog,
    id: &str,
) -> std::result::Result<Response<BoxBody>, HttpError> {
    let entry = cat
        .get(id)
        .ok_or_else(|| HttpError::UnknownDataset(id.to_string()))?;
    let mut s = String::new();
    dataset_json(&mut s, entry);
    s.push('\n');
    Ok(json_ok_response(s))
}

fn dataset_json(s: &mut String, entry: &crate::serve::catalog::DatasetEntry) {
    // Hand-rolled JSON, same style as info --json. The id passes the
    // catalog validator so there's nothing to escape, but we escape it
    // defensively anyway.
    use std::fmt::Write as _;
    let fp_hex: String = entry
        .fingerprint
        .0
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    let _ = write!(
        s,
        "{{\"dataset_id\":\"{}\",\"fingerprint\":\"blake3:{}\"}}",
        json_esc(&entry.id),
        fp_hex
    );
}

async fn stream_response(
    catalog: &Catalog,
    id: &str,
    query: &str,
) -> std::result::Result<Response<BoxBody>, HttpError> {
    let entry = catalog
        .get(id)
        .ok_or_else(|| HttpError::UnknownDataset(id.to_string()))?;
    let params = parse_query(query).map_err(HttpError::BadQuery)?;
    let shuffle = params
        .shuffle
        .unwrap_or_else(|| "chunk-shuffled".to_string());
    let seed = params.seed.unwrap_or(0);
    let sample = params.sample;
    let epochs = params.epochs.unwrap_or(1);
    let rank = params.rank;
    let world_size = params.world_size;
    if rank.is_some() != world_size.is_some() {
        return Err(HttpError::BadQuery(
            "rank and world_size must be specified together".into(),
        ));
    }
    let partition = match (rank, world_size) {
        (Some(r), Some(w)) if w > 1 && r < w => Some((r, w)),
        _ => None,
    };

    let path = entry.path.clone();
    let fp_hex: String = entry
        .fingerprint
        .0
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();

    // Channel carries already-framed body chunks. The sync pipeline
    // writes via the TxWriter adapter (which wraps each write in a
    // Frame::data); the async body iterates the Receiver.
    let (tx, rx) =
        tokio::sync::mpsc::channel::<std::result::Result<Frame<Bytes>, std::io::Error>>(32);

    let shuffle_for_task = shuffle.clone();
    tokio::task::spawn_blocking(move || {
        let tx_for_err = tx.clone();
        let res = run_pipeline(
            &path,
            &shuffle_for_task,
            seed,
            epochs,
            sample,
            partition,
            tx,
        );
        if let Err(e) = res {
            let line = format!(
                "{{\"_shuflr_error\":\"{}\",\"detail\":\"{}\"}}\n",
                error_code(&e),
                json_esc(&e.to_string())
            );
            let _ = tx_for_err.blocking_send(Ok(Frame::data(Bytes::from(line))));
        }
    });

    let rx_stream = ReceiverStream::new(rx);
    let body = StreamBody::new(rx_stream);
    let resp = Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/x-ndjson; charset=utf-8")
        .header("shuflr-effective-seed", seed.to_string())
        .header("shuflr-fingerprint", format!("blake3:{fp_hex}"))
        .header("shuflr-shuffle", &shuffle)
        .body(Either::Right(body))
        .map_err(|e| HttpError::Internal(format!("response build: {e}")))?;
    Ok(resp)
}

/// Blocking sync-side pipeline runner. Writes NDJSON into `tx`.
fn run_pipeline(
    path: &std::path::Path,
    shuffle: &str,
    seed: u64,
    epochs: u32,
    sample: Option<u64>,
    partition: Option<(u32, u32)>,
    tx: tokio::sync::mpsc::Sender<std::result::Result<Frame<Bytes>, std::io::Error>>,
) -> Result<()> {
    let sink = TxWriter { tx };
    let mut bufsink = std::io::BufWriter::with_capacity(256 * 1024, sink);

    let mut epoch = 0u64;
    loop {
        if epochs != 0 && epoch >= u64::from(epochs) {
            break;
        }
        run_pipeline_epoch(path, shuffle, seed, epoch, sample, partition, &mut bufsink)?;
        epoch += 1;
    }
    std::io::Write::flush(&mut bufsink).map_err(Error::Io)?;
    Ok(())
}

fn run_pipeline_epoch(
    path: &std::path::Path,
    shuffle: &str,
    seed: u64,
    epoch: u64,
    sample: Option<u64>,
    partition: Option<(u32, u32)>,
    sink: &mut impl std::io::Write,
) -> Result<()> {
    match shuffle {
        "none" => {
            let input = crate::io::Input::open(path)?;
            let cfg = crate::pipeline::PassthroughConfig {
                max_line: 16 * 1024 * 1024,
                on_error: OnError::Skip,
                sample,
                ensure_trailing_newline: true,
                partition,
            };
            crate::pipeline::passthrough(input, sink, &cfg)?;
            Ok(())
        }
        "buffer" => {
            let input = crate::io::Input::open(path)?;
            let cfg = crate::pipeline::BufferConfig {
                buffer_size: 100_000,
                seed: epoch_seed(seed, epoch),
                max_line: 16 * 1024 * 1024,
                on_error: OnError::Skip,
                sample,
                ensure_trailing_newline: true,
                partition,
            };
            crate::pipeline::buffer(input, sink, &cfg)?;
            Ok(())
        }
        "reservoir" => {
            let input = crate::io::Input::open(path)?;
            let cfg = crate::pipeline::ReservoirConfig {
                k: 10_000,
                seed: epoch_seed(seed, epoch),
                max_line: 16 * 1024 * 1024,
                on_error: OnError::Skip,
                ensure_trailing_newline: true,
                partition,
            };
            crate::pipeline::reservoir(input, sink, &cfg)?;
            Ok(())
        }
        #[cfg(feature = "zstd")]
        "chunk-shuffled" => {
            let reader = crate::io::zstd_seekable::SeekableReader::open(path)?;
            let cfg = crate::pipeline::ChunkShuffledConfig {
                seed,
                epoch,
                max_line: 16 * 1024 * 1024,
                on_error: OnError::Skip,
                sample,
                ensure_trailing_newline: true,
                partition,
                emit_threads: 1,
                emit_prefetch: 8,
            };
            crate::pipeline::chunk_shuffled(reader, sink, &cfg)?;
            Ok(())
        }
        #[cfg(feature = "zstd")]
        "index-perm" => {
            let cfg = crate::pipeline::IndexPermZstdConfig {
                seed,
                epoch,
                sample,
                ensure_trailing_newline: true,
                cache_capacity: crate::pipeline::index_perm_zstd::DEFAULT_CACHE_CAPACITY,
                partition,
                on_build_frame: None,
                build_threads: 0,
                emit_threads: 1,
                emit_prefetch: 32,
            };
            crate::pipeline::index_perm_zstd(path, sink, &cfg)?;
            Ok(())
        }
        other => Err(Error::Input(format!(
            "shuffle mode '{other}' not supported on HTTP transport"
        ))),
    }
}

fn epoch_seed(seed: u64, epoch: u64) -> u64 {
    if epoch == 0 {
        return seed;
    }
    let key = crate::seed::Seed::new(seed).epoch(epoch);
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&key[..8]);
    u64::from_le_bytes(bytes)
}

/// `io::Write` adapter that sends every non-empty `write` as one
/// `Bytes` on the async channel. Deliberately blocks on send — this
/// runs inside `tokio::task::spawn_blocking`, and blocking is the
/// mechanism that propagates TCP backpressure all the way through to
/// the sync core.
struct TxWriter {
    tx: tokio::sync::mpsc::Sender<std::result::Result<Frame<Bytes>, std::io::Error>>,
}

impl std::io::Write for TxWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        let frame = Frame::data(Bytes::copy_from_slice(buf));
        self.tx
            .blocking_send(Ok(frame))
            .map_err(|e| std::io::Error::other(format!("send to body channel failed: {e}")))?;
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn error_code(e: &Error) -> &'static str {
    match e {
        Error::NotFound { .. } => "not_found",
        Error::PermissionDenied { .. } => "perm_denied",
        Error::OversizedRecord { .. } => "oversized_record",
        Error::InputChanged { .. } => "input_changed",
        Error::Input(_) => "input",
        Error::Io(_) => "io",
        Error::CompressedInputUnsupported { .. } => "compressed_unsupported",
    }
}

#[derive(Default, Debug)]
struct StreamParams {
    seed: Option<u64>,
    shuffle: Option<String>,
    sample: Option<u64>,
    epochs: Option<u32>,
    rank: Option<u32>,
    world_size: Option<u32>,
}

/// Minimal application/x-www-form-urlencoded parser that handles the
/// subset of queries the HTTP transport cares about. Unknown keys are
/// ignored — future additions (compression mode, batch size hints) can
/// be added without breaking existing clients.
fn parse_query(q: &str) -> std::result::Result<StreamParams, String> {
    let mut out = StreamParams::default();
    if q.is_empty() {
        return Ok(out);
    }
    for pair in q.split('&') {
        if pair.is_empty() {
            continue;
        }
        let Some((key, val)) = pair.split_once('=') else {
            continue;
        };
        let val = percent_decode(val);
        match key {
            "seed" => out.seed = Some(val.parse().map_err(|e| format!("bad seed: {e}"))?),
            "shuffle" => out.shuffle = Some(val),
            "sample" => out.sample = Some(val.parse().map_err(|e| format!("bad sample: {e}"))?),
            "epochs" => out.epochs = Some(val.parse().map_err(|e| format!("bad epochs: {e}"))?),
            "rank" => out.rank = Some(val.parse().map_err(|e| format!("bad rank: {e}"))?),
            "world_size" => {
                out.world_size = Some(val.parse().map_err(|e| format!("bad world_size: {e}"))?)
            }
            _ => {}
        }
    }
    Ok(out)
}

/// Minimal %-decoder (RFC 3986 §2.1). Missing a byte for a `%` just
/// passes the `%` through — we're lenient because the query-string
/// contract is narrow.
fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        if b == b'%' && i + 2 < bytes.len() {
            let hi = hex_val(bytes[i + 1]);
            let lo = hex_val(bytes[i + 2]);
            if let (Some(hi), Some(lo)) = (hi, lo) {
                out.push((hi << 4) | lo);
                i += 3;
                continue;
            }
        }
        if b == b'+' {
            out.push(b' ');
        } else {
            out.push(b);
        }
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

fn json_esc(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => {
                use std::fmt::Write as _;
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            c => out.push(c),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_basic_query() {
        let p = parse_query("seed=42&shuffle=index-perm&sample=100").unwrap();
        assert_eq!(p.seed, Some(42));
        assert_eq!(p.shuffle.as_deref(), Some("index-perm"));
        assert_eq!(p.sample, Some(100));
    }

    #[test]
    fn parses_partition() {
        let p = parse_query("rank=1&world_size=4").unwrap();
        assert_eq!(p.rank, Some(1));
        assert_eq!(p.world_size, Some(4));
    }

    #[test]
    fn empty_query_is_ok() {
        let p = parse_query("").unwrap();
        assert!(p.seed.is_none() && p.shuffle.is_none());
    }

    #[test]
    fn unknown_keys_ignored() {
        let p = parse_query("seed=7&unknown=xyz&shuffle=none").unwrap();
        assert_eq!(p.seed, Some(7));
        assert_eq!(p.shuffle.as_deref(), Some("none"));
    }

    #[test]
    fn bad_number_is_error() {
        assert!(parse_query("seed=notanumber").is_err());
    }

    #[test]
    fn percent_decode_works() {
        assert_eq!(percent_decode("a%20b"), "a b");
        assert_eq!(percent_decode("x+y"), "x y");
        // A non-ASCII byte sequence still %-decodes correctly when it
        // produces valid UTF-8 (here %c3%bf = U+00FF in UTF-8).
        assert_eq!(percent_decode("%c3%bf"), "\u{ff}");
        // An illegal-UTF-8 byte is rendered via the Unicode replacement
        // character — we choose lossy over fallible here because query
        // params are a narrow surface; bad percent-encoding shouldn't
        // kill the request.
        assert_eq!(percent_decode("%ff"), "\u{fffd}");
        assert_eq!(percent_decode("raw"), "raw");
    }

    #[test]
    fn http_config_rejects_public_bind() {
        use std::net::{IpAddr, Ipv4Addr};
        let cat = Catalog::from_args::<&str>(&[]).unwrap();
        let cfg = HttpConfig::new(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 9000),
            cat,
        );
        assert!(cfg.is_err(), "non-loopback bind must be rejected in PR-30");
    }

    #[test]
    fn http_config_accepts_loopback() {
        use std::net::{IpAddr, Ipv4Addr};
        let cat = Catalog::from_args::<&str>(&[]).unwrap();
        let cfg = HttpConfig::new(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0), cat);
        assert!(cfg.is_ok());
    }
}
