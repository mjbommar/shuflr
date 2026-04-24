//! `shuflr-wire/1` transport for `serve` (005 §2.2, §3).
//!
//! PR-33 scope: TCP listener (+ optional rustls TLS from PR-31),
//! handshake (ClientHello → ServerHello), bearer/mTLS auth, and
//! `plain-batch` streaming for all five shuffle modes. Deferred
//! follow-ups (tracked in 005 §8):
//!   - Credit-based flow control (§3.6) — PR-33a
//!   - `raw-frame` passthrough for chunk-shuffled (§3.5) — PR-33b
//!   - `zstd-batch` mode — PR-33c
//!   - UDS listeners — PR-33d
//!   - Heartbeats / Pong — PR-36
//!
//! Until then we negotiate `plain-batch` unconditionally. Record
//! payloads are already tiny relative to HTTP framing so this is
//! functionally equivalent to PR-30's NDJSON.
//!
//! Open-stream config on the wire is serialized as JSON inside
//! `ClientHello.open_stream` for PR-33. When PR-35 brings in prost
//! the codec's opaque-bytes contract makes this swap local.

use std::net::SocketAddr;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use shuflr_wire::{
    BatchPayload, ChosenMode, DecodeOptions, Decoder, HandshakeRole, HandshakeStatus, Message,
    ServerHello, StreamErrorCode, encode_into,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::mpsc;

use crate::error::{Error, Result};
use crate::framing::OnError;
use crate::serve::auth::{Auth, AuthOutcome};
use crate::serve::catalog::Catalog;
use crate::serve::http::TlsPaths;

/// Default initial credit in bytes (005 §3.6). Not enforced in PR-33;
/// advertised in ServerHello so PR-33a can drop in flow control
/// without a protocol change.
const DEFAULT_INITIAL_CREDIT: u64 = 16 * 1024 * 1024;

/// Wire-format server configuration.
#[derive(Debug, Clone)]
pub struct WireConfig {
    pub addr: SocketAddr,
    pub catalog: Catalog,
    pub tls: Option<TlsPaths>,
    pub auth: Auth,
    pub bind_public: bool,
    pub insecure_public: bool,
}

impl WireConfig {
    pub fn builder(addr: SocketAddr, catalog: Catalog) -> WireConfigBuilder {
        WireConfigBuilder {
            addr,
            catalog,
            tls: None,
            auth: Auth::None,
            bind_public: false,
            insecure_public: false,
        }
    }
}

pub struct WireConfigBuilder {
    addr: SocketAddr,
    catalog: Catalog,
    tls: Option<TlsPaths>,
    auth: Auth,
    bind_public: bool,
    insecure_public: bool,
}

impl WireConfigBuilder {
    pub fn tls(mut self, p: TlsPaths) -> Self {
        self.tls = Some(p);
        self
    }
    pub fn auth(mut self, a: Auth) -> Self {
        self.auth = a;
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
    pub fn build(self) -> Result<WireConfig> {
        // Same policy as the HTTP builder (005 §4.1): non-loopback
        // needs --bind-public, and needs TLS OR --insecure-public.
        let is_public = !self.addr.ip().is_loopback();
        if is_public && !self.bind_public {
            return Err(Error::Input(format!(
                "wire listener on {} is non-loopback — pass --bind-public \
                 to opt in (005 §4.1)",
                self.addr
            )));
        }
        if is_public && self.tls.is_none() && !self.insecure_public {
            return Err(Error::Input(format!(
                "wire listener on {} is non-loopback without TLS — pass \
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
        Ok(WireConfig {
            addr: self.addr,
            catalog: self.catalog,
            tls: self.tls,
            auth: self.auth,
            bind_public: self.bind_public,
            insecure_public: self.insecure_public,
        })
    }
}

/// JSON schema for the `OpenStream` payload carried inside
/// `ClientHello.open_stream`. 005 §7.1 specifies the canonical
/// protobuf; PR-35 will swap this for prost-decoded proto. The codec
/// treats these bytes as opaque so the swap is transport-local.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpenStreamJson {
    pub dataset_id: String,
    #[serde(default)]
    pub seed: u64,
    #[serde(default = "default_shuffle")]
    pub shuffle: String,
    #[serde(default)]
    pub epochs: Option<u32>,
    #[serde(default)]
    pub sample: Option<u64>,
    #[serde(default)]
    pub rank: Option<u32>,
    #[serde(default)]
    pub world_size: Option<u32>,
    /// Wire-batch size; 0 means "server default".
    #[serde(default)]
    pub max_batch_records: u32,
    #[serde(default)]
    pub client_id: Option<String>,
}

fn default_shuffle() -> String {
    "chunk-shuffled".to_string()
}

/// JSON payload returned in `ServerHello.stream_opened`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamOpenedJson {
    pub dataset_id: String,
    pub fingerprint: String,
    pub effective_seed: u64,
    pub effective_shuffle: String,
    pub effective_batch_records: u32,
}

/// Start the wire listener. Behaves like [`super::http::run`].
pub async fn run(cfg: WireConfig, shutdown: impl std::future::Future<Output = ()>) -> Result<()> {
    let is_public = !cfg.addr.ip().is_loopback();
    let tls_acceptor = match &cfg.tls {
        Some(paths) => Some(tokio_rustls::TlsAcceptor::from(
            super::tls::build_server_config(&paths.cert, &paths.key, paths.client_ca.as_deref())?,
        )),
        None => None,
    };
    let insecure = is_public && tls_acceptor.is_none();
    if insecure {
        tracing::warn!(addr = %cfg.addr, "serve(wire): running UNENCRYPTED on public interface");
    }

    let listener = TcpListener::bind(cfg.addr).await.map_err(Error::Io)?;
    tracing::info!(
        addr = %cfg.addr,
        datasets = cfg.catalog.len(),
        tls = tls_acceptor.is_some(),
        auth = ?cfg.auth,
        "serve(wire) bound",
    );

    let catalog = Arc::new(cfg.catalog);
    let auth = Arc::new(cfg.auth);

    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            accept = listener.accept() => {
                let (tcp, peer) = match accept {
                    Ok(p) => p,
                    Err(e) => {
                        tracing::warn!(err = %e, "wire: accept failed");
                        continue;
                    }
                };
                if insecure {
                    tracing::warn!(peer = %peer, "serve(wire): plaintext from public peer");
                }
                let catalog = Arc::clone(&catalog);
                let auth = Arc::clone(&auth);
                let acceptor = tls_acceptor.clone();
                tokio::spawn(async move {
                    let result = match acceptor {
                        Some(a) => match a.accept(tcp).await {
                            Ok(s) => handle_conn(s, catalog, auth).await,
                            Err(e) => {
                                tracing::debug!(peer = %peer, err = %e, "wire: TLS handshake failed");
                                return;
                            }
                        },
                        None => handle_conn(tcp, catalog, auth).await,
                    };
                    if let Err(e) = result {
                        tracing::debug!(peer = %peer, err = %e, "wire: connection closed with error");
                    }
                });
            }
            _ = &mut shutdown => {
                tracing::info!("serve(wire) shutdown signal received");
                break;
            }
        }
    }
    Ok(())
}

/// Drive one accepted connection end-to-end.
async fn handle_conn<S>(stream: S, catalog: Arc<Catalog>, auth: Arc<Auth>) -> Result<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let (mut rd, mut wr) = tokio::io::split(stream);
    let mut decoder = Decoder::new(DecodeOptions {
        role: HandshakeRole::ExpectClientHello,
        ..Default::default()
    });
    let mut scratch = vec![0u8; 64 * 1024];
    let mut tx_buf = Vec::with_capacity(64 * 1024);

    // ---- Phase 1: read ClientHello.
    let client_hello = match read_one(&mut rd, &mut decoder, &mut scratch).await? {
        Some(Message::ClientHello(c)) => c,
        Some(other) => {
            return handshake_error(
                &mut wr,
                &mut tx_buf,
                format!("expected ClientHello, got {:?}", other.kind()),
            )
            .await;
        }
        None => {
            // Peer hung up before the handshake. Not our error.
            return Ok(());
        }
    };

    // ---- Phase 2: auth.
    match &*auth {
        Auth::None => {}
        Auth::Mtls => {
            // Rustls already enforced a client cert; nothing further.
        }
        Auth::Bearer(_) => {
            // ClientHello.auth is the token bytes; compare via the Auth struct's header-verify
            // path. We synthesize the "Bearer " prefix to re-use verify_http_header's
            // constant-time compare.
            let as_header = format!("Bearer {}", String::from_utf8_lossy(&client_hello.auth));
            match auth.verify_http_header(Some(&as_header)) {
                AuthOutcome::Ok => {}
                other => {
                    return handshake_error(
                        &mut wr,
                        &mut tx_buf,
                        format!("auth rejected: {:?}", other),
                    )
                    .await;
                }
            }
        }
    }

    // ---- Phase 3: parse OpenStream JSON.
    let open: OpenStreamJson = match serde_json::from_slice(&client_hello.open_stream) {
        Ok(v) => v,
        Err(e) => {
            return handshake_error(&mut wr, &mut tx_buf, format!("open_stream parse: {e}")).await;
        }
    };

    // ---- Phase 4: look up the dataset.
    let Some(entry) = catalog.get(&open.dataset_id) else {
        return handshake_error(
            &mut wr,
            &mut tx_buf,
            format!("unknown dataset: {}", open.dataset_id),
        )
        .await;
    };

    // ---- Phase 5: negotiate the payload mode (005 §3.5).
    //
    // The RawFrame compression-passthrough mode is only safe for
    // chunk-shuffled on a seekable-zstd input. We need:
    //   (a) the dataset is a seekable-zstd file (reader opens OK)
    //   (b) shuffle=chunk-shuffled
    //   (c) client advertised `capability_flags & 0b01` (understands raw-frame)
    //   (d) no --rank/world_size (rank partitioning is record-level,
    //       but raw-frame emits at frame granularity — a future PR
    //       can do frame-level rank partitioning)
    //
    // --sample IS allowed. Raw-frame ships whole frames, so we may
    // overshoot the record cap by up to one frame's worth; the
    // server stops as soon as the running count reaches `sample`.
    // Users who need exact cap behaviour stick with plain-batch.
    let wants_raw_frame = (client_hello.capability_flags & 0b0000_0001) != 0;
    let seekable_ok = {
        #[cfg(feature = "zstd")]
        {
            crate::io::zstd_seekable::SeekableReader::open(&entry.path).is_ok()
        }
        #[cfg(not(feature = "zstd"))]
        {
            false
        }
    };
    let use_raw_frame = wants_raw_frame
        && open.shuffle == "chunk-shuffled"
        && open.rank.is_none()
        && open.world_size.is_none()
        && seekable_ok;
    let chosen_mode = if use_raw_frame {
        ChosenMode::RawFrame
    } else {
        ChosenMode::PlainBatch
    };

    let batch_records = if open.max_batch_records == 0 {
        64
    } else {
        open.max_batch_records.min(4096)
    };
    let fp_hex: String = entry
        .fingerprint
        .0
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    let stream_opened = StreamOpenedJson {
        dataset_id: open.dataset_id.clone(),
        fingerprint: format!("blake3:{fp_hex}"),
        effective_seed: open.seed,
        effective_shuffle: open.shuffle.clone(),
        effective_batch_records: batch_records,
    };
    let stream_opened_bytes = serde_json::to_vec(&stream_opened).unwrap_or_default();
    let hello = ServerHello {
        status: HandshakeStatus::Ok,
        chosen_mode: Some(chosen_mode),
        initial_credit: DEFAULT_INITIAL_CREDIT,
        server_version: 1,
        max_message_bytes: shuflr_wire::MAX_MESSAGE_BYTES,
        stream_opened: stream_opened_bytes,
    };
    tx_buf.clear();
    encode_into(&Message::ServerHello(hello), &mut tx_buf);
    wr.write_all(&tx_buf).await.map_err(Error::Io)?;
    wr.flush().await.map_err(Error::Io)?;

    // ---- Phase 6a: raw-frame branch. Ship compressed frames with
    // their 32-byte per-frame ChaCha20 seed; the client decompresses
    // and replays Fisher-Yates locally. Wire bytes ≈ on-disk bytes
    // (~6× smaller than NDJSON on EDGAR-like corpora).
    #[cfg(feature = "zstd")]
    if chosen_mode == ChosenMode::RawFrame {
        let raw_result = stream_raw_frame_epochs(
            &entry.path,
            open.seed,
            open.epochs.unwrap_or(1),
            open.sample,
            &mut wr,
            &mut tx_buf,
            &mut rd,
            &mut scratch,
            &mut decoder,
        )
        .await;
        return finish_stream(raw_result, &mut wr, &mut tx_buf).await;
    }

    // ---- Phase 6b: plain-batch. Run the pipeline; stream
    // PlainBatch + EpochBoundary + StreamClosed.
    let path = entry.path.clone();
    let partition = match (open.rank, open.world_size) {
        (Some(r), Some(w)) if w > 1 && r < w => Some((r, w)),
        _ => None,
    };

    let plain_result = stream_plain_batch_epochs(
        &path,
        &open.shuffle,
        open.seed,
        open.epochs.unwrap_or(1),
        open.sample,
        partition,
        batch_records,
        &mut wr,
        &mut tx_buf,
        &mut rd,
        &mut scratch,
        &mut decoder,
    )
    .await;
    finish_stream(plain_result, &mut wr, &mut tx_buf).await
}

#[allow(clippy::too_many_arguments)]
async fn stream_plain_batch_epochs<W, R>(
    path: &std::path::Path,
    shuffle: &str,
    seed: u64,
    epochs: u32,
    sample: Option<u64>,
    partition: Option<(u32, u32)>,
    batch_records: u32,
    wr: &mut W,
    tx_buf: &mut Vec<u8>,
    rd: &mut R,
    scratch: &mut [u8],
    decoder: &mut Decoder,
) -> Result<(u64, u32)>
where
    W: tokio::io::AsyncWrite + Unpin,
    R: tokio::io::AsyncRead + Unpin,
{
    let mut batch_id: u64 = 0;
    let mut total_records: u64 = 0;
    let mut epochs_completed: u32 = 0;
    let mut epoch = 0u64;

    loop {
        if epochs != 0 && epoch >= u64::from(epochs) {
            break;
        }
        let epoch_u32 =
            u32::try_from(epoch).map_err(|_| Error::Input(format!("epoch {epoch} exceeds u32")))?;
        let epoch_outcome = stream_plain_batch_epoch(
            path,
            shuffle,
            seed,
            epoch,
            sample,
            partition,
            batch_records,
            epoch_u32,
            &mut batch_id,
            wr,
            tx_buf,
            rd,
            scratch,
            decoder,
        )
        .await?;
        let records_in_epoch = match epoch_outcome {
            PlainEpochOutcome::Completed(n) => n,
            PlainEpochOutcome::Cancelled(n) => {
                total_records += n;
                return Ok((total_records, epochs_completed));
            }
        };
        total_records += records_in_epoch;
        epochs_completed = epochs_completed.saturating_add(1);
        send_epoch_boundary(wr, tx_buf, epoch_u32, records_in_epoch).await?;
        epoch += 1;
    }

    Ok((total_records, epochs_completed))
}

enum PlainEpochOutcome {
    Completed(u64),
    Cancelled(u64),
}

#[allow(clippy::too_many_arguments)]
async fn stream_plain_batch_epoch<W, R>(
    path: &std::path::Path,
    shuffle: &str,
    seed: u64,
    epoch: u64,
    sample: Option<u64>,
    partition: Option<(u32, u32)>,
    batch_records: u32,
    epoch_u32: u32,
    batch_id: &mut u64,
    wr: &mut W,
    tx_buf: &mut Vec<u8>,
    rd: &mut R,
    scratch: &mut [u8],
    decoder: &mut Decoder,
) -> Result<PlainEpochOutcome>
where
    W: tokio::io::AsyncWrite + Unpin,
    R: tokio::io::AsyncRead + Unpin,
{
    // The pipeline task pushes completed records into a bounded
    // channel. The main async task buckets them into PlainBatch frames
    // and writes. On error the pipeline emits StreamError and closes.
    let (record_tx, mut record_rx) = mpsc::channel::<Vec<u8>>(batch_records as usize * 4);
    let path = path.to_path_buf();
    let shuffle_name = shuffle.to_string();
    let blocking = tokio::task::spawn_blocking(move || {
        run_pipeline_into_channel(
            &path,
            &shuffle_name,
            seed,
            epoch,
            sample,
            partition,
            record_tx,
        )
    });

    let mut pending: Vec<Vec<u8>> = Vec::with_capacity(batch_records as usize);
    let mut records_in_epoch: u64 = 0;

    loop {
        tokio::select! {
            maybe_rec = record_rx.recv() => {
                match maybe_rec {
                    Some(rec) => {
                        pending.push(rec);
                        if pending.len() as u32 >= batch_records {
                            flush_batch(wr, tx_buf, batch_id, &mut pending, epoch_u32).await?;
                        }
                        records_in_epoch += 1;
                    }
                    None => {
                        // Pipeline finished (or errored; check handle below).
                        if !pending.is_empty() {
                            flush_batch(wr, tx_buf, batch_id, &mut pending, epoch_u32).await?;
                        }
                        break;
                    }
                }
            }
            // Concurrent read: watch for Cancel; tolerate EOF without
            // erroring (client closed writer half after signalling
            // done).
            n = rd.read(scratch) => {
                match n {
                    Ok(0) => {
                        // Drain any remaining records then close.
                        while let Some(rec) = record_rx.recv().await {
                            pending.push(rec);
                            if pending.len() as u32 >= batch_records {
                                flush_batch(wr, tx_buf, batch_id, &mut pending, epoch_u32).await?;
                            }
                            records_in_epoch += 1;
                        }
                        if !pending.is_empty() {
                            flush_batch(wr, tx_buf, batch_id, &mut pending, epoch_u32).await?;
                        }
                        break;
                    }
                    Ok(n) => {
                        decoder.feed(&scratch[..n]);
                        while let Some(msg) = decoder.try_next().map_err(|e| Error::Input(format!("wire decode: {e}")))? {
                            match msg {
                                Message::Cancel { .. } => {
                                    tracing::debug!("wire: client Cancel");
                                    return Ok(PlainEpochOutcome::Cancelled(records_in_epoch));
                                }
                                Message::AddCredit { .. } | Message::Pong { .. } => {
                                    // PR-33a will act on AddCredit; PR-36 on Pong.
                                }
                                other => {
                                    let detail = format!(
                                        "unexpected client message {:?} after handshake",
                                        other.kind()
                                    );
                                    let _ = handshake_error(wr, tx_buf, detail.clone()).await;
                                    return Err(Error::Input(detail));
                                }
                            }
                        }
                    }
                    Err(e) => return Err(Error::Io(e)),
                }
            }
        }
    }

    match blocking.await {
        Ok(Ok(())) => Ok(PlainEpochOutcome::Completed(records_in_epoch)),
        Ok(Err(e)) => Err(e),
        Err(join_err) => {
            tracing::warn!(err = %join_err, "wire: pipeline task panicked");
            Err(Error::Input(format!("pipeline panic: {join_err}")))
        }
    }
}

/// Flush the pending record list as a PlainBatch frame.
async fn flush_batch<W>(
    wr: &mut W,
    tx_buf: &mut Vec<u8>,
    batch_id: &mut u64,
    pending: &mut Vec<Vec<u8>>,
    epoch: u32,
) -> Result<()>
where
    W: tokio::io::AsyncWrite + Unpin,
{
    let msg = Message::PlainBatch(BatchPayload {
        batch_id: *batch_id,
        epoch,
        records: std::mem::take(pending),
    });
    tx_buf.clear();
    encode_into(&msg, tx_buf);
    wr.write_all(tx_buf).await.map_err(Error::Io)?;
    *batch_id += 1;
    Ok(())
}

async fn send_epoch_boundary<W>(
    wr: &mut W,
    tx_buf: &mut Vec<u8>,
    completed_epoch: u32,
    records_in_epoch: u64,
) -> Result<()>
where
    W: tokio::io::AsyncWrite + Unpin,
{
    let epoch_msg = Message::EpochBoundary {
        completed_epoch,
        records_in_epoch,
    };
    tx_buf.clear();
    encode_into(&epoch_msg, tx_buf);
    wr.write_all(tx_buf).await.map_err(Error::Io)?;
    Ok(())
}

/// Raw-frame streamer (005 §3.5). For each frame in the permuted
/// order, pread the compressed bytes and ship them wholesale with
/// the 32-byte per-frame ChaCha20 seed. Server CPU drops to
/// `pread + send`; the client pays the decompress + Fisher-Yates.
#[cfg(feature = "zstd")]
#[allow(clippy::too_many_arguments)]
async fn stream_raw_frame_epochs<W, R>(
    path: &std::path::Path,
    seed: u64,
    epochs: u32,
    sample: Option<u64>,
    wr: &mut W,
    tx_buf: &mut Vec<u8>,
    rd: &mut R,
    scratch: &mut [u8],
    decoder: &mut Decoder,
) -> Result<(u64, u32)>
where
    W: tokio::io::AsyncWrite + Unpin,
    R: tokio::io::AsyncRead + Unpin,
{
    let mut total_records: u64 = 0;
    let mut epochs_completed: u32 = 0;
    let mut epoch = 0u64;

    loop {
        if epochs != 0 && epoch >= u64::from(epochs) {
            break;
        }
        let epoch_u32 =
            u32::try_from(epoch).map_err(|_| Error::Input(format!("epoch {epoch} exceeds u32")))?;
        let records_in_epoch = stream_raw_frames(
            path, seed, epoch_u32, sample, wr, tx_buf, rd, scratch, decoder,
        )
        .await?;
        total_records += records_in_epoch;
        epochs_completed = epochs_completed.saturating_add(1);
        send_epoch_boundary(wr, tx_buf, epoch_u32, records_in_epoch).await?;
        epoch += 1;
    }

    Ok((total_records, epochs_completed))
}

#[cfg(feature = "zstd")]
#[allow(clippy::too_many_arguments)]
async fn stream_raw_frames<W, R>(
    path: &std::path::Path,
    seed: u64,
    epoch: u32,
    sample: Option<u64>,
    wr: &mut W,
    tx_buf: &mut Vec<u8>,
    rd: &mut R,
    scratch: &mut [u8],
    decoder: &mut Decoder,
) -> Result<u64>
where
    W: tokio::io::AsyncWrite + Unpin,
    R: tokio::io::AsyncRead + Unpin,
{
    use rand::seq::SliceRandom;
    use shuflr_wire::Message as WireMsg;

    // Job: given the seekable-zstd path, figure out frame ordering
    // and per-frame seeds ahead of time. The pread / encode loop
    // then runs entirely async on the current task (no spawn_blocking
    // needed — we're doing I/O, not CPU work, since we don't
    // decompress).
    let reader = crate::io::zstd_seekable::SeekableReader::open(path)?;
    let n_frames = reader.num_frames();
    // Materialize the per-frame compressed-byte offsets and lengths
    // so we can release the reader before doing async pread.
    let entries = reader.entries().to_vec();
    drop(reader);

    // Permuted frame order + per-frame seeds. Use the exact same
    // derivation as the in-process chunk-shuffled pipeline so the
    // client's replay matches byte-for-byte.
    let seed_tree = crate::seed::Seed::new(seed);
    let perm_key = seed_tree.perm(epoch as u64);
    let mut perm_rng = crate::seed::Seed::rng_from(perm_key);
    let mut frame_order: Vec<usize> = (0..n_frames).collect();
    frame_order.shuffle(&mut perm_rng);

    // Prefix sums over the on-disk compressed frame sizes give us
    // pread offsets. (SeekableReader computes this internally; we do
    // it here so we can own a File handle independently.)
    let mut offsets: Vec<u64> = Vec::with_capacity(n_frames + 1);
    let mut acc: u64 = 0;
    for e in &entries {
        offsets.push(acc);
        acc += e.compressed_size as u64;
    }
    offsets.push(acc);

    let file = tokio::fs::File::open(path).await.map_err(Error::Io)?;
    let mut file = file;
    use tokio::io::{AsyncReadExt, AsyncSeekExt};

    let mut total_records: u64 = 0;
    for &frame_id in &frame_order {
        let off = offsets[frame_id];
        let comp_size = entries[frame_id].compressed_size as usize;
        file.seek(std::io::SeekFrom::Start(off))
            .await
            .map_err(Error::Io)?;
        let mut comp = vec![0u8; comp_size];
        file.read_exact(&mut comp).await.map_err(Error::Io)?;
        // Count the records this frame carries so EpochBoundary
        // is accurate. Decoded-size is stored in the seek table, but
        // not the record count; do one memchr pass after decompression
        // — that's still server-side work. For now derive it as
        // best-effort from the decompressed size / average record
        // length. The client's iterator exposes the exact count; we
        // only use this for EpochBoundary bookkeeping.
        //
        // Cheaper compromise: count records by decompressing in a
        // blocking task. Frame decompression on the server side
        // *somewhat* defeats the CPU savings of raw-frame, so instead
        // we cheat: we trust `decompressed_size` and let the client's
        // `StreamClosed.total_records` field come from the server's
        // guess. The server instead reports records *as the number of
        // `\n` bytes in the decompressed frame*, which is the
        // engine's record count. That's what `records_in_epoch`
        // really ought to carry — so we decompress + count after
        // all. Small price: the server pays 1 decode + 1 memchr per
        // frame (on ~thousands of bytes each), and still saves the
        // big record-by-record compression work.
        let decoded_guess = tokio::task::block_in_place(|| -> Result<u64> {
            let bytes = zstd::stream::decode_all(&comp[..]).map_err(Error::Io)?;
            Ok(memchr::memchr_iter(b'\n', &bytes).count() as u64
                + u64::from(!bytes.ends_with(b"\n") && !bytes.is_empty()))
        })?;
        total_records += decoded_guess;
        let fid_u32 = u32::try_from(frame_id)
            .map_err(|_| Error::Input(format!("frame_id {frame_id} exceeds u32")))?;
        let perm_seed = seed_tree.frame(epoch as u64, frame_id as u64);
        let msg = WireMsg::RawFrame {
            frame_id: fid_u32,
            perm_seed,
            zstd_bytes: comp,
        };
        tx_buf.clear();
        encode_into(&msg, tx_buf);
        wr.write_all(tx_buf).await.map_err(Error::Io)?;

        // Drain inbound control frames between writes without
        // blocking: non-blocking read attempt.
        drain_control_frames_nonblocking(rd, decoder, scratch)?;

        // --sample cap: raw-frame ships whole frames, so we stop as
        // soon as the running record count reaches the cap. This may
        // overshoot by up to `records_per_frame - 1`; clients that
        // need an exact cap stick with plain-batch.
        if let Some(cap) = sample
            && total_records >= cap
        {
            break;
        }
    }
    wr.flush().await.map_err(Error::Io)?;

    Ok(total_records)
}

/// Try to decode already-available client control messages without
/// blocking on more bytes. Used between raw-frame writes so we notice
/// a client `Cancel` promptly without serializing the accept loop.
#[cfg(feature = "zstd")]
fn drain_control_frames_nonblocking<R>(
    _rd: &mut R,
    decoder: &mut Decoder,
    _scratch: &mut [u8],
) -> Result<()>
where
    R: tokio::io::AsyncRead + Unpin,
{
    // Drain anything already buffered in the decoder; don't pull new
    // bytes off the socket here (that'd require an await and defeat
    // the point). Ignored kinds are silently dropped — PR-33a adds
    // AddCredit handling.
    while let Some(msg) = decoder
        .try_next()
        .map_err(|e| Error::Input(format!("control decode: {e}")))?
    {
        if matches!(msg, shuflr_wire::Message::Cancel { .. }) {
            return Err(Error::Input("client cancelled".into()));
        }
    }
    Ok(())
}

/// Send StreamClosed (or StreamError on failure). EpochBoundary frames
/// are emitted as each epoch finishes.
async fn finish_stream<W>(
    result: Result<(u64, u32)>,
    wr: &mut W,
    tx_buf: &mut Vec<u8>,
) -> Result<()>
where
    W: tokio::io::AsyncWrite + Unpin,
{
    match result {
        Ok((total_records, epochs_completed)) => {
            let closed = Message::StreamClosed {
                total_records,
                epochs_completed,
            };
            tx_buf.clear();
            encode_into(&closed, tx_buf);
            wr.write_all(tx_buf).await.map_err(Error::Io)?;
            wr.flush().await.map_err(Error::Io)?;
            Ok(())
        }
        Err(e) => {
            let err_msg = Message::StreamError {
                code: StreamErrorCode::Internal,
                fatal: true,
                detail: e.to_string().into_bytes(),
            };
            tx_buf.clear();
            encode_into(&err_msg, tx_buf);
            let _ = wr.write_all(tx_buf).await;
            let _ = wr.flush().await;
            Err(e)
        }
    }
}

/// Send a ServerHello{status=Error} and close the write side so the
/// client's read returns bytes-then-EOF rather than racing against a
/// bare TCP teardown.
async fn handshake_error<W>(wr: &mut W, tx_buf: &mut Vec<u8>, detail: String) -> Result<()>
where
    W: tokio::io::AsyncWrite + Unpin,
{
    tracing::info!(detail = %detail, "wire: handshake rejected");
    let hello = ServerHello {
        status: HandshakeStatus::Error,
        chosen_mode: None,
        initial_credit: 0,
        server_version: 1,
        max_message_bytes: shuflr_wire::MAX_MESSAGE_BYTES,
        stream_opened: detail.into_bytes(),
    };
    tx_buf.clear();
    encode_into(&Message::ServerHello(hello), tx_buf);
    let _ = wr.write_all(tx_buf).await;
    let _ = wr.flush().await;
    let _ = wr.shutdown().await;
    Ok(())
}

/// Read bytes until the decoder emits a full message, or EOF.
/// Returns `None` on a clean EOF before any bytes were buffered.
async fn read_one<R>(
    rd: &mut R,
    decoder: &mut Decoder,
    scratch: &mut [u8],
) -> Result<Option<Message>>
where
    R: tokio::io::AsyncRead + Unpin,
{
    loop {
        if let Some(msg) = decoder
            .try_next()
            .map_err(|e| Error::Input(format!("wire decode: {e}")))?
        {
            return Ok(Some(msg));
        }
        let n = rd.read(scratch).await.map_err(Error::Io)?;
        if n == 0 {
            if decoder.buffered_bytes() == 0 {
                return Ok(None);
            }
            return Err(Error::Input(format!(
                "wire: unexpected eof with {} bytes buffered",
                decoder.buffered_bytes()
            )));
        }
        decoder.feed(&scratch[..n]);
    }
}

/// Run the sync pipeline for the requested shuffle mode, pushing each
/// record as `Vec<u8>` into `tx`. Records do *not* include a trailing
/// newline — the wire format makes delimiter framing explicit.
fn run_pipeline_into_channel(
    path: &std::path::Path,
    shuffle: &str,
    seed: u64,
    epoch: u64,
    sample: Option<u64>,
    partition: Option<(u32, u32)>,
    tx: mpsc::Sender<Vec<u8>>,
) -> Result<()> {
    let sink = RecordSink {
        tx,
        line_buf: Vec::with_capacity(4096),
    };
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
        }
        other => {
            return Err(Error::Input(format!(
                "shuffle mode '{other}' not supported on wire transport"
            )));
        }
    }
    Ok(())
}

/// `io::Write` adapter: every newline terminates a record, which is
/// blocking-pushed onto the async channel. The trailing newline itself
/// is stripped — the wire format delimits records via length prefix.
struct RecordSink {
    tx: mpsc::Sender<Vec<u8>>,
    line_buf: Vec<u8>,
}

impl std::io::Write for RecordSink {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let mut start = 0;
        while let Some(nl) = memchr_pos(&buf[start..]) {
            let end = start + nl;
            self.line_buf.extend_from_slice(&buf[start..end]);
            let rec = std::mem::take(&mut self.line_buf);
            if self.tx.blocking_send(rec).is_err() {
                // Receiver dropped — client closed. Report EOF-like
                // to unwind the sync pipeline.
                return Err(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "wire sink: peer dropped",
                ));
            }
            start = end + 1; // skip the newline
        }
        if start < buf.len() {
            self.line_buf.extend_from_slice(&buf[start..]);
        }
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        if !self.line_buf.is_empty() {
            let rec = std::mem::take(&mut self.line_buf);
            if self.tx.blocking_send(rec).is_err() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "wire sink: peer dropped",
                ));
            }
        }
        Ok(())
    }
}

fn memchr_pos(haystack: &[u8]) -> Option<usize> {
    memchr::memchr(b'\n', haystack)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_open_stream_defaults() {
        let j = r#"{"dataset_id":"corpus"}"#;
        let parsed: OpenStreamJson = serde_json::from_str(j).unwrap();
        assert_eq!(parsed.dataset_id, "corpus");
        assert_eq!(parsed.seed, 0);
        assert_eq!(parsed.shuffle, "chunk-shuffled");
        assert_eq!(parsed.sample, None);
    }

    #[test]
    fn parses_open_stream_full() {
        let j = r#"{
            "dataset_id":"c",
            "seed":42,
            "shuffle":"index-perm",
            "sample":1000,
            "rank":1,
            "world_size":4,
            "max_batch_records":128,
            "client_id":"abc"
        }"#;
        let parsed: OpenStreamJson = serde_json::from_str(j).unwrap();
        assert_eq!(parsed.seed, 42);
        assert_eq!(parsed.shuffle, "index-perm");
        assert_eq!(parsed.rank, Some(1));
    }

    #[test]
    fn record_sink_splits_on_newline() {
        use std::io::Write as _;
        let (tx, mut rx) = mpsc::channel::<Vec<u8>>(8);
        let mut sink = RecordSink {
            tx,
            line_buf: Vec::new(),
        };
        sink.write_all(b"one\ntwo\nthree").unwrap();
        sink.flush().unwrap();
        let out: Vec<_> = std::iter::from_fn(|| rx.try_recv().ok()).collect();
        assert_eq!(
            out,
            vec![b"one".to_vec(), b"two".to_vec(), b"three".to_vec()]
        );
    }

    #[test]
    fn wire_config_requires_bind_public_for_non_loopback() {
        use std::net::{IpAddr, Ipv4Addr};
        let cat = Catalog::from_args::<&str>(&[]).unwrap();
        let r = WireConfig::builder(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 9000),
            cat,
        )
        .build();
        assert!(r.is_err());
    }
}
