# 005 — `serve`: multi-transport service surface

**Status:** design, not yet implemented. Supersedes [002 §7](./002-revised-plan.md#7-service-surface-serve-subcommand). Amendments 003 and 004 are orthogonal and stand.

**Motivation in one paragraph.** 002 cut everything but gRPC from v1 of `serve`. That was the wrong call. The primary consumer pattern — PyTorch `IterableDataset` running in network-constrained environments — is poorly served by gRPC-only: it locks out curl/wget/`jq` for smoke-testing, demands a protobuf toolchain for every consumer, and can't pass the on-disk zstd compression through to the wire. This doc specifies three transports that share one sync core: plain HTTP/1.1 (the universal fallback), `shuflr-wire/1` (a custom binary protocol paired with a Rust-core Python client, designed around the compression and framing properties the engine already has), and gRPC (for shops already invested in it). All three are feature-gated on the binary; users pick which to compile in.

---

## 0. Scope and relationship to 002

| 002 section | Status under 005 |
|---|---|
| §7.1 — proto contract (`shuflr.v1`) | **retained, reused** — gRPC transport uses it verbatim; `shuflr-wire/1` reuses `OpenStream`/`StreamOpened` as payloads inside its handshake |
| §7.2 — multi-client: `independent` only | retained |
| §7.3 — disconnect / resume | retained; resume tokens still v1.1-deferred |
| §7.4 — backpressure via HTTP/2 flow control | **expanded** — HTTP/1.1 uses chunked-body backpressure; `shuflr-wire/1` uses explicit credit-based flow control (§3.6) |
| §7.5 — observability | retained |
| §7.6 — client auth story (mTLS only) | **expanded** — TLS is now optional; auth options are `none` / `bearer` / `mtls` |
| "HTTP/2 SSE and HTTP/3 cut" (decision row 11) | HTTP/3 and SSE still deferred, but for different reasons (see §9) |

Everything else in 002 stands.

## 1. Decisions that changed from 002

| # | 005 decision | 002 said | Why it changed |
|---|---|---|---|
| 1 | Three transports in v1: HTTP/1.1, `shuflr-wire/1`, gRPC | gRPC only | PyTorch in firewalled envs needs HTTP; network-constrained training wants compression passthrough that gRPC/proto-opaque-bytes can't provide |
| 2 | TLS is optional | mTLS implied for any public bind | Corporate networks often route HTTP-only; forcing TLS either blocks the use case or drives users to wrap in their own proxy — worse opsec than clear opt-in |
| 3 | `--insecure-public` explicit flag for public-bind-without-TLS | N/A | Makes accidental plain-text public bind loud (red WARN per request) instead of impossible, so the common case doesn't fight us |
| 4 | Auth defaults to `none`; `bearer` and `mtls` available | mTLS-only | Fits the "serve to a sidecar on the same host" common case; bearer covers almost everything else; mTLS stays available for regulated envs |
| 5 | Custom binary transport paired with first-party Python client (`shuflr-client`, Rust core via pyo3) | — | Only way to pass compressed zstd frames to the wire, get xxh3 checksums, and do credit-based flow control without reinventing half of gRPC. Also the only way to hit ~disk-size wire traffic for `chunk-shuffled` |
| 6 | HTTP client always available in `shuflr-client`, selected by URL scheme | — | Training environments frequently block non-HTTP egress; the client has to work there even when it loses the compression win |
| 7 | URL scheme is the transport-negotiation primitive | — | No auto-probing, no magic — URL says the protocol. Fallback is opt-in by passing multiple URLs |
| 8 | `shuflr-wire/1` handshake uses the same `OpenStream` protobuf as gRPC | would have been a bespoke struct | Keeps one source of truth for config, makes cross-transport parity trivial, saves us a second wire-format spec |
| 9 | `raw-frame` compression mode only available for `chunk-shuffled` | — | Correctness: `index-perm` scatters records across frames, so frame-passthrough breaks the permutation semantics. Documented in §3.5 |
| 10 | Shared sync core, three thin async edges; one sync→async bridge pattern per transport | 002 had this implicitly | Made explicit because implementation requires discipline to preserve |

## 2. Transports

### 2.1 Plain HTTP/1.1 chunked NDJSON

The zero-dep on-ramp. Every language has a good HTTP/1.1 client. Works through every proxy and LB. Useful for smoke-testing (`curl host:9000/v1/streams/foo?sample=10`), for consumers in languages we don't have a client library for, and as the fallback inside `shuflr-client` when `shuflr-wire/1` can't connect.

**Endpoints:**

```
GET  /v1/datasets
     → 200 application/json
       [{"dataset_id": "...", "approx_records": N, "approx_bytes": N,
         "fingerprint": "blake3:..."}, ...]

GET  /v1/datasets/{dataset_id}
     → 200 application/json  (same shape as list, one element)

GET  /v1/streams/{dataset_id}?{params}
     → 200 application/x-ndjson; charset=utf-8
       Transfer-Encoding: chunked
       shuflr-effective-seed: <u64>
       shuflr-fingerprint: blake3:<hex>
       shuflr-stream-id: <uuid>
       (one record per line, trailing \n on every line)

GET  /v1/health
     → 200 application/json {"status": "SERVING"}

GET  /metrics
     → 200 text/plain (Prometheus exposition; see §6)
```

**Query params** on `/v1/streams/{id}` (all optional unless noted):

| param | type | default | notes |
|---|---|---|---|
| `seed` | u64 | 0 | `0` = server assigns and echoes in `shuflr-effective-seed` |
| `shuffle` | enum | `chunk-shuffled` | `none`, `buffer`, `chunk-shuffled`, `index-perm`, `reservoir` |
| `epochs` | u32 | 1 | `0` = infinite |
| `sample` | u64 | unlimited | `--sample` equivalent |
| `rank` | u32 | 0 | requires `world_size` ≥ 1 |
| `world_size` | u32 | 1 | |
| `buffer_size` | u64 | 100000 | mode=buffer only |
| `reservoir_size` | u64 | 10000 | mode=reservoir only |
| `client_id` | string | auto | for PRF hierarchy isolation |
| `resume_epoch` | u32 | 0 | |
| `resume_records_seen` | u64 | 0 | |

**Error handling mid-stream.** HTTP status is committed when the first chunk is flushed, which is before we start decoding. On an unrecoverable error after streaming begins, the server emits a final NDJSON line containing *exactly* `{"_shuflr_error":"<code>","detail":"<string>"}` and closes the connection. Clients that want precise error classification should prefer `shuflr-wire/1`; NDJSON consumers that don't care just see a truncated stream with a parseable sentinel.

**Backpressure** is TCP-native (the connection's send buffer fills when the client stops reading), which works correctly for slow consumers but provides no application-level visibility. `shuflr-wire/1` fixes that (§3.6).

### 2.2 `shuflr-wire/1` — custom binary over TCP / UDS / TLS

The hyper-efficient transport. Designed around three properties the engine already has:

1. The `chunk-shuffled` pipeline already decompresses a full frame into memory, shuffles record order within it, and emits. The **decompressed-frame step can be elided** if we ship the compressed bytes to the client and let the client do the decode + in-frame shuffle. Wire bytes ≈ on-disk bytes (≈ 6× smaller than NDJSON on EDGAR-like JSON corpora). Server CPU drops to `pread + send`; workers scale linearly with NICs rather than cores.
2. Records are opaque bytes to the engine, so they're also opaque to the wire — no per-record parsing on either side. Length-prefixed batches amortize framing overhead.
3. Shuflr is fully deterministic from `(seed, config, fingerprint, client_id)`. A client can **reconstruct the record permutation without any per-record metadata on the wire** — only a frame-ID and a per-frame seed. This is the foundation of the raw-frame mode.

Spec in §3. The protocol is self-contained (no dependency on proto except reusing `OpenStream`/`StreamOpened` as handshake payloads, so anyone implementing it from the spec needs a minimal prost/pyo3-prost decoder — same as gRPC already requires).

### 2.3 gRPC

002 §7.1's protobuf design carries forward verbatim. Feature-gated on `--features grpc` because the tonic dep graph is ~200 crates. For shops that already run gRPC infra and don't want to pull in a new client library, this is the off-ramp.

Unchanged from 002: `ListDatasets`, `Stream(stream OpenStream) returns (stream StreamServerMessage)`, `Health`.

### 2.4 URL scheme — transport negotiation

The Python client (§5) and the `shuflr-cat` smoke-test helper (bundled with the binary) both accept URLs and pick the transport from the scheme. **No auto-probing.**

| Scheme | Transport | TLS | Notes |
|---|---|---|---|
| `http://host:port/v1/...` | HTTP/1.1 | no | |
| `https://host:port/v1/...` | HTTP/1.1 | yes | rustls |
| `shuflr://host:port/{dataset}` | `shuflr-wire/1` over TCP | no | |
| `shuflrs://host:port/{dataset}` | `shuflr-wire/1` over TCP + TLS | yes | rustls |
| `shuflr+unix:///path/to.sock/{dataset}` | `shuflr-wire/1` over UDS | no | TLS over UDS rejected at parse |
| `grpc+tcp://host:port` | gRPC (when feature is compiled in) | no | |
| `grpcs://host:port` | gRPC + TLS | yes | |

Multi-URL fallback: `shuflr-client` accepts `urls=[...]` and tries each in order. Typical pattern for a training cluster:

```python
urls=[
    "shuflr+unix:///var/run/shuflr.sock/corpus",  # same-host sidecar
    "shuflr://10.0.0.5:9000/corpus",              # VPN fallback
    "https://training-data.corp.internal/v1/streams/corpus",  # firewall fallback
]
```

Each URL is tried with a bounded handshake timeout. The first that completes an `OpenStream` becomes the active transport. If all fail, `OSError` up to PyTorch.

## 3. `shuflr-wire/1` protocol

All multi-byte integers are **little-endian**, matching the on-disk seekable-zstd format. All lengths are in bytes. There is no text in the protocol; all error messages are UTF-8 byte strings inside typed error messages.

### 3.1 Framing

Every message after the handshake is:

```
+-----------+--------------+--------------------+--------+
| kind (u8) | payload_len  | payload            | xxh3   |
|           | (u32, LE)    | (payload_len bytes)| (u64)  |
+-----------+--------------+--------------------+--------+
```

`xxh3` covers `kind || payload_len_le || payload`. The client MAY skip verification for throughput (the kernel already checksums at TCP); the server always computes and sends.

Max `payload_len`: **512 MiB** (bigger than EDGAR's largest single-record frame of 427 MiB, with headroom). Server rejects anything larger at config parse.

### 3.2 Handshake

Client sends `ClientHello`, server sends `ServerHello`. Both are framed as in §3.1 with `kind=0`. No xxh3 checksum on the handshake (chicken-and-egg with versioning).

**ClientHello payload:**

```
magic             [u8; 8] = b"SHUFLRW1"
version           u8       = 1
capability_flags  u8       // bit 0: understands raw-frame mode
                          // bit 1: understands zstd-batch mode
                          // bit 2: sends xxh3 on control messages
                          // bits 3-7: reserved, MUST be 0
auth_kind         u8       // 0 none, 1 bearer, 2 mtls (only meaningful with TLS)
auth_len          u16
auth              [u8; auth_len]     // bearer token bytes; empty for none/mtls
open_stream_len   u32
open_stream       [u8; open_stream_len]  // protobuf-encoded shuflr.v1.OpenStream
```

`open_stream` uses the exact `OpenStream` message from 002 §7.1. Reusing it means a server can route config parsing through one path regardless of transport.

**ServerHello payload:**

```
status            u8       // 0 = ok; else error code from §3.4
chosen_mode       u8       // 1 raw-frame, 2 zstd-batch, 3 plain-batch (valid iff status=0)
initial_credit    u64      // bytes; server won't send more until client returns credit
server_version    u8       // 1
max_message_bytes u32      // informational; server rejects beyond this
stream_opened_len u32
stream_opened     [u8; stream_opened_len]  // protobuf-encoded StreamOpened (or error detail if status != 0)
```

**Mode negotiation.** The server picks `chosen_mode` from the intersection of (a) what the client's `capability_flags` say it can decode, (b) what the requested shuffle mode allows (§3.5), and (c) what the server's config permits (e.g. an operator may disable raw-frame to centralize decompression cost). Preference order, best first: `raw-frame`, `zstd-batch`, `plain-batch`. `plain-batch` is the universal fallback.

If the server can't satisfy any mode the client understands, it replies with `status != 0` and closes.

### 3.3 Stream messages (server → client)

| kind | name | payload layout |
|---|---|---|
| 1 | `RawFrame` | `frame_id u32 \| perm_seed u64 \| zstd_bytes[...]` |
| 2 | `ZstdBatch` | `batch_id u64 \| epoch u32 \| n_records u32 \| zstd_bytes[...]` (decompresses to `n_records × (len u32, bytes[len])`) |
| 3 | `PlainBatch` | `batch_id u64 \| epoch u32 \| n_records u32 \| n_records × (len u32, bytes[len])` |
| 4 | `EpochBoundary` | `completed_epoch u32 \| records_in_epoch u64` |
| 5 | `StreamError` | `code u8 \| fatal u8 \| detail_bytes[...]` |
| 6 | `StreamClosed` | `total_records u64 \| epochs_completed u32` |
| 7 | `Heartbeat` | `now_unix_nanos u64` (server sends every 5s on idle; client may ignore) |

### 3.4 Control messages (client → server)

| kind | name | payload layout |
|---|---|---|
| 10 | `AddCredit` | `add_bytes u64` |
| 11 | `Cancel` | `reason_bytes[...]` |
| 12 | `Pong` | `now_unix_nanos u64` (response to server `Heartbeat`; optional) |

### 3.5 Compression modes

**`raw-frame` (only valid for `shuffle=chunk-shuffled`).** Server preads a frame from the seekable-zstd input, sends the raw compressed bytes with `(frame_id, perm_seed)`. Client decompresses, runs the same Fisher-Yates with `perm_seed` we use server-side, emits records in shuffled order. Byte-for-byte equivalence with the CLI's `chunk-shuffled` output at the same `(seed, epoch)` — tested the same way PRs 26-28 tested parallel equivalence: server-side sequential output vs. client-side replayed output, `diff -q`.

Why `chunk-shuffled` only: the output record order is a permutation *within* each frame, with frames emitted in a permuted-but-fixed order. The frame identity is preserved on the wire. `index-perm` emits records in a global permutation that deliberately breaks frame identity — a client receiving frame N can't know which records from N belong next in the output without the full record index. Shipping the index to the client works but defeats the sidecar model.

**`zstd-batch`.** Server accumulates N records (`n_records` configurable, server default 64) into a single zstd stream, ships the frame. Client decompresses, iterates records. Works for every shuffle mode. Compression ratio is worse than `raw-frame` (small-context zstd vs. 2 MiB-context on-disk frames) but still much better than NDJSON over HTTP.

**`plain-batch`.** No compression. Universal fallback — guaranteed to work with any client, any corpus, any environment. Used when the client's `capability_flags` only promise plain-batch, or when the server is configured to disable zstd (e.g., already doing TLS hardware-accelerated compression downstream).

### 3.6 Credit-based flow control

At handshake, server advertises `initial_credit` bytes. Every `RawFrame`/`ZstdBatch`/`PlainBatch` the server sends debits its available-credit counter by `payload_len + header_len`. When available credit drops below `low_watermark` (default 25% of initial credit), the server pauses and waits for an `AddCredit` message.

The client's job: periodically send `AddCredit(n)` where `n` is bytes consumed since the last credit refill. Typical implementation: the Python client ticks credit every batch-consumed callback.

**Why this over TCP-native backpressure.** With TCP, a slow consumer fills the kernel's socket buffer silently; the server sees `write()` block but can't distinguish "slow network" from "slow consumer" and can't reliably evict one before the other. With explicit credit, the server sees the client running out and can emit a `StreamError{code=SLOW_CONSUMER, fatal=true}` with a clean close, releasing the pipeline's chunk pool. Same signal 002 §7.4 wanted, just explicit rather than inferred.

### 3.7 Versioning and extensibility

Magic `SHUFLRW` + single-byte version. Bump version for breaking changes. Within a version, **reserved bits in `capability_flags` and reserved message kinds (8, 9, 13+) MAY be defined in future minor revisions**; a server receiving an unknown reserved bit set by a client treats it as 0 (client will degrade; server does not). A client receiving an unknown message kind from the server MUST close the connection with `Cancel{reason="unknown message kind"}` — no silent skipping, because message bytes are meaningless without the schema.

## 4. Security

### 4.1 Bind policy

| bind target | TLS required? | auth required? | flag gate |
|---|---|---|---|
| `127.0.0.1`, `::1` | no | no | none |
| UDS (`unix:///...`) | no (rejected) | no (file perms) | none |
| Any other interface | **one of:** TLS or `--insecure-public` | optional | `--bind-public` |

`--bind-public` is necessary for any non-loopback bind, unchanged from 002. What's new: `--bind-public` without TLS is allowed **iff** `--insecure-public` is also set. The server then:

- Logs `WARN serve: running UNENCRYPTED on public interface <addr>; all traffic is plaintext and all tokens are in the clear` at startup.
- Emits the same WARN once per accepted connection with the peer address.
- Exposes a Prometheus gauge `shuflr_insecure_connections_total` (counter) so operators can alert on this.

The flag name is deliberately unfriendly. `--no-tls` would be too easy.

### 4.2 TLS

`--tls-cert <pem>` and `--tls-key <pem>` enable rustls-backed TLS on HTTP and `shuflr-wire`. gRPC uses the same cert material via tonic's rustls integration.

Minimum TLS version: 1.3. No opt-out. No negotiation of TLS 1.2.

### 4.3 Authentication

`--auth=none|bearer|mtls` (default `none`). Orthogonal to TLS.

- **none.** No auth. Intended for loopback, UDS, same-host sidecar, and test deployments. Fine with `--insecure-public` if operators accept the risk.
- **bearer.** `--auth-tokens <file>` is a newline-delimited file of allowed bearer tokens (compared by constant-time equality). Client sends the token in `ClientHello.auth` (binary) or `Authorization: Bearer <token>` (HTTP) or gRPC metadata. Token file is reread on `SIGHUP` for rotation without downtime.
- **mtls.** Requires TLS. `--tls-client-ca <pem>` specifies the CA(s) that may sign client certs. The client's common-name is logged; it is NOT used for authorization in v1 — token-style authz (bearer) handles that when needed.

`--auth=bearer` with `--auth-tokens` missing is a startup error. `--auth=mtls` without `--tls-cert` is a startup error.

### 4.4 Rate limits and quotas

Out of scope for v1. A single server is assumed to be within a trust boundary (sidecar, VPN, or authenticated fleet). If external rate limiting is needed, put a proxy in front. 002 §7.4's slow-consumer eviction is the only resource control shipped in v1; it bounds memory per client but not request rate.

## 5. Python client (`shuflr-client`)

### 5.1 Packaging

- Rust crate `shuflr-client` in the workspace, alongside `shuflr` and `shuflr-cli`.
- Python bindings via `pyo3`.
- Wheels built via `maturin build --release` on CI, targeting:
  - `manylinux_2_28_x86_64`
  - `manylinux_2_28_aarch64`
  - `macosx_11_0_universal2`
  - `win_amd64` (best-effort; primary target is Linux)
- Published to PyPI as `shuflr-client`. Version tracks the Rust core (not the CLI binary). Breaking protocol changes bump major.
- Pure-Python wrapper in `shuflr_client/__init__.py` re-exports the pyo3 classes with type stubs (`.pyi`).

No Python-side dependencies beyond the standard library. Torch is an optional peer — `shuflr.IterableDataset` subclasses `torch.utils.data.IterableDataset` if torch is importable, else falls back to a structurally identical standalone class.

### 5.2 API surface

```python
import shuflr

ds = shuflr.Dataset(
    urls="shuflr://host:9000/corpus",    # or list for fallback
    seed=42,                              # 0 = server-assigned
    shuffle="index-perm",                 # or "chunk-shuffled", "buffer", ...
    epochs=1,                             # 0 = infinite
    sample=None,                          # Optional[int]
    rank=None,                            # auto-detected from PyTorch worker
    world_size=None,                      # auto-detected
    client_id=None,                       # auto-generated uuid by default
    auth_token=None,                      # bearer token string
    timeout=30.0,                         # handshake timeout per URL
    prefetch_batches=4,                   # wire-level lookahead
    tls_verify=True,                      # None = default system roots
    tls_client_cert=None,                 # (cert_path, key_path) for mTLS
    resume_state_path=None,               # Optional[str] — JSON file for resume
)

# Usage A: raw bytes iterator
for record_bytes in ds:                   # type: bytes
    ...

# Usage B: JSON-decoded iterator (uses orjson if available)
for record in ds.as_json():               # type: Any
    ...

# Usage C: PyTorch
import torch
loader = torch.utils.data.DataLoader(ds, batch_size=None, num_workers=4)
# num_workers > 1: each worker opens its own stream with auto rank/world_size
```

Key design notes:

- Records are delivered as `bytes` by default. No implicit JSON decode — consumers who want dicts call `.as_json()` (internally `orjson.loads` when available, else `json.loads`).
- **PyTorch multi-worker.** When `num_workers > 1`, each worker subprocess reads `torch.utils.data.get_worker_info()` and passes `rank=worker_info.id, world_size=worker_info.num_workers` to the server. Server-side rank-partitioning (002 §2) guarantees disjoint streams.
- **Resume.** If `resume_state_path` is set, the client writes `(epoch, records_seen, stream_id)` every N records. On reconnect, those flow into the next `OpenStream` as `ResumeHint`. 002 §7.3's best-effort semantics: server skips the claimed records; not cryptographically verified.
- **Blocking API.** The iterator is synchronous (PyTorch workers are processes, not async). Internally: pyo3-held tokio runtime on a dedicated thread does the streaming I/O; the foreground thread blocks on an `mpsc::Receiver`.

### 5.3 URL handling

Parsing is strict — bad scheme → `ValueError` at `Dataset(...)` construction, not at first `__iter__`. Multi-URL fallback tries each in order with `timeout` per handshake. A tried-and-failed URL is reported in the error chain on the final `OSError`.

Scheme → transport table is the same as §2.4. One addition for Python: `shuflr+unix:///path/to.sock/{dataset}` is parsed as UDS path + dataset suffix; the Python client enforces the path part contains a valid socket (statable as socket file).

### 5.4 PyTorch example

```python
import os, torch, shuflr

ds = shuflr.Dataset(
    urls=[
        "shuflr+unix:///var/run/shuflr.sock/corpus",
        "https://shuflr.corp.internal/v1/streams/corpus",  # firewall fallback
    ],
    seed=int(os.environ.get("TRAIN_SEED", "42")),
    shuffle="index-perm",
    epochs=0,  # infinite; controller stops by global step count
    prefetch_batches=8,
    auth_token=os.environ.get("SHUFLR_TOKEN"),
    resume_state_path=f"/var/cache/shuflr/resume-rank{os.environ['RANK']}.json",
)

loader = torch.utils.data.DataLoader(
    ds.as_json(),
    batch_size=None,       # shuflr-client handles batching; torch passes through
    num_workers=4,
)

for record in loader:
    # record is a dict parsed from JSON
    loss = train_step(record)
```

### 5.5 Wire-level vs. Python-level batching

Two batching layers, deliberately:

- **Wire batches** (`ZstdBatch`/`PlainBatch`) live inside `shuflr-client` and are invisible to Python. Their size balances compression ratio (bigger = better) against first-byte latency (bigger = longer wait for the first record). Server default 64 records, override via `?wire_batch_size=N` on HTTP or `OpenStream.max_batch_records` on wire/gRPC.
- **Python batches** are the `DataLoader(batch_size=M)` the caller configures. Torch already handles this downstream of our iterator; we don't get involved.

Users only think about the Python batch size; `shuflr-client` auto-tunes wire batches based on `prefetch_batches` and observed throughput.

## 6. Observability

Retained from 002 §7.5 with these additions:

| Metric | Type | Labels | Added in | Notes |
|---|---|---|---|---|
| `shuflr_transport_connections_active` | gauge | `transport` ∈ `{http,wire,grpc}` | 005 | |
| `shuflr_wire_mode_chosen_total` | counter | `mode` ∈ `{raw_frame,zstd_batch,plain_batch}` | 005 | |
| `shuflr_credit_wait_seconds` | histogram | `dataset_id` | 005 | time server waits for `AddCredit` |
| `shuflr_insecure_connections_total` | counter | `transport` | 005 | only incremented on `--insecure-public` |

HTTP exposes `/metrics` on the main listener (same port). `shuflr-wire` and gRPC share the same Prometheus registry; a separate `--stats-addr` is still supported for isolation.

## 7. Examples

### 7.1 Server: same-host sidecar, no TLS, no auth

```bash
shuflr serve \
    --wire unix:///var/run/shuflr.sock \
    --dataset main=/data/corpus.jsonl.zst
```

Client (Python):

```python
ds = shuflr.Dataset("shuflr+unix:///var/run/shuflr.sock/main", shuffle="index-perm")
```

### 7.2 Server: internal HTTP, bearer auth

```bash
shuflr serve \
    --http 10.0.0.5:9000 --bind-public --insecure-public \
    --auth=bearer --auth-tokens /etc/shuflr/tokens \
    --dataset corpus=/data/corpus-v2.jsonl.zst
```

Client: `curl -H "Authorization: Bearer $TOKEN" http://10.0.0.5:9000/v1/streams/corpus?sample=10`

### 7.3 Server: public TLS, mTLS, wire + HTTP

```bash
shuflr serve \
    --http 0.0.0.0:443 --wire 0.0.0.0:9443 --bind-public \
    --tls-cert /etc/ssl/shuflr.pem --tls-key /etc/ssl/shuflr.key \
    --auth=mtls --tls-client-ca /etc/ssl/client-ca.pem \
    --dataset corpus=/data/corpus.jsonl.zst
```

Client:

```python
ds = shuflr.Dataset(
    "shuflrs://shuflr.example.com:9443/corpus",
    tls_client_cert=("/etc/ssl/client.pem", "/etc/ssl/client.key"),
    shuffle="chunk-shuffled",  # wire will negotiate raw-frame mode → ~6× less bandwidth
)
```

### 7.4 Client: fallback chain for a flaky VPN

```python
ds = shuflr.Dataset(
    urls=[
        "shuflrs://fast.example.com:9443/corpus",   # preferred: wire + TLS
        "https://fast.example.com/v1/streams/corpus",  # firewall-friendly
        "shuflr+unix:///var/run/shuflr.sock/corpus", # last resort if local
    ],
    auth_token=os.environ["SHUFLR_TOKEN"],
    timeout=10.0,
)
```

## 8. PR breakdown

| PR | Content | Est. LOC | Blocks |
|---|---|---|---|
| 29 | This doc. | 0 code | none |
| 30 | HTTP transport: loopback only, no auth, NDJSON. Async-edge scaffolding lives here and is reused by 32/33/35. | ~400 | 31, 33, 34 |
| 31 | TLS + auth (`--tls-cert/--tls-key`, `--auth=bearer`, `--insecure-public`). Applies to HTTP first; wire + gRPC pick it up. | ~300 | 33, 34 |
| 32 | `shuflr-wire` codec crate (new workspace member). Pure framing / parsing / checksums; no I/O. Unit + proptest round-trips. | ~400 | 33 |
| 33 | `shuflr-wire/1` transport wired to serve. TCP + UDS, TLS via PR-31. Reuses codec from PR-32. | ~400 | 34 |
| 34 | `shuflr-client` Python package: Rust core + pyo3, HTTP + wire, `Dataset` / `IterableDataset`, CI for manylinux + macOS wheels, PyTorch smoke test. | ~800 incl. CI | — |
| 35 | gRPC transport, feature-gated `--features grpc`. Implements 002 §7.1 proto over the same sync core. | ~500 | — |
| 36 | Observability: `/metrics` endpoint, tracing spans, slow-consumer eviction. | ~400 | — |

Dependencies are linear for 30→31→33→34. PR-32 can land in parallel with 30-31. PR-35 and PR-36 can land at any point after PR-34 if we want to treat the shuflr-client release as the v1 ship line.

**Tests per PR:**
- PR-30: HTTP round-trip tests against every shuffle mode; NDJSON format pinned by `assert_cmd` + regex; 404 / 500 paths.
- PR-31: rustls roundtrip; bearer token match + mismatch; `--insecure-public` WARN emission; startup-error matrix.
- PR-32: proptest framing round-trip; malformed-frame handling; xxh3 verification; max-message-size cap.
- PR-33: in-process client+server byte-identity test vs. CLI output for every mode × every compression mode.
- PR-34: `maturin develop` + pytest against a loopback server; byte-identity test vs. HTTP; PyTorch `DataLoader` with `num_workers=4` rank-partitioning test; resume-round-trip test.
- PR-35: tonic client + server byte-identity vs. CLI.
- PR-36: prom scrape test; slow-consumer eviction deadline test.

## 9. Explicitly deferred

Each of these was considered and rejected for v1 of `serve`. All can be added in future amendments without breaking 005 wire compat.

- **HTTP/3 / QUIC.** Rust `quinn` + `h3` stack is solid in 2026, but requires another TLS config surface and brings no clear win over `shuflr-wire/1` for the ML case (which dominates). Defer until a user with a real flaky-mobile-edge use case shows up.
- **WebSockets.** SSE or HTTP streaming plus an HTTP POST control endpoint cover every one-way streaming case we've identified. WebSockets only win if we add rich client→server control, which we don't plan.
- **Server-Sent Events.** Superseded by plain HTTP chunked NDJSON for our one-way case. If we later need auto-reconnect with `Last-Event-ID` semantics from browsers, SSE is ~50 lines on top of the HTTP transport.
- **Dynamic dataset catalog reload.** Datasets are fixed at `shuflr serve` startup via `--dataset ID=PATH`. Reloadable catalogs imply a control-plane API and multi-generation file-handle lifecycle; both are larger than a single PR.
- **OAuth / OIDC / SAML.** Bearer tokens + mTLS cover the use cases in scope. OIDC integrates via a front proxy for now.
- **Server-side rate limiting, quotas.** 002 §7.4's slow-consumer eviction stays the only resource control in v1.
- **Arrow Flight, Kafka wire protocol, Parquet-over-stream.** Adds heavy deps for marginal wins over `shuflr-wire/1` for the PyTorch case.

---

## Appendix A — wire size, worked example

EDGAR 0.05 corpus (from bench/001):

- Decompressed JSON: 192.68 GB
- On-disk zstd-seekable: 30.92 GB (6.23× ratio)
- Mean record: 161 KB

Full-epoch `chunk-shuffled` emit over 1.2M records:

| Transport | Wire bytes (est.) | Server CPU (est.) | Notes |
|---|---|---|---|
| HTTP / NDJSON | ~193 GB | full decompress + write | no compression over wire |
| `shuflr-wire/1` `plain-batch` | ~193 GB | full decompress + write | wire framing only |
| `shuflr-wire/1` `zstd-batch` | ~35-45 GB | full decompress + re-compress | context smaller than on-disk frames |
| `shuflr-wire/1` `raw-frame` | **~31 GB** | pread + send | **client** decompresses |
| gRPC (proto-opaque bytes) | ~195 GB | full decompress + write | proto framing adds ~1% |

The 6× saving isn't academic — it's the difference between a 300 Mbps link finishing in ~15 minutes vs. ~1.5 hours.

## Appendix B — threat model (summary)

What the protocol defends against:

- **Accidental plaintext on public bind.** `--insecure-public` makes this a deliberate, loud choice.
- **Token reuse across clients.** Bearer tokens are scoped per-client-id in logs; rotation is a single file update + SIGHUP.
- **Path traversal from the client.** Clients reference `dataset_id`s; filesystem paths are in the server's config only.
- **Slow-consumer DoS of one pipeline crowding out others.** Credit-based flow control + slow-consumer eviction.
- **Replay of intercepted streams when TLS is off.** Out of scope — if you can't use TLS, you accept replay. Operators who need this run behind WireGuard/equivalent.

What it does **not** defend against:

- **Server compromise.** Any attacker with the server's private key or token file can impersonate the server or any client. Standard opsec applies.
- **Client-side tampering with `client_id`.** `client_id` drives the PRF hierarchy; a client that lies about it sees a different (but still deterministic) stream. Not a privilege escalation — just different output. 002 §3 is clear on this.
- **Traffic analysis over TLS.** Record sizes and timing are visible to on-path observers even with TLS. Probably doesn't matter for training corpora but noted.
