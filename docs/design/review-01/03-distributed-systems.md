# shuflr v1 — Distributed Systems Review

*Reviewer: senior distributed systems engineer. I have been paged for leaked mmap FDs after a client disconnect. I care about what happens on a bad Tuesday, not a demo.*

## TL;DR

**Would I deploy the service surface in `001-initial-plan.md` to production? No — but not because the core idea is wrong.** The chunked-riffle algorithm and the stdout pipe are fine. The network service is one line of proto and a hand-wave about backpressure. There is no auth story, no client-disconnect semantics, no per-client isolation model, and no resumability — and any one of those is enough to take down a node hosting three training jobs. Cut HTTP/2 and HTTP/3 from v1, lock down a real gRPC contract, bind to loopback by default, and ship a narrower but defensible service. Everything below is in service of that.

## Strengths

- Sync core + `tokio` only at the edge is the right split; it keeps the service layer from infecting the shuffle hot path.
- `crossbeam` bounded channels give you real (not advisory) backpressure inside the process.
- `--seed` + ChaCha20 means "give me the same order again" is a well-defined request — this is a precondition for any resumability story.
- Stateless-by-default posture (no index file for `chunk-shuffled`) means restart semantics are simpler than e.g. Kafka — you don't have to manage a WAL.

## Concerns and Gaps

### 1. The gRPC API is one line. That is the whole problem.

`stream Record GetBatch(Request)` is not an API; it is an aspiration. Real questions the plan does not answer:

- Is the stream server-streaming or bidi? (It must be bidi if clients ever want to ack, pause, or change rate.)
- What uniquely identifies a *session*? A client-chosen `stream_id`? A `(seed, input_fingerprint, config_hash)` tuple? Without this, "resume" is undefined even in principle.
- Versioning: no `package shuflr.v1;`, no reserved fields, no deprecation plan. Once a training job depends on this proto, you cannot move fields.
- `--sample N` interaction with streaming: does the server count toward N across reconnects, or per-RPC?

A concrete proto sketch is below — it is the single most useful thing this review can contribute.

### 2. Disconnect semantics are undefined, and this is a footgun at 3am.

The plan says resumable cursors are v2. That is tenable **only if the server is explicitly documented as ephemeral and clients are told to treat disconnects as "start over with the same seed."** Today's document says neither. Three concrete failure modes:

- Client pulls 10k records, TCP RST. Server-side: does the `ShuffleCore` task drain or get cancelled? If it drains into a now-dead channel, you leak a thread and an mmap until GC. Tonic will not save you here — you have to plumb cancellation into the sync core.
- Client reconnects with same `(seed, config)`. Does it get records 1..N again (duplicate work, at-least-once semantics) or records N+1..? The plan implies the former but never says so.
- Server crashes, systemd restarts it. Same question. Answer should be: "identical output given identical `(seed, input_fingerprint, config)`; the client's own cursor tells it where to resume reading." Say that out loud.

### 3. `--fanout` / `--partition` / `--independent` are terminology, not a spec.

Pin these down in v1 even if only one is implemented:

- **fanout (broadcast):** every connected client sees every record. New client joining mid-stream: does it get records from "now," or from epoch start? Pick one. Recommendation: "from now"; late joiners request `epoch=current, offset=latest`.
- **partition (shared iterator):** records distributed disjointly. A client dying means its in-flight records are lost unless you ack them. That means ack-based delivery, which means bidi streaming, which means a real protocol. **My recommendation: do not ship partition mode in v1.** It is a distributed queue, not a shuffler. Punt to v1.2.
- **independent:** each client gets its own shuffle iterator with its own seed. Trivially correct. Ship this as the v1 multi-client story and nothing else.

### 4. Backpressure is handled inside the process, not at the API boundary.

"Bounded channels fill, scheduler stops prefetching" is true and fine — inside one process. What does a slow gRPC client see? Tonic's server-streaming `Sender::send` will `.await` when the HTTP/2 flow-control window is full. If the shuffle core is feeding that sender through a `mpsc`, the `mpsc` backs up, the crossbeam bridge backs up, and eventually the chunk-pool idles. Good. But:

- A single slow client stalls the iterator for **that client only** (fine for `independent`, catastrophic for `partition`).
- There is no admission control. If 50 clients connect, you spin up 50 shuffle pipelines, each holding `M · chunk_size` mapped. Default 8 × 256 MiB × 50 = 100 GiB of mmap. The kernel will not thank you.
- There is no client-visible timeout. Recommend: server-side `keepalive` + a `max_slow_consumer_lag` (records or bytes behind head) after which the server cancels with `RESOURCE_EXHAUSTED`.

### 5. Multi-tenant isolation is not addressed at all.

Three clients, one `shuflr` process: do they share a `ChunkPool`? Today's architecture diagram shows one pool. If clients have different `(input, chunk-size)` they cannot share. Decision needed: **one pipeline per `(input_fingerprint, shuffle_config)` equivalence class, reference-counted, torn down when last client leaves.** Otherwise RSS is unbounded in the number of distinct configs.

### 6. Observability needs histograms, not just counters.

Prometheus bytes/sec is not enough. Concrete v1 metric set:

- `shuflr_rpc_duration_seconds{rpc,code}` — histogram, native-histogram if supported.
- `shuflr_stream_records_sent_total{stream_id}` — counter.
- `shuflr_stream_client_lag_bytes{stream_id}` — gauge (bytes buffered but not acked by HTTP/2).
- `shuflr_chunk_rotation_duration_seconds` — histogram; this is the latency tail that will bite you.
- `shuflr_mmap_active_bytes` — gauge. Page this when > budget.
- `shuflr_channel_depth{stage}` — gauge for `chunk→shuffle`, `shuffle→sink`.
- `shuflr_errors_total{kind}` — kind ∈ {malformed_line, oversized_line, client_cancelled, slow_consumer_evicted, io}.
- `tracing` spans: one root span per RPC with `stream_id`, `seed`, `input_fp`; child spans for chunk loads and shuffle passes. Export OTLP.

### 7. Security: the plan has none. This is the biggest v1 gap.

An unauthenticated reader of arbitrary files on the network is a data-exfiltration daemon. Minimum v1 posture:

- Default `--listen` / `--grpc` binds to `127.0.0.1` and UDS. Binding to a non-loopback address requires `--bind-public` (explicit opt-in) **and** TLS config.
- mTLS via `--tls-cert`, `--tls-key`, `--tls-client-ca`. Rustls, not OpenSSL.
- A shared-secret bearer-token mode (`--auth-token-file`) for the cases where mTLS is overkill (single-tenant sidecar).
- The input path is a **server-side** choice, not a client-side one. Clients select among pre-configured `dataset_id`s; they never pass a filesystem path over the wire. This is non-negotiable.

### 8. gRPC vs HTTP/2 vs HTTP/3

Pick one for v1. gRPC over TLS with HTTP/2 transport covers:

- Training frameworks (PyTorch, JAX dataloaders) — gRPC clients are trivial.
- Streaming with native flow control.
- mTLS, deadlines, cancellation, bidi — all in-box.

HTTP/2 SSE: niche, no win over gRPC here. **Cut.** HTTP/3 / QUIC: aspirational, and the `quinn` + `tonic` story is not production-clean yet. **Cut.** Raw TCP/UDS framed output: keep — it is the `netcat` story and it is 200 lines of code.

### 9. Deployment topology is unspecified — pick two.

Propose these as the canonical v1 topologies, and design accordingly:

**A. Training sidecar (primary).** `shuflr` runs on the same node as the trainer, UDS socket, one dataset, `--independent` per rank or one shared `partition` iterator (v1.2). No TLS. This is 80% of usage.

**B. Shared dataset node.** One `shuflr` per dataset host, serves multiple training jobs over gRPC+mTLS on a private network. Multi-tenant: one pipeline per `(dataset_id, config)`, capped by `--max-concurrent-streams`. This is where all the hard problems in §4–§7 actually matter.

Do not pretend to support "public internet" deployment in v1. Document that it is unsupported.

## Concrete gRPC API Sketch (v1)

```proto
syntax = "proto3";
package shuflr.v1;

// Server-selected dataset catalog; clients never send paths.
service Shuflr {
  // List datasets this server is configured to serve.
  rpc ListDatasets(ListDatasetsRequest) returns (ListDatasetsResponse);

  // Open a shuffled stream. Bidi so clients can send flow-control /
  // ack messages; v1 server MAY ignore client messages other than Cancel.
  rpc Stream(stream StreamClientMessage) returns (stream StreamServerMessage);

  // Liveness/readiness.
  rpc Health(HealthRequest) returns (HealthResponse);
}

message ListDatasetsRequest {}
message ListDatasetsResponse {
  repeated DatasetInfo datasets = 1;
}
message DatasetInfo {
  string dataset_id = 1;        // server-assigned, stable
  string input_fingerprint = 2; // hash of (size, mtime, path-set)
  uint64 approx_records = 3;    // best-effort
  uint64 approx_bytes = 4;
}

message StreamClientMessage {
  oneof kind {
    OpenStream open = 1;
    Cancel cancel = 2;
    // Reserved for v1.x: Ack, Pause, Resume, RateHint.
  }
}

message OpenStream {
  string dataset_id = 1;
  uint64 seed = 2;              // required; no implicit randomness
  ShuffleConfig config = 3;
  ResumeHint resume = 4;        // optional; best-effort in v1
  uint32 max_batch_records = 5; // client hint; server decides
  uint32 max_batch_bytes = 6;
}

message ShuffleConfig {
  enum Mode {
    MODE_UNSPECIFIED = 0;
    NONE = 1;
    CHUNK_RR = 2;
    CHUNK_SHUFFLED = 3;
    RESERVOIR = 4;
  }
  Mode mode = 1;
  uint64 chunk_size_bytes = 2;  // 0 = server default
  uint32 active_chunks = 3;     // 0 = server default
  uint32 reservoir_size = 4;
  uint32 epochs = 5;            // 0 = infinite
  bool jitter_boundaries = 6;
}

// v1 semantics: best-effort. Server MAY restart at epoch 0.
// v2 will guarantee exact resume given identical (dataset_id, seed, config).
message ResumeHint {
  uint32 epoch = 1;
  uint64 records_already_seen = 2;
}

message Cancel { string reason = 1; }

message StreamServerMessage {
  oneof kind {
    StreamOpened opened = 1;
    RecordBatch batch = 2;
    EpochBoundary epoch = 3;
    StreamError error = 4;
    StreamClosed closed = 5;
  }
}

message StreamOpened {
  string stream_id = 1;         // server-assigned; log this
  string input_fingerprint = 2; // echoed for client verification
  ShuffleConfig effective = 3;  // with server defaults filled in
}

message RecordBatch {
  // Length-prefixed concat is the wire choice (see plan Q3).
  // Each record is raw bytes; server does not parse JSON.
  repeated bytes records = 1;
  uint64 first_record_index = 2; // monotonic within stream
  uint32 epoch = 3;
}

message EpochBoundary { uint32 completed_epoch = 1; }

message StreamError {
  enum Code {
    CODE_UNSPECIFIED = 0;
    MALFORMED_RECORD = 1;
    OVERSIZED_RECORD = 2;
    IO_ERROR = 3;
    SLOW_CONSUMER = 4;
    SERVER_SHUTDOWN = 5;
    INTERNAL = 6;
  }
  Code code = 1;
  string detail = 2;
  bool fatal = 3;               // if true, stream will close
}

message StreamClosed {
  uint64 total_records_sent = 1;
  uint32 epochs_completed = 2;
}

message HealthRequest {}
message HealthResponse {
  enum Status { UNKNOWN = 0; SERVING = 1; NOT_SERVING = 2; DRAINING = 3; }
  Status status = 1;
}
```

**Notes on the shape:**

- `package shuflr.v1` from day one. Breaking changes go to `v2`, never mutate `v1`.
- Bidi stream even though v1 only reads client `Cancel` — this avoids a wire-breaking change when we add `Ack`/`Pause`.
- `dataset_id` is server-configured; paths never cross the wire. This closes the exfiltration footgun in §7.
- `ResumeHint` is declared but **documented as best-effort in v1**. Clients see the field, can start planning against it, and v2 upgrades the guarantee without changing the proto.
- `RecordBatch.first_record_index` gives clients a real cursor to persist, which is how they will actually recover across crashes in v1.

## v1 Minimum Bar for "Deployable"

To call the service production-shippable, I want all of these:

1. Proto frozen as `shuflr.v1`, shape above (or equivalent), buf-lint clean.
2. gRPC over TLS; default bind is loopback / UDS; `--bind-public` gates non-loopback.
3. mTLS or `--auth-token-file`; no anonymous non-loopback binds, full stop.
4. Server-configured dataset catalog; no client-supplied paths.
5. Disconnect semantics documented: "ephemeral server, deterministic given `(seed, config, input_fingerprint)`; clients persist `first_record_index` to resume by skipping."
6. One pipeline per `(dataset_id, effective_config)`, ref-counted, torn down on last client leave. `--max-concurrent-streams`, `--max-concurrent-pipelines`.
7. Slow-consumer eviction with `StreamError{SLOW_CONSUMER}` after configurable lag.
8. Cancellation plumbed end-to-end: dropped RPC → tokio task cancel → crossbeam channel close → sync core exit → mmap drop. Tested with a disconnect chaos test.
9. Metric set from §6 exported; a Grafana dashboard JSON checked in.
10. Multi-client mode in v1 is **`independent` only**. `fanout` and `partition` explicitly deferred.
11. HTTP/2-SSE and HTTP/3 cut from v1 scope; said so in the doc.

## Open Questions

- Should `seed` be required or should the server assign one when omitted and echo it back in `StreamOpened`? (Leaning: assign-and-echo; it makes "give me *a* shuffle" the easy path without losing reproducibility.)
- `input_fingerprint` definition: `blake3(sorted(path, size, mtime))` or content-hash? Content-hash is correct but expensive on TB inputs; metadata-hash is cheap but can drift. Probably metadata-hash with a `--strict-fingerprint` opt-in.
- Do we want a `DrainAndReload` admin RPC for dataset swaps without dropping streams, or is SIGTERM + client reconnect acceptable? (I'd accept SIGTERM for v1.)
- Is `reservoir` mode even sensible over gRPC? It only makes sense for stdin inputs, which the service does not take. Consider rejecting `RESERVOIR` in `Stream` and making it CLI-only.
- Per-stream CPU/memory accounting: cgroups-per-pipeline is overkill; a coarse `max_records_per_sec` rate-limit per stream is probably the v1 answer.
