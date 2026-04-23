# shuflr — Design v2 (Revised)

**Status:** Authoritative v1 spec. Supersedes 001 on all conflicts.
**Date:** 2026-04-22
**Prior:** `docs/design/001-initial-plan.md` (iteration 1, now frozen)
**Informed by:** seven reviews in `docs/design/review-01/` (systems-perf, ML practitioner, distributed systems, Rust architect, security, correctness/stats, CLI-UX)

## 0. What Changed From v1

Headline decisions. Each has a rationale section later in this doc; cross-references to the reviews are in the appendix.

| # | Change | Rationale |
|---|---|---|
| 1 | Default I/O mode: `--pread` with a 2 MiB ring, **not** `--mmap` | mmap pollutes page cache and has unbounded RSS under the plan's `MADV_SEQUENTIAL` — RSS claim in 001 was false |
| 2 | Default chunk size: **16 MiB × 8 active = 128 MiB resident** (was 256 MiB × 8 = 2 GiB) | Matches kernel read-ahead window, fits L3, avoids NUMA drift |
| 3 | **Deterministic boundary reconciliation** replaces pure probabilistic jitter | A training-data tool needs coverage guarantees, not "recovered in expectation" |
| 4 | **`index-perm` promoted to v1** as the recommended training mode | `chunk-shuffled` does not unbias source-order locality; shipping only it would be dangerous for LLM training |
| 5 | **`buffer` mode split from `reservoir`** | Vitter Algorithm R emits exactly K records; a buffer shuffle emits all N. Conflating them was misleading |
| 6 | **Resumable cursors and `--rank`/`--world-size` promoted to v1** | Pre-emption-resilient training demands both; deferring was a fatal v1 gap for the stated use case |
| 7 | **Subcommands** (`stream` implicit, `serve`, `analyze`, `completions`) | `serve` was a flag that silently turned pipe mode into a daemon; bad UX |
| 8 | **Crate collapse: 4 → 2** (`shuflr` library + `shuflr-cli` binary) | Four crates before any code imposes cross-crate `pub` friction with no payoff |
| 9 | **Network binds loopback/UDS by default**; `--bind-public` gates TLS; no anonymous public binds | 001 had zero auth story; shipping that would be an exfiltration oracle |
| 10 | **Concrete `shuflr.v1` proto** replaces "`stream Record GetBatch(Request)`" handwave | v1 must commit to a wire contract or trainers can't depend on it |
| 11 | **HTTP/2 SSE and HTTP/3 cut from v1** | gRPC+TLS covers the use case; HTTP/2 SSE is niche; HTTP/3/QUIC is not production-clean with tonic |
| 12 | **`--filter` and `--project` cut** | Violated the stated non-goal ("not a JSON transformation tool"). Pipe to jq |
| 13 | **PRF hierarchy** (`master → {epoch, chunk, interleave, jitter, shard, client}`) | Enables skip-to-record resume without RNG replay; 001 had only `epoch_seed` |
| 14 | **Feature flags** on the binary (`grpc`, `prom`, `zstd`, `gzip`, `s3`, `gcs`, `uring`) | Zero features = ~400-crate build for `cargo install`; default stays lean |

What stayed the same: the chunk-shuffled core idea, seeded `ChaCha20Rng` everywhere, sync core + tokio-only-at-edge, bounded `crossbeam` channels, stdout-is-sacred, `thiserror` in lib / `anyhow` in bin.

---

## 1. Architecture Overview

```
┌──────────────────────────────────────────────────────────────────────┐
│  shuflr process                                                      │
│                                                                      │
│  Sync core (one OS thread per pipeline)                              │
│  ┌────────────┐   ┌────────────┐   ┌────────────┐   ┌────────────┐   │
│  │ Prefetcher │──▶│ Scanner    │──▶│ Shuffler   │──▶│ Emitter    │   │
│  │ pread/uring│   │ memchr →   │   │ Fisher-    │   │ writev /   │   │
│  │ 2 MiB ring │   │ offsets[]  │   │ Yates +    │   │ send_zc /  │   │
│  └────────────┘   └────────────┘   │ interleave │   │ tokio mpsc │   │
│                                    └────────────┘   └─────┬──────┘   │
│                                                           │          │
│  Async edge (tokio, only if serve subcommand)             │          │
│  ┌──────────────────────────────────────────────┐  ┌──────▼──────┐   │
│  │ tonic gRPC server                            │◀─│ bridge thrd │   │
│  │ pulls from tokio::sync::mpsc per client      │  │ blocking_   │   │
│  └──────────────────────────────────────────────┘  │ send()      │   │
│                                                    └─────────────┘   │
└──────────────────────────────────────────────────────────────────────┘
```

**Hard rules:**
- Sync core **never** calls `.await` or `block_on`. Writes go through `impl Write` passed by the sink, or through `handle.blocking_send` on a `tokio::sync::mpsc` owned by a dedicated bridge thread.
- Tokio handlers **never** call sync blocking code; they only `recv()` from the mpsc.
- One sync pipeline per `(dataset_id, effective_config)` equivalence class; reference-counted; torn down when the last client leaves.
- Bounded channels everywhere; back-pressure propagates prefetcher ← scanner ← shuffler ← emitter without explicit flow control.

## 2. Core Algorithms

### 2.1 `chunk-shuffled` — zero-prep default

Logically divide the file into `N = S / C` chunks of `C = 16 MiB` each; keep `M = 8` active. Per chunk:

1. Read via I/O layer (§4) into a 16 MiB buffer.
2. Scan with `memchr::memchr_iter(b'\n', &buf)` to build an offset array `Vec<u32>` of line starts (u32 ok because `C < 4 GiB`).
3. Fisher-Yates on `offsets` (seeded from `chunk_seed`, §3).
4. Register the chunk with the interleaver.

Emit by **weighted random interleave** across the M active chunks, weight ∝ remaining lines. When a chunk is exhausted, `munmap` or release the pread buffer, advance to the next chunk.

**Quality characterization — honest:**

> Not a uniform permutation. Output is a block-interleaved local shuffle: a line is displaced by at most ~`M · lines_per_chunk` positions from its origin (≈ 130k positions at defaults). Originally-adjacent lines co-occur at probability `~1/lines_per_chunk` (vs. `~2/N_lines` uniform) — roughly `N/2 · M` times the uniform baseline. **Inadequate for inputs sorted by any attribute** (document length, language, source, timestamp); pre-randomize once with `index-perm` or an external `terashuf` pass.

`shuflr analyze` (§6.4) reports chunk-local byte/token distribution divergence to warn users whose corpus looks sorted.

### 2.2 `index-perm` — provably uniform, recommended for training

One scan pass builds `offsets: Vec<u64>` (file byte offsets of every line start). Fisher-Yates on the whole array, then emit in shuffled order via `pread` of each line.

**RAM scaling (guidance):**

| Lines | Index RAM (8 bytes/line) |
|---|---|
| 10⁷ | 80 MB — trivial |
| 10⁹ | 8 GB — fits on a training box |
| 10¹⁰ | 80 GB — needs a large RAM host |
| 10¹¹ | 800 GB — use `2pass` instead (deferred to v1.x) |

Index is persisted to `<input>.shuflr-idx` (atomic write: `.tmp` + rename) so subsequent epochs start immediately. Format: `[magic:8][version:1][input_fingerprint:32][line_count:8][offsets:8*N]`, little-endian.

**Time-to-first-record:** indexed file ≈ 50 ms (re-stat + mmap index); unindexed ≈ `S / bandwidth` (one full sequential scan).

### 2.3 `buffer` — local shuffle, displacement-bounded

Ring buffer of size `K`. For each input record, if ring has a slot, enqueue; else swap-evict a random slot and emit the evicted record. At EOF, drain remainder in random order.

**Output:** exactly `N` records; every record displaced by at most `K` positions. Mirrors HuggingFace `IterableDataset.shuffle(buffer_size=K)`.

### 2.4 `reservoir` — sample exactly K records

Vitter's Algorithm R. Output is a uniform random K-subset of the input.

**This is a sampler, not a shuffler.** The flag is named accordingly; `buffer` is distinct.

### 2.5 `2pass` — external uniform shuffle (deferred to v1.x)

Jane Street / terashuf algorithm. Scatter to M pile files, shuffle each, concatenate. Cited for completeness; not in v1. Users needing provably uniform shuffle on files too large for `index-perm` must preprocess with `terashuf` or wait for v1.x.

### 2.6 `none` — pass-through

Serial read and emit. Fastest path; used with `--sample` for "head of file" or as a fast `cat` replacement with batching.

---

## 3. Determinism: PRF Hierarchy

All randomness derives from `--seed: u64` via a tree keyed by BLAKE3. Any sub-computation is addressable without RNG replay.

```
master_seed
├── epoch_seed(e)        = blake3(master_seed ‖ "epoch" ‖ e)
│   ├── chunk_seed(c)    = blake3(epoch_seed ‖ "chunk" ‖ c)       → intra-chunk Fisher-Yates
│   ├── interleave_seed  = blake3(epoch_seed ‖ "interleave")      → weighted-random interleave
│   ├── jitter_seed      = blake3(epoch_seed ‖ "jitter")          → boundary jitter (§5)
│   └── perm_seed        = blake3(epoch_seed ‖ "perm")            → index-perm Fisher-Yates
├── shard_seed(r, W)     = blake3(master_seed ‖ "shard" ‖ r ‖ W)  → --rank r of --world-size W
└── client_seed(cid)     = blake3(master_seed ‖ "client" ‖ cid)   → serve-mode independent streams
```

The ChaCha20Rng for any node is `ChaCha20Rng::from_seed(derived[0..32])`. This enables:
- Exact resume: given `(master_seed, epoch, chunk, intra_chunk_pos)`, seek directly to the next record without replaying prior chunks.
- Per-rank disjoint partitions that remain deterministic across restarts.
- Independent client streams that are still reproducible (logged `client_seed` ⇒ replayable).

---

## 4. I/O Layer

### 4.1 Access modes

| Mode | When | Mechanism |
|---|---|---|
| `--io=pread` (default) | All files, all platforms | 16-entry ring of 2 MiB aligned buffers; `pread` or `io_uring` submit; scan overlapped with next-chunk read |
| `--io=mmap` | Small files (< 256 MiB) or debugging | `memmap2::Mmap` + `madvise(MADV_SEQUENTIAL \| MADV_WILLNEED)`; **mandatory `MADV_DONTNEED` on chunk retire** |
| `--io=direct` | Linux ≥ 5.13 NVMe, opt-in | `io_uring` + `O_DIRECT` + registered buffers + `IORING_SETUP_SQPOLL`; bypasses page cache; reads sector-aligned |

**Block size:** `--io-block=2MiB` default. Reads are `pread(fd, buf, 2MiB, offset)` regardless of shuffle chunk size. The shuffle chunk (16 MiB) is filled by 8 block reads.

**RSS bound** (as specified) holds only in `--io=pread` or `--io=direct`. Under `--io=mmap`, `MADV_DONTNEED` on retire is mandatory and unconditionally called.

### 4.2 OS support matrix

| Platform | `pread` | `mmap`+`MADV_DONTNEED` | `posix_fadvise` | `O_DIRECT` | `io_uring` | `sendfile` | `MSG_ZEROCOPY` |
|---|---|---|---|---|---|---|---|
| Linux ≥ 5.13 | ✓ | ✓ | ✓ | ✓ | ✓ (opt-in) | ✓ | ✓ |
| Linux < 5.13 | ✓ | ✓ | ✓ | ✓ | (basic) | ✓ | — |
| macOS | ✓ | ✓ (via `MADV_FREE_REUSABLE`) | — | `F_NOCACHE` substitute | — | — | — |
| Windows | best-effort | ✓ | — | `FILE_FLAG_NO_BUFFERING` | `FILE_FLAG_OVERLAPPED` | — | — |

Windows is **best-effort only** in v1 (tests may be skipped). Linux is the tier-1 platform; macOS is tier-2.

### 4.3 Zero-copy output (feature-gated)

- `--output-zc` (Linux ≥ 6.0, `uring` feature): `IORING_OP_SEND_ZC` with registered send buffers.
- Fallback: `writev` on `io::stdout().lock()` with iovecs pointing into the scan buffers.
- `sendfile` is useless for shuffled output (non-contiguous). Documented explicitly.

### 4.4 Compression (deferred to v1.1)

When compression lands:
- **Format:** Facebook `zstd_seekable` frame-indexed, 2 MiB frames. No arbitrary `.gz` / `.zst` support — only seekable variants. The CLI will error out with a clear message on unseekable compressed input.
- **Cost:** 3–5% compression-ratio penalty vs. monolithic zstd level 3; one decoder thread per active chunk.
- **Invariant:** chunk boundaries and boundary reconciliation ranges must align to frame boundaries.

### 4.5 File identity and mutation

At open time, record `(st_dev, st_ino, st_size, st_mtime)` as the **input fingerprint** (a tuple, blake3-hashed for wire/log emission). Before every chunk (re)read, re-stat and compare. On divergence, emit `IoError::InputChanged` and exit (or in serve mode, close the stream cleanly).

**SIGBUS handler** (installed unconditionally when any mmap is held): async-signal-safe, `siglongjmp` to a per-thread jmp_buf set around mmap reads. On catch, emit `MappedFileTruncated` and close cleanly rather than segfaulting. Optional `--require-immutable` flag: `fstat` at start, refuse to run if file is writable by the current uid (training pipelines should serve from read-only snapshots).

---

## 5. Boundary Reconciliation

A line straddling a chunk boundary is lost to naive chunk reads. 001 relied on probabilistic jitter across epochs; this was inadequate for training workloads.

**v1 behavior:** deterministic reconciliation. For each interior boundary `k`:

1. After loading chunks `k` and `k+1`, `pread` up to `max_line` bytes (default 16 MiB cap) past the nominal boundary.
2. Locate the first complete line whose start offset lies in chunk `k`'s byte range and whose end lies in chunk `k+1`.
3. Emit it at a PRF-determined interleave position (`jitter_seed(k)`).

Cost: one extra `pread` of ≤ `max_line` bytes per interior boundary per epoch. Worst case `N · max_line` = 6.4 GB at defaults on a 100 GB file; almost always cache-warm because the next chunk's prefetch has already fetched these bytes.

**Jitter is still applied** as a position offset within the epoch's interleave order — it distributes reconciled-line placement but is no longer responsible for coverage. Coverage is deterministic: every line in the input appears in every epoch's output.

---

## 6. CLI Surface

### 6.1 Subcommands

| Command | Purpose |
|---|---|
| `shuflr [OPTIONS] [INPUT...]` | Default: `stream` to stdout (implicit subcommand) |
| `shuflr stream [...]` | Explicit form of default |
| `shuflr serve [...]` | Long-lived gRPC / UDS service |
| `shuflr analyze [...]` | Scan + report chunk-local distribution divergence; warn if chunk-shuffled would be unsafe |
| `shuflr index [...]` | Build `.shuflr-idx` file explicitly (for `index-perm` mode) |
| `shuflr verify [...]` | Validate JSONL framing; exit 65 on malformed |
| `shuflr completions <shell>` | Emit completion script for bash/zsh/fish/powershell |

### 6.2 Flag set — v1 primary

```
POSITIONAL
  [INPUT...]                 file(s) or glob; '-' or omit for stdin

COMMON
  -s, --shuffle MODE         none | chunk-rr | chunk-shuffled | index-perm
                             | buffer[:K] | reservoir:K
                             default: chunk-shuffled for files, buffer:100000 for stdin
      --seed U64             reproducibility (or $SHUFLR_SEED)
  -n, --sample N             stop after N records (per epoch)
  -e, --epochs N|inf         passes over input                           [1]
      --rank R --world-size W   distributed partitioning: emit disjoint 1/W share
      --resume TOKEN         resume from a cursor token (see §7.3)
      --progress[=WHEN]      never | auto | always                       [auto]

TUNING
      --chunk-size BYTES     per-shuffle-chunk                           [16MiB]
      --active-chunks M      concurrent chunks                           [8]
      --io MODE              pread | mmap | direct                       [pread]
      --io-block BYTES       pread size                                  [2MiB]
      --batch-size N         records per emit batch                      [1 stdout, 128 network]
      --on-error POLICY      skip | fail | passthrough                   [skip]
      --max-line BYTES       per-record size cap                         [16MiB]
      --require-immutable    refuse to run on writable input

MODES (mutually exclusive)
      --dry-run              scan + report, emit nothing
      --verify               full validation pass

DIAGNOSTIC
      --log-level LEVEL      error|warn|info|debug|trace (or $SHUFLR_LOG)  [info]
  -h, --help                 short help
      --help-full            full flag set including serve
  -V, --version
```

### 6.3 `serve` subcommand flags

```
shuflr serve [OPTIONS] [INPUT...]

  --grpc ADDR                gRPC listener (grpc+tcp://[::1]:50051 default; grpc+unix:///path)
  --bind-public              allow non-loopback bind (required for non-loopback --grpc)
  --tls-cert PATH            PEM cert (required with --bind-public)
  --tls-key PATH             PEM key (required with --bind-public)
  --tls-client-ca PATH       enable mTLS by requiring client certs from this CA
  --auth-token-file PATH     shared-secret bearer auth (non-loopback requires --tls)
  --max-connections N        process-wide cap                            [64]
  --max-streams-per-peer N                                               [4]
  --idle-timeout DUR                                                     [30s]
  --handshake-timeout DUR                                                [5s]
  --dataset ID=PATH          map dataset_id to server-local path (repeatable)
  --stats ADDR               Prometheus listener (loopback default; same rules as --grpc)
```

**Security defaults, non-negotiable:**
- No `--grpc 0.0.0.0:…` without `--bind-public`.
- No `--bind-public` without `--tls-cert`/`--tls-key`.
- Clients never transmit filesystem paths. They reference `dataset_id`s from a server-configured catalog (`--dataset`).

### 6.4 `analyze` output

```
$ shuflr analyze corpus.jsonl
shuflr analyze: 842 GiB, ~1.3B records, 53896 chunks of 16 MiB

distribution checks (sampling 32 chunks):
  byte-frequency KL vs. global:  0.002 nats   (uniform)
  record-length-percentile drift: +3%         (uniform)
  top-1 token prefix KL:          0.014 nats  (uniform)

verdict: chunk-shuffled is safe; no source-order locality detected.

$ shuflr analyze sorted-by-length.jsonl
...
  byte-frequency KL vs. global:  0.003 nats   (uniform)
  record-length-percentile drift: +91%        (highly skewed)
  top-1 token prefix KL:          0.008 nats  (uniform)

verdict: chunk-shuffled UNSAFE. Recommend one-time index-perm via:
    shuflr index sorted-by-length.jsonl
then use --shuffle=index-perm for subsequent runs.
```

Exit 0 on safe, exit 3 on unsafe (scripts can gate on this).

### 6.5 Exit codes (sysexits.h)

| Code | Condition |
|---|---|
| 0 | Success |
| 3 | `analyze` verdict unsafe |
| 64 | EX_USAGE — flag parse failure |
| 65 | EX_DATAERR — `--verify` or `--on-error=fail` caught malformed JSON |
| 66 | EX_NOINPUT — input file missing |
| 73 | EX_CANTCREAT — bind failed |
| 77 | EX_NOPERM — permission denied on read or bind |

No Rust backtrace in normal output. Messages go to stderr prefixed `shuflr:`.

### 6.6 Env vars

- `SHUFLR_SEED` — equivalent to `--seed`
- `SHUFLR_LOG` — tracing-subscriber `EnvFilter` syntax
- `NO_COLOR` — respected
- `RUST_BACKTRACE` — internal only; never instructed

No `.shuflrrc`. Revisit for v2 if `serve` grows persistent deployment needs.

### 6.7 Cookbook (dogfooding checklist)

```bash
# 1. Pipe shuffled corpus into jq
shuflr corpus.jsonl | jq -r '.text' > texts.txt

# 2. Reproducible 10k sample
shuflr --seed 42 --sample 10000 corpus.jsonl > sample.jsonl

# 3. Infinite-epoch stream for training
shuflr --epochs inf --seed 7 corpus.jsonl | ./train.py

# 4. Distributed training, rank 3 of 8
shuflr --rank 3 --world-size 8 --seed 42 --epochs inf corpus.jsonl | ./train.py

# 5. Shuffle stdin from a generator
zstdcat big.jsonl.zst | shuflr --shuffle buffer:200000

# 6. Persisted uniform shuffle for repeated epochs
shuflr index corpus.jsonl              # writes corpus.jsonl.shuflr-idx
shuflr --shuffle=index-perm --epochs 3 --seed 11 corpus.jsonl

# 7. Serve to remote trainers (sidecar, loopback)
shuflr serve --grpc grpc+unix:///tmp/shuflr.sock --dataset main=/data/corpus.jsonl

# 8. Serve over mTLS on a shared dataset node
shuflr serve --grpc grpc+tcp://0.0.0.0:50051 --bind-public \
    --tls-cert server.pem --tls-key server.key --tls-client-ca clients-ca.pem \
    --dataset main=/data/corpus.jsonl --max-connections 32

# 9. Safety check before committing to chunk-shuffled
shuflr analyze corpus.jsonl || shuflr index corpus.jsonl

# 10. Quick smoke test
shuflr --dry-run corpus.jsonl
```

---

## 7. Service Surface (`serve` subcommand)

### 7.1 Proto contract (`shuflr.v1`)

```proto
syntax = "proto3";
package shuflr.v1;

service Shuflr {
  rpc ListDatasets(ListDatasetsRequest) returns (ListDatasetsResponse);
  rpc Stream(stream StreamClientMessage) returns (stream StreamServerMessage);
  rpc Health(HealthRequest) returns (HealthResponse);
}

message ListDatasetsRequest {}
message ListDatasetsResponse {
  repeated DatasetInfo datasets = 1;
}
message DatasetInfo {
  string dataset_id = 1;
  string input_fingerprint = 2;  // blake3 hex of (path, size, mtime) or content, per server policy
  uint64 approx_records = 3;
  uint64 approx_bytes = 4;
}

message StreamClientMessage {
  oneof kind {
    OpenStream open = 1;
    Cancel cancel = 2;
    // Reserved (wire-compat): Ack, Pause, Resume, RateHint.
  }
}
message OpenStream {
  string dataset_id = 1;
  uint64 seed = 2;                     // 0 = server assigns and echoes
  ShuffleConfig config = 3;
  ResumeHint resume = 4;               // best-effort in v1
  uint32 max_batch_records = 5;        // client hint
  uint32 max_batch_bytes = 6;
  uint32 rank = 7;                     // 0 if --world-size unset
  uint32 world_size = 8;               // 0 = single-rank
  string client_id = 9;                // stable per-client ID for client_seed derivation
}
message ShuffleConfig {
  enum Mode {
    MODE_UNSPECIFIED = 0;
    NONE = 1;
    CHUNK_RR = 2;
    CHUNK_SHUFFLED = 3;
    INDEX_PERM = 4;
    BUFFER = 5;
    // RESERVOIR intentionally not exposed over gRPC; CLI-only (per distributed-systems review)
  }
  Mode mode = 1;
  uint64 chunk_size_bytes = 2;  // 0 = server default
  uint32 active_chunks = 3;     // 0 = server default
  uint32 buffer_size = 4;       // for mode=BUFFER
  uint32 epochs = 5;            // 0 = infinite
}
message ResumeHint {
  uint32 epoch = 1;
  uint64 records_already_seen = 2;   // v1 server skips this many records; not cryptographically verified
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
  string stream_id = 1;
  string input_fingerprint = 2;
  uint64 effective_seed = 3;
  ShuffleConfig effective_config = 4;
}
message RecordBatch {
  repeated bytes records = 1;
  uint64 first_record_index = 2;   // monotonic within stream
  uint32 epoch = 3;
  // Records are raw bytes. Server does not parse JSON.
}
message EpochBoundary {
  uint32 completed_epoch = 1;
  uint64 records_this_epoch = 2;
  uint64 boundary_reconciled = 3;
}
message StreamError {
  enum Code {
    CODE_UNSPECIFIED = 0;
    MALFORMED_RECORD = 1;
    OVERSIZED_RECORD = 2;
    IO_ERROR = 3;
    SLOW_CONSUMER = 4;
    SERVER_SHUTDOWN = 5;
    AUTH_DENIED = 6;
    RESOURCE_EXHAUSTED = 7;
    INTERNAL = 8;
  }
  Code code = 1;
  string detail = 2;
  bool fatal = 3;
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

### 7.2 Multi-client semantics

**v1 ships `independent` only.** Each client stream gets its own pipeline keyed by `(dataset_id, effective_config, client_seed(client_id))`. Pipelines with the same config share upstream reads via a ref-counted `ChunkPool`, but each client has its own shuffle RNG.

`fanout` and `partition` are **explicitly deferred.** The `--rank`/`--world-size` pattern covers distributed training without requiring server-side coordination.

### 7.3 Disconnect and resume semantics

**The server is ephemeral.** State is `(seed, config, input_fingerprint, client_id)`; given identical inputs, the server emits identical output. Clients persist `RecordBatch.first_record_index` and reconnect with `ResumeHint{epoch, records_already_seen}`.

- v1: `ResumeHint` is **best-effort** — server skips `records_already_seen` records. Not cryptographically verified.
- v1.1: opaque, MAC'd resume tokens via XChaCha20-Poly1305 with a server-side key (per security review L4). Prevents a client from handing back *another* stream's token.

On client disconnect, the pipeline's ref-count decrements; when zero (and after `--idle-timeout`), the pipeline is torn down, mmaps dropped, file handles closed.

On server crash/restart, all clients see `UNAVAILABLE`; they reconnect with the same `(seed, config, dataset_id, client_id)` and their own persisted `first_record_index`.

### 7.4 Backpressure and slow-consumer eviction

HTTP/2 flow control back-pressures naturally through the tokio mpsc → bridge thread → sync core channel. When a client's buffered-but-unacked bytes exceed `--max-slow-consumer-lag-bytes` (default 256 MiB) for longer than `--slow-consumer-timeout` (default 30s), the stream is closed with `StreamError{RESOURCE_EXHAUSTED}`.

### 7.5 Observability

**Tracing:** one root span per RPC, tagged with `{stream_id, seed, client_id, input_fingerprint}`. Child spans for chunk loads, scans, shuffle passes. OTLP export behind a `otlp` feature flag.

**Metrics** (Prometheus, `--stats` endpoint):

| Metric | Type | Labels |
|---|---|---|
| `shuflr_rpc_duration_seconds` | histogram | `rpc`, `code` |
| `shuflr_stream_records_sent_total` | counter | `dataset_id` |
| `shuflr_stream_client_lag_bytes` | gauge | `dataset_id` |
| `shuflr_chunk_rotation_duration_seconds` | histogram | `dataset_id` |
| `shuflr_mmap_active_bytes` | gauge | — |
| `shuflr_channel_depth` | gauge | `stage` ∈ `{prefetch, scan, shuffle, emit}` |
| `shuflr_errors_total` | counter | `kind` ∈ `{malformed,oversized,io,slow_consumer,auth,cancelled,internal}` |
| `shuflr_pipelines_active` | gauge | — |
| `shuflr_connections_active` | gauge | — |
| `shuflr_boundary_reconciled_total` | counter | `dataset_id` |

**Label cardinality cap:** `dataset_id` values come from the server's catalog (bounded); `client_id` never labels metrics (high-cardinality attack surface). Per-client stats are tracing-only.

### 7.6 Deployment topologies

**A. Training sidecar (primary, 80% of usage).** One `shuflr serve` per training node, UDS socket, `--grpc grpc+unix:///tmp/shuflr.sock`. Trainer ranks on the same node connect over UDS. No TLS, no auth (loopback). One dataset per sidecar. This covers LLM pretraining, finetuning, distillation — the canonical case.

**B. Shared dataset node.** One `shuflr serve` per dataset host, mTLS on a private network (`--tls-cert/--tls-key/--tls-client-ca`). Multiple training jobs across the cluster connect as clients; one pipeline per `(dataset_id, effective_config)` ref-counted across clients. Useful for teams sharing a corpus without replicating it to every training node.

**Explicitly unsupported:** public-internet binds. The CLI refuses `--bind-public` without TLS and has no mode that serves an anonymous public endpoint.

---

## 8. Distributed Training Integration

### 8.1 CLI mode with `--rank`/`--world-size`

Each of `W` ranks runs its own `shuflr` process against the same file:

```bash
# rank r of W
shuflr --rank $RANK --world-size $WORLD_SIZE --seed 42 --epochs inf corpus.jsonl | ./train.py
```

Partitioning: each rank emits chunks where `(chunk_id ⊕ shard_seed(r, W)) mod W == r`. Intra-chunk shuffle uses `chunk_seed` derived from the full master_seed — so ranks see different records but the *underlying permutation* is deterministic.

**Disjointness guarantee:** `∀ r ≠ r', emitted(r) ∩ emitted(r') = ∅` (per epoch). Property-tested.

### 8.2 Python client example

Shipped in `examples/pytorch/shuflr_dataset.py`:

```python
import grpc, json, os
from torch.utils.data import IterableDataset
from shuflr.v1 import shuflr_pb2, shuflr_pb2_grpc

class ShuflrDataset(IterableDataset):
    def __init__(self, channel, dataset_id, seed, world_size=1, rank=0,
                 client_id=None, resume_index=0):
        self.channel = channel
        self.dataset_id = dataset_id
        self.seed = seed
        self.world_size = world_size
        self.rank = rank
        self.client_id = client_id or f"pid-{os.getpid()}"
        self.resume_index = resume_index

    def __iter__(self):
        stub = shuflr_pb2_grpc.ShufflrStub(self.channel)
        def open_msg():
            yield shuflr_pb2.StreamClientMessage(open=shuflr_pb2.OpenStream(
                dataset_id=self.dataset_id,
                seed=self.seed,
                rank=self.rank, world_size=self.world_size,
                client_id=self.client_id,
                resume=shuflr_pb2.ResumeHint(records_already_seen=self.resume_index),
                config=shuflr_pb2.ShuffleConfig(
                    mode=shuflr_pb2.ShuffleConfig.INDEX_PERM,
                    epochs=0,  # infinite
                ),
            ))
        for msg in stub.Stream(open_msg()):
            if msg.WhichOneof("kind") == "batch":
                for rec_bytes in msg.batch.records:
                    yield json.loads(rec_bytes)
```

**DataLoader contract:** use `num_workers=0`. gRPC streams and DataLoader worker forks do not mix. Document this loudly.

### 8.3 Resume contract

Clients should persist `first_record_index` of the last batch they processed. On restart, pass it as `ResumeHint.records_already_seen`. With `INDEX_PERM`, resume is exact. With `CHUNK_SHUFFLED`, resume is exact too — the PRF hierarchy lets the server skip to the right chunk and intra-chunk offset without RNG replay.

With `BUFFER`, exact resume is **not possible** in v1 (the buffer state is too large). Document it; warn on `ResumeHint` + `BUFFER`.

---

## 9. Security Posture

### 9.1 Threat model

- **Adversary A (corpus producer):** non-malicious but buggy upstream. Malformed JSONL, truncated files, symlinks into `/proc`, oversized lines.
- **Adversary B (network peer):** same VPC/k8s namespace. Wants to read data or DoS the server.
- **Adversary C (supply chain):** yanked, typo-squatted, or maliciously updated crate.
- **Assets:** corpus bytes (often PII-containing); filesystem read surface of the process; liveness.
- **Trust boundaries:** input bytes → parser; socket → service; process → unsafe mmap; release artifact → operator.

### 9.2 Defaults checklist

- Listeners bind `127.0.0.1` / UDS `0600` by default.
- `--bind-public` refused without `--tls-cert`/`--tls-key`.
- Non-loopback bearer auth refused without TLS.
- Input files: regular-files-only by default (reject `S_IFLNK`, `S_IFCHR`, `S_IFBLK`, `S_IFIFO`, `S_IFSOCK`). Open with `O_NOFOLLOW | O_CLOEXEC`; `fstat` the fd (not the path) before mmap.
- Globs refuse `..` after canonicalization; refuse filesystem crossings unless `--cross-fs`.
- `--validate-utf8=strict` default; leading BOM stripped once per file; embedded NULs subject to `--on-error` policy; CRLF normalized to LF when `--framing=lf` (default).
- `--max-line=16MiB` default. `--verify` additionally caps JSON nesting depth at 128.
- `--max-connections=64`, `--max-streams-per-peer=4`, `--idle-timeout=30s`, `--handshake-timeout=5s`.
- SIGBUS handler installed; `--require-immutable` available.
- Error logs carry `{byte_offset, length, blake3_hex, reason}` only. **Never** record bytes.

### 9.3 Supply chain

Ship `deny.toml` enforcing: licenses ∈ {MIT, Apache-2.0, BSD-3-Clause, Unicode-DFS-2016, ISC}; `advisories.vulnerability = "deny"`; `advisories.yanked = "deny"`; `bans.multiple-versions = "warn"`; `sources.unknown-registry = "deny"`; `sources.unknown-git = "deny"`.

CI: `cargo deny check` + `cargo audit` on every PR. Nightly rerun against main. `Cargo.lock` committed (binary crate). Release builds use `cargo auditable` to embed SBOM. Artifacts signed with Sigstore/cosign; install docs point at verification, not `curl | sh`.

Adopt `cargo-vet` with a trust set: `rust-lang`, `dtolnay`, `tokio-rs`, `BurntSushi`, `Amanieu`. Widen deliberately.

### 9.4 `unsafe` code policy

- `#![deny(unsafe_code)]` at the top of every module **except** `shuflr::io::mmap` and `shuflr::io::uring` (and their direct descendants).
- Those modules carry a top-of-file `//! Invariants:` block.
- Every `unsafe` block has a `// SAFETY:` comment referencing the invariant that justifies it.
- CI grep asserts every `unsafe {` is followed within two lines by `// SAFETY:`.
- `cargo-geiger` tracked in CI; surface-area regressions blocked.
- `CODEOWNERS` names a reviewer for `crates/shuflr/src/io/`.

### 9.5 Sandboxing (optional, off by default)

`--sandbox` (Linux): Landlock ruleset restricting reads to the input path(s), no writes anywhere; seccomp-bpf filter dropping `openat` after the open phase. A test matrix exercises it; not enabling it is fine, but enabling a broken sandbox would be worse than none — gate behind explicit opt-in.

---

## 10. Crate Layout and Feature Flags

### 10.1 Revised layout — two crates

```
shuflr/
├── Cargo.toml                     # workspace: 2 members
├── crates/
│   ├── shuflr/                    # library (all engine code)
│   │   ├── src/
│   │   │   ├── lib.rs
│   │   │   ├── error.rs           # pub enum Error, re-exported
│   │   │   ├── seed.rs            # PRF hierarchy
│   │   │   ├── framing.rs         # memchr-based record iteration over &[u8]
│   │   │   ├── shuffle/           # chunk_shuffled, index_perm, buffer, reservoir, none
│   │   │   ├── io/                # mmap, pread, uring; ByteSource trait
│   │   │   ├── sink/              # stdout writev, tcp, uds, grpc (feature)
│   │   │   ├── pipeline.rs        # prefetch → scan → shuffle → emit
│   │   │   └── service/           # gRPC server glue (feature = grpc)
│   │   └── benches/throughput.rs
│   └── shuflr-cli/                # binary: clap, anyhow, wiring
│       └── src/main.rs
└── docs/design/
```

`chunked-shuffle` may be extracted as a standalone crate once the algorithm is stable (six-month rule).

### 10.2 Feature flags

```toml
# crates/shuflr/Cargo.toml
[features]
default = []
grpc    = ["dep:tonic", "dep:prost", "dep:tokio"]
prom    = ["dep:prometheus", "dep:metrics", "dep:metrics-exporter-prometheus"]
otlp    = ["dep:opentelemetry-otlp"]
uring   = ["dep:io-uring"]                # Linux only
zstd    = ["dep:zstd"]                    # deferred v1.1
gzip    = ["dep:flate2"]                  # deferred v1.1
s3      = ["dep:aws-sdk-s3"]              # deferred v1.1
gcs     = ["dep:google-cloud-storage"]    # deferred v1.1
sandbox = ["dep:landlock", "dep:seccompiler"]  # Linux only
```

`shuflr-cli` selects the features it needs. `cargo install shuflr --features grpc,prom` is the canonical serve build; default `cargo install shuflr` builds only the stream subcommand.

CI matrix runs `cargo hack --each-feature check` to catch unused deps and feature combinations.

### 10.3 Error shape

```rust
// shuflr::error
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("io: {0}")]           Io(#[from] std::io::Error),
    #[error("framing: {0}")]      Framing(#[from] framing::Error),
    #[error("shuffle: {0}")]      Shuffle(#[from] shuffle::Error),
    #[error("input changed: {0}")] InputChanged(String),
    #[error("mmap truncated")]    MappedFileTruncated,
    #[error("oversized record: {len} > max_line {cap}")] OversizedRecord { len: u64, cap: u64 },
}
pub type Result<T> = std::result::Result<T, Error>;
```

`anyhow::Error` appears only in `shuflr-cli`.

### 10.4 Sync-async bridge (pinned pattern)

```rust
// service/stream.rs — gRPC handler
async fn stream(&self, req: Request<Streaming<StreamClientMessage>>)
    -> Result<Response<ReceiverStream<StreamServerMessage>>>
{
    let (tx, rx) = tokio::sync::mpsc::channel(64);
    let handle = tokio::runtime::Handle::current();
    // Dedicated OS thread drives the sync pipeline. It calls
    // handle.block_on(tx.send(msg)) — the only place this is allowed.
    std::thread::spawn(move || {
        let pipeline = build_pipeline(...);  // sync
        for msg in pipeline {
            if handle.block_on(tx.send(msg)).is_err() { break; } // client gone
        }
    });
    Ok(Response::new(ReceiverStream::new(rx)))
}
```

The **only** place `block_on` is permitted is the bridge thread; enforced by a CI grep. The sync core never sees tokio.

### 10.5 Lint posture

```toml
[workspace.lints.rust]
unsafe_op_in_unsafe_fn = "warn"
unused_must_use = "deny"
missing_docs = "warn"

[workspace.lints.clippy]
unwrap_used = "warn"
expect_used = "warn"
dbg_macro = "warn"
print_stdout = "warn"       # stdout is the data channel
print_stderr = "allow"      # tracing goes here
needless_collect = "warn"
redundant_clone = "warn"
manual_let_else = "warn"
```

Each `lib.rs` / `main.rs` starts with:

```rust
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]
```

Data-channel writes **never** use `print!`/`println!`. Sinks take `impl Write`; the CLI passes `io::stdout().lock()`. This makes the lint free of noise and the sinks trivially testable against `Vec<u8>`.

---

## 11. Testing Strategy

### 11.1 Proptest properties (v1)

1. **Determinism.** `run(input, seed, cfg) == run(input, seed, cfg)` byte-for-byte over ≥ 100 `(input, seed)` pairs.
2. **Permutation (index-perm).** `multiset(output) == multiset(input)`; exact length; no dupes; no drops.
3. **Coverage (chunk-shuffled + deterministic reconciliation).** `∀ epoch: multiset(output_epoch) == multiset(input)`.
4. **Disjoint ranks.** `∀ r ≠ r' ∈ [0, W): emitted(r) ∩ emitted(r') = ∅` at fixed seed.
5. **Epoch independence.** Kendall tau between outputs at epochs `e₁ ≠ e₂` exceeds a threshold with probability ≥ 1 - ε.
6. **Intra-chunk uniformity.** Empirical distribution of a line's output slot within its chunk is uniform (chi-squared, p > 0.01).
7. **Adjacency bias.** For fixed `c`, `N`, `M`, empirical adjacent-pair probability matches closed-form `~1/c` within ±10% over 10k seeds.
8. **Seed-tree isolation.** Changing `chunk_seed(k)` alters only chunk `k`'s emission; all others byte-identical.
9. **Vitter R uniformity.** Output size exactly K; each input line included with probability `K/N` (K-S).
10. **Buffer displacement bound.** Output position of every record is within `K` of its input position. Hard bound.

### 11.2 Edge cases (explicit test matrix)

| Case | Behavior |
|---|---|
| 0-byte file | Empty output, exit 0 |
| 1-line file, no trailing `\n` | Emit line; warn under `--verify` |
| File smaller than `--chunk-size` | Single-chunk Fisher-Yates; no interleave; no warn |
| Line length ≥ `--chunk-size` but ≤ `--max-line` | Emit intact from origin chunk; reconciliation handles boundary straddle |
| Line length > `--max-line` | Skip per `--on-error` policy |
| CRLF endings | `\r` stripped under default `--framing=lf`; `--framing=raw` preserves |
| Embedded NULs | Pass through; never null-terminate internally |
| File grows during read | Snapshot size at open; ignore tail |
| File truncated during read | SIGBUS handler → `MappedFileTruncated` → clean exit |
| Symlink at input path (no `--follow-symlinks`) | Refuse, EX_USAGE |
| `--input /dev/zero` (no `--allow-devices`) | Refuse, EX_USAGE |

### 11.3 Fuzz targets (cargo-fuzz)

- `framing::parse` over arbitrary bytes (JSONL delimiter handling)
- `verify::parse_json` (bounded depth)
- `index::parse` (the `.shuflr-idx` file format)
- `proto::decode` (gRPC message deserialization — feature = grpc)

### 11.4 Chaos / integration tests

- `tests/disconnect.rs` — open gRPC stream, pull N batches, kill client, assert server drops pipeline, no leaked mmaps (`/proc/self/maps` diff).
- `tests/file_mutation.rs` — serve a file, truncate it under the serve process, assert SIGBUS handler catches and closes stream cleanly.
- `tests/slow_consumer.rs` — throttle reads; assert eviction at `--max-slow-consumer-lag-bytes` with `RESOURCE_EXHAUSTED`.
- `tests/rank_disjoint.rs` — run 8 `--rank` processes in parallel; collect outputs; assert disjoint.

### 11.5 Benchmarks (criterion)

- `throughput_passthrough` — `--shuffle=none`, `--io=pread`; target ≥ 2 GB/s single-thread.
- `throughput_chunk_shuffled` — target ≥ 1 GB/s at defaults.
- `throughput_index_perm` — target ≥ 400 MB/s emit (seek-bound).
- `ttfb_chunk_shuffled` — target ≤ 50 ms on a 100 GB file.
- `scan_cold_vs_warm` — quantifies prefetch budget (cold pages vs. in-cache).

Regressions block PRs; a 5% throughput drop requires a written waiver.

---

## 12. MSRV and Platform Policy

**Rust:** edition 2024, resolver 3, MSRV = latest stable. Policy: users are expected to have `rustup`. Distro-shipped stable rustc on bookworm (1.63) and jammy (1.75) is not supported. README.md states this in a "Requirements" section; CI enforces via `cargo +stable check` and `cargo +MSRV_PINNED check` against the declared MSRV.

**Platforms:**
- **Tier 1 — Linux x86_64, aarch64.** Full test matrix. `uring` feature builds only here.
- **Tier 2 — macOS arm64.** Full test matrix minus io_uring, O_DIRECT, Landlock/seccomp.
- **Tier 3 — Windows x86_64.** Best-effort. `shuflr stream` expected to work; `shuflr serve` untested; no perf claims.

---

## 13. v1 Scope Summary

**Ships in v1:**

- `stream` subcommand with `chunk-shuffled` (default), `index-perm`, `buffer`, `reservoir`, `chunk-rr`, `none` modes
- `index`, `verify`, `analyze`, `completions` subcommands
- `serve` subcommand with `shuflr.v1` gRPC proto, `independent` multi-client, mTLS, bearer-token auth, loopback default
- `--rank`/`--world-size` CLI partitioning
- Resumable cursors (best-effort; MAC'd tokens in v1.1)
- PRF hierarchy for addressable determinism
- Deterministic boundary reconciliation
- `pread` default I/O, `mmap` opt, `direct`(uring) opt behind `uring` feature
- SIGBUS handler, file-identity tracking, `--require-immutable`
- Prometheus metrics (via `prom` feature), tracing spans, OTLP (via `otlp` feature)
- Man page + shell completions (via `clap_mangen` + `clap_complete`)
- Feature-flagged binary size control
- PyTorch `IterableDataset` example

**Explicitly deferred (with rationale):**

| Feature | Defer to | Why |
|---|---|---|
| `2pass` external shuffle | v1.x | `index-perm` covers up to ~10¹⁰ lines; very-large corpora users have terashuf today |
| Seekable zstd input | v1.1 | Compression format choice is final (`zstd_seekable` 2 MiB frames); implementation is non-trivial |
| Object storage (s3, gs) | v1.1 | Byte-range HTTP semantics are uniform; can land after I/O layer stabilizes |
| HTTP/2 SSE, HTTP/3 | never / v2 | gRPC+TLS covers the use case; HTTP/3 tonic story is not production-clean |
| `--fanout` broadcast | v1.x | Distributed training uses `--rank`/`--world-size`; fanout is for observability/debugging |
| `--partition` shared iterator | v1.x or never | It's a distributed queue, not a shuffler |
| Landlock/seccomp sandbox | v1.1 | Optional, off-by-default; test matrix cost is real |
| MAC'd resume tokens | v1.1 | v1 uses best-effort `records_already_seen`; token integrity is a discrete upgrade |
| Parquet / Arrow IPC input | v2 | Complex integration; not a JSONL tool |
| Weighted multi-input mixes | v1.1 | Upsample-to-exhaustion with `--mix-policy` is designed but implementation punted |

---

## 14. Open Questions (remaining)

1. **Input fingerprint policy.** `blake3(path, size, mtime)` is cheap but can drift without content change. Content-hash (`blake3(file_bytes)`) is correct but expensive on TB inputs. Proposal: metadata-hash by default, `--strict-fingerprint` opt-in for content-hash. Decide before v1 release.
2. **Jitter window `J` when reconciliation is deterministic.** Jitter now only randomizes reconciled-line *position within interleave*, not whether it's emitted. Smaller `J` (e.g., `max_line`) suffices. Confirm via empirical adjacency test before freezing default.
3. **`metrics` vs `prometheus` crate.** `metrics` + `metrics-exporter-prometheus` is more idiomatic in 2026. Lean yes but benchmark exporter throughput at 1M metrics/sec before committing.
4. **Weighted interleave vs. round-robin default for chunk-shuffled.** Weighted-random-by-remaining-lines is closer to uniform over the M-chunk union; round-robin is simpler. Measure and decide; default lean weighted-random.
5. **UDS socket perms.** `0600` (owner only) default. `--uds-mode 660` for shared-group sidecars? Or require group-owned sockets to be explicitly configured? Decide in v1.
6. **Should `reservoir` mode be `stream`-only?** It's not meaningful over `serve` (finite output; client would need to wait for EOF to get any records in some pathological cases). Distributed-systems review recommends rejecting `RESERVOIR` in `Stream` RPC. Accepted in the proto (see `ShuffleConfig.Mode` — reservoir intentionally omitted). Confirm in docs.
7. **Windows posture.** "Best-effort" — what does that mean in PR policy? Accept contributions but don't block on test failures? Decide before opening the repo for contributions.

---

## 15. Appendix — Review Cross-Reference

Each review's strongest recommendations and where this doc acts on them. Non-exhaustive; reviews are the source of truth.

| Review | Section in 002 |
|---|---|
| 01 systems-perf — pread default, 16 MiB chunks, prefetch pipeline, `MADV_DONTNEED`, OS matrix, zstd_seekable commit | §1, §4, §0 (rows 1–2) |
| 02 ML practitioner — resumable cursors v1, rank/world-size v1, index-perm promoted, chunk-shuffled safety warning, PyTorch example | §6.2, §7.3, §8, §0 (rows 4, 6) |
| 03 distributed systems — proto contract, multi-client `independent`-only, auth defaults, metric set, two topologies | §7, §0 (rows 9–11) |
| 04 Rust architect — crate collapse, feature flags, sync-async bridge, error enum, lint pattern | §1, §10, §0 (row 8, 14) |
| 05 security — threat model, loopback default, SIGBUS, glob hardening, supply chain, unsafe policy, log hygiene | §9, §4.5, §0 (row 9) |
| 06 correctness — honest quality wording, deterministic reconciliation, reservoir/buffer split, PRF hierarchy, property tests, edge cases | §2, §3, §5, §11, §0 (rows 3, 5, 13) |
| 07 CLI UX — subcommands, flag cut to 12 primary, colon syntax, sysexits, help mock, cookbook, env vars, man page | §6, §0 (row 7, 12) |

---

## 16. What Ships Next

Action items for the scaffolding, in order:

1. Collapse workspace to `crates/shuflr` + `crates/shuflr-cli`. Remove `shuflr-core`, `shuflr-io`, `shuflr-sink` directories. (Net delete.)
2. Add feature-flag skeleton to `crates/shuflr/Cargo.toml` per §10.2 (all deps `optional = true`, no code yet).
3. Add `deny.toml` per §9.3; wire `cargo deny check` into a stub CI workflow.
4. Add empty criterion bench (`crates/shuflr/benches/throughput.rs`) that `use criterion` and has one `fn noop`, so CI has a target.
5. Add `#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]` to the (future) `lib.rs` / `main.rs`.
6. Commit `Cargo.lock`.
7. Update `CLAUDE.md` to reflect 2-crate layout and point to this doc as the authoritative spec.

After scaffolding: implement §2.6 `none` + §4.1 `pread` + §6 CLI skeleton as the first real PR. That proves the pipeline end-to-end with the simplest possible shuffle, and everything else layers on.
