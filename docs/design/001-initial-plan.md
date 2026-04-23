# shuflr — Initial Design (v1)

**Status:** Draft, iteration 1
**Date:** 2026-04-22

A swiss-army knife for streaming large JSONL files in randomized, shuffled, or otherwise non-serial orders — without loading them into memory. Like `jq` or `netcat` in spirit: one focused tool, composable with the rest of the shell, also deployable as a network service.

## Motivation

Large JSONL files (LLM training corpora, logs, event streams) routinely exceed RAM. Existing options force a tradeoff:

- **Full load + shuffle** (`shuf`) — blows up on anything above a few GB.
- **External-memory batch shuffle** (`terashuf`, Jane Street 2-pass) — produces a new shuffled file, not a stream; requires free disk equal to input size.
- **Shuffle buffer** (HuggingFace streaming, WebDataset) — only *local* shuffle, quality depends on buffer size.
- **Index + random seek** (`sjsonl`) — true random access, but Python-coupled and relies on scattered seeks.

There is no well-maintained tool that does: *stream a large JSONL file in shuffled order with low preprocessing, low memory, and high throughput, over stdout or a network socket*. `shuflr` fills that gap.

The name is a vowel-drop of **shuffler**, in the tradition of Flickr/Tumblr. The *algorithm* is closer to a riffle shuffle (card-interleave), but naming the tool "riffle" was blocked by existing projects on crates.io (sharkdp's pager) and GitHub (Apache Uniffle port).

## Core Algorithm: Chunked Riffle Shuffle

Default mode, no preprocessing required:

1. `stat(file)` → total size `S`
2. Divide logically into `N` chunks of `~S/N` bytes
3. mmap `M` active chunks (`M << N`) with `MADV_SEQUENTIAL | MADV_WILLNEED`
4. Within each active chunk, scan for `\n` (SIMD via `memchr`) to build a local offset array
5. Shuffle the per-chunk offset array (Fisher-Yates, in place)
6. Interleave output across the `M` chunks (round-robin or weighted random)
7. When a chunk is exhausted, `munmap` and map the next
8. At epoch boundary, reshuffle chunk order and **jitter boundary offsets** so lines that straddled a boundary in the previous epoch are picked up in the next

**Properties:**

- Memory: `O(M · chunk_size)` for mapped regions + `O(lines_per_chunk · 8)` for offset arrays
- I/O: purely sequential within each chunk; kernel prefetcher stays happy
- Shuffle quality: block-interleaved with intra-chunk randomness. Not uniform, but qualitatively similar to a very large shuffle buffer, and much cheaper than a full index
- Coverage: boundary-lost lines recovered probabilistically across epochs via boundary jitter

For users who need provably uniform global shuffle, `shuflr` also supports `--shuffle=index-perm` (build byte-offset index, shuffle, seek) and `--shuffle=2pass` (Jane Street external shuffle to a new file).

## Scope (v1)

**In scope for first release:**

- Chunked riffle shuffle (`chunk-rr`, `chunk-shuffled`) — the core contribution
- Serial pass-through (`none`) — useful as a fast `cat` replacement with batching
- Reservoir buffer shuffle (`reservoir=K`) — for stdin streams
- stdout output with optional batching
- gRPC streaming service
- Single-file and directory-glob inputs
- Uncompressed JSONL; seekable-zstd is v1.1

**Explicitly deferred to later iterations:**

- Full index persistence (`--index=path.idx`) and `index-perm` mode
- 2-pass external shuffle
- HTTP/2 and HTTP/3 transports
- Compressed inputs (gz, lz4) — require pre-chunking or streaming-only mode
- Parquet/Arrow IPC
- Multi-consumer fanout and partitioning
- Resumable cursors
- Weighted record sampling

## Design Surface

### I/O

| Flag | Meaning |
|---|---|
| `--input PATH` | file or glob; repeatable for weighted mixes (`--input a:0.7 --input b:0.3`) |
| `--mmap` / `--pread` / `--direct` | access mode (default `--mmap`) |
| `--framing {lf,varint,u32le}` | record framing (default `lf`) |
| `--delim BYTE` | record separator for `lf` framing (default `\n`) |
| `--max-line BYTES` | bound per-record size (default 16MB) |

Remote inputs (`s3://`, `gs://`, `http(s)://` with Range) deferred to v1.1 behind a feature flag.

### Ordering / Shuffling

| Flag | Meaning |
|---|---|
| `--shuffle {none,chunk-rr,chunk-shuffled,reservoir}` | strategy (default `chunk-shuffled`) |
| `--chunk-size BYTES` \| `--chunks N` | chunk granularity (default 256 MiB) |
| `--active-chunks M` | concurrent mapped chunks (default 8) |
| `--seed U64` | reproducibility |
| `--epochs N` \| `inf` | number of passes (default 1) |
| `--jitter-boundaries` | shift chunk boundaries per epoch (default on for epochs > 1) |
| `--reservoir-size K` | buffer size for reservoir mode |
| `--sample N` \| `--sample-rate F` | record-count or Bernoulli caps |

### Consumption / Output

| Flag | Meaning |
|---|---|
| (default) | stdout, newline-delimited |
| `--listen ADDR` | raw TCP or UDS (`tcp://:9000`, `unix:///tmp/shuflr.sock`) |
| `--grpc ADDR` | gRPC streaming endpoint |
| `--batch-size N` | records per batch (default 1 for stdout, 128 for network) |
| `--batch-bytes BYTES` | alternative batch trigger |
| `--flush-interval DUR` | max latency before flush |
| `--framing-out {lf,u32le}` | output framing |
| `--filter EXPR` | minimal jq-like predicate (skip non-matching) |
| `--project PATHS` | comma-separated field projection |

### Observability & Ops

| Flag | Meaning |
|---|---|
| `--stats ADDR` | Prometheus metrics endpoint |
| `--log-level {error,warn,info,debug,trace}` | structured JSON to stderr |
| `--progress` | TTY progress bar |
| `--dry-run` | scan, report stats, emit nothing |
| `--verify` | validate JSONL integrity, report malformed lines |
| `--on-error {skip,fail,passthrough,log}` | malformed-line policy (default `skip`) |

## Architecture Sketch

```
┌─────────────────────────────────────────────────────────────┐
│  shuflr                                                     │
│                                                             │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐   │
│  │ ChunkPool    │───▶│ ShuffleCore  │───▶│ OutputSink   │   │
│  │ mmap + idx   │    │ interleave   │    │ stdout/tcp/  │   │
│  │ M active     │    │ emit order   │    │ gRPC/UDS     │   │
│  └──────┬───────┘    └──────────────┘    └──────────────┘   │
│         │                                                   │
│  ┌──────▼────────┐                                          │
│  │ Scheduler     │  prefetch next chunk                     │
│  │ (bg thread)   │  jitter boundaries per epoch             │
│  └───────────────┘                                          │
└─────────────────────────────────────────────────────────────┘
```

Threads communicate via `crossbeam` bounded channels. Backpressure flows naturally from sink → shuffle-core → chunk-pool: if the consumer stalls, the bounded channels fill, the scheduler stops prefetching, and no work is wasted.

## Crate Layout

```
shuflr/
├── Cargo.toml             # workspace
├── crates/
│   ├── shuflr-core/       # chunk pool, shuffle algorithms, indexing
│   ├── shuflr-io/         # input sources (mmap, pread, stdin)
│   ├── shuflr-sink/       # output transports (stdout, tcp, gRPC)
│   └── shuflr-cli/        # binary crate: arg parsing, wiring (produces `shuflr`)
├── docs/
│   └── design/            # design iterations
└── tests/
    └── corpora/           # small fixtures; large corpora git-ignored
```

## Key Dependencies (tentative)

- `memmap2` — mmap wrapper
- `memchr` — SIMD newline scan
- `rand` + `rand_chacha` — seedable, reproducible shuffling
- `crossbeam-channel` — bounded queues
- `tokio` — async runtime (for network sinks)
- `tonic` + `prost` — gRPC
- `clap` (derive) — CLI
- `tracing` + `tracing-subscriber` — structured logging
- `prometheus` or `metrics` — stats export
- `xxhash-rust` — cheap line hashing for `--checksum` mode

## Performance Targets (v1)

- ≥ 1 GB/s throughput on NVMe SSD for `--shuffle=none` passthrough
- ≥ 500 MB/s for `--shuffle=chunk-shuffled` with default chunk size
- ≤ 2× memory of `--active-chunks · chunk-size` (so ~2 GiB RSS at defaults)
- Time-to-first-record ≤ 50 ms (no preprocessing for chunk modes)

## Correctness Strategy

- **Property tests** (`proptest`): for any seed & chunk config, every line in the input appears in some epoch (within N epochs for some N).
- **Determinism tests:** same seed + same input = byte-identical output.
- **Boundary-loss accounting:** instrument and log expected vs. observed line counts per epoch; alert if divergence exceeds bound implied by chunk count.
- **Fuzzing:** AFL++ on the record-framing parsers.

## Open Questions

1. **Default shuffle mode:** `chunk-shuffled` or `chunk-rr`? The former is higher quality; the latter is marginally faster. Lean toward `chunk-shuffled` since the intra-chunk scan is cheap (~25 ms per 256 MiB chunk).
2. **Record-size distribution:** if lines are *highly* skewed (one 1 GB JSON line among millions of small ones), chunk boundaries become pathological. Do we detect and warn, or handle gracefully? Current plan: respect `--max-line` and log oversized-line skips.
3. **Output format for batched network emission:** one JSON array per batch, or length-prefixed concatenation of individual records? Probably the latter — simpler for streaming consumers, no accumulation on the receiver.
4. **`--fanout` semantics:** if multiple clients connect, do they share the shuffle iterator (same order, each line to exactly one client) or broadcast (same order, every line to every client)? Deferred to v2, but worth pinning terminology now.

## Non-Goals

- Not a JSON transformation tool — delegate to `jq`.
- Not a distributed training framework — expose a clean gRPC stream; let the training loop handle sharding across ranks.
- Not a general object store — stick to file-like inputs.
- Not a replacement for `terashuf` / `sort -R` when the user truly wants a fully materialized shuffled file on disk. The `2pass` mode is deferred; users who need it today have existing tools.
