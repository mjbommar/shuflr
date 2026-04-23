# shuflr

A Rust CLI and service for streaming large JSONL files in shuffled order, without loading them into memory. The name is a vowel-drop of **shuffler** — the tool that shuffles records out of a file, one stream at a time.

## What this tool is

- A **swiss-army knife** for non-serial consumption of very large JSONL files — LLM training corpora, event logs, sampled datasets.
- Usable as a **CLI pipe** (stdout, jq/netcat-style) *or* as a **network service** (gRPC streaming; TCP/UDS; HTTP/2 later).
- Optimized for **low preprocessing, low memory, high throughput** — the default mode does no pre-scan of the file.

## What this tool is **not**

- Not a JSON transformation tool. Minimal `--filter` / `--project` only; real transforms belong in `jq` downstream.
- Not a distributed training framework. It emits a clean shuffled stream; consumers handle sharding.
- Not a replacement for materialized batch shuffling (`terashuf`, `sort -R`) when the user wants a new shuffled file on disk.

## Core idea

**Chunked riffle shuffle:** logically divide the file into N chunks, `pread` M at a time, scan each for record offsets (SIMD newline search), Fisher-Yates within each chunk, weighted-random interleave across chunks. Boundary-straddling lines are handled by deterministic reconciliation (one extra bounded `pread` per interior boundary per epoch), not probabilistic jitter. No index file required for this mode.

For provably uniform shuffles, `--shuffle=index-perm` builds and persists a `.shuflr-idx` file (8 bytes per line) and emits in a Fisher-Yates permutation of the offsets. This is the recommended mode for LLM training; `chunk-shuffled` is the zero-prep default for analytics and already-random inputs.

## Authoritative spec

**`docs/design/002-revised-plan.md`** is the v1 specification. It supersedes 001 on every conflict. Reviews that drove 002 live in `docs/design/review-01/`.

**Amendments (applied on top of 002):**
- `003-compression-formats.md` — compression codec policy. Supersedes 002 §4.4; promotes streaming `.gz` / `.zst` / `.bz2` / `.xz` to v1 (buffer/none/reservoir only); partially superseded by 004.
- `004-convert-subcommand.md` — adds `shuflr convert` + `shuflr info` subcommands (v1). Promotes the seekable-zstd writer AND reader to v1; defines record-aligned frames as a shuflr invariant. This is the canonical answer to "how do I make my corpus seekable?"
- `005-serve-multi-transport.md` — supersedes 002 §7. Three transports (plain HTTP/1.1 NDJSON, `shuflr-wire/1` custom binary, gRPC) on a shared sync core. First-party Python client (`shuflr-client`, Rust + pyo3 + maturin) wraps them. TLS is optional; `--insecure-public` opt-in for unencrypted public bind. Auth defaults to none (bearer + mTLS available). Design only; PR-30 onward implements.

When reading 002, check later-numbered docs for amendments to any section you care about. Design docs are append-only iterations (`001-`, `002-`, …). When the design evolves, write a new doc referencing the prior one; don't rewrite history.

## Repository structure

```
crates/
  shuflr/         # library: engine, algorithms, I/O, sinks, service (all feature-gated)
  shuflr-cli/     # binary: arg parsing, wiring (produces the `shuflr` binary)
  shuflr-client/  # Python package (Rust + pyo3, built with maturin) — `pip install shuflr-client`
docs/design/      # numbered design docs + review directories
tests/corpora/    # small fixtures; large corpora are git-ignored
deny.toml         # cargo-deny policy (licenses, advisories, sources)
.github/workflows/ci.yml   # check + test + each-feature + deny
```

Three crates. `shuflr` holds all engine code; `shuflr-cli` is the binary; `shuflr-client` is the Python wheel (Rust core, HTTP transport per PR-34a; wire transport lands in PR-36). Run Python tests via `crates/shuflr-client/scripts/test.sh`.

## Conventions

- **Rust edition:** 2024, resolver 3. MSRV = latest stable; users bring their own rustup. Distro-shipped stable rustc is not supported.
- **Error handling:** `anyhow` only in `shuflr-cli`; `thiserror` in `shuflr` with a single re-exported `shuflr::Error` enum.
- **Async:** `tokio` is gated behind the `grpc` feature and lives only at the gRPC edge. Sync core never calls `.await` or `block_on`. The sync→async bridge is a dedicated OS thread that calls `handle.block_on(tx.send(...))` on a `tokio::sync::mpsc` — this is the only permitted `block_on` in the codebase.
- **Reproducibility:** every randomized operation takes a seed. RNG is `rand_chacha::ChaCha20Rng`. Sub-computations are addressed through a BLAKE3 PRF hierarchy (master → epoch → {chunk, interleave, jitter, perm} / shard / client) — see 002 §3. No implicit `thread_rng()`.
- **Logging:** `tracing` to stderr only. Never log record bytes — log `{byte_offset, length, blake3_hex, reason}` only. PII in corpora is real.
- **Stdout is the data channel.** Sinks take `impl Write`; the CLI passes `io::stdout().lock()`. This means no `println!`/`print!` in library code, which makes the `clippy::print_stdout = warn` lint noise-free.
- **Library crate rules:** zero `unwrap()`/`expect()` outside tests; every `unsafe { }` has a `// SAFETY:` comment; `#![deny(unsafe_code)]` on every module except `shuflr::io::mmap` and `shuflr::io::uring`.
- **Test shim:** every `lib.rs` / `main.rs` starts with `#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]` so test code doesn't drown in lint warnings.

## Performance posture

- Throughput targets: ≥ 2 GB/s passthrough (`--shuffle=none`, `--io=pread`), ≥ 1 GB/s `chunk-shuffled`, ≥ 400 MB/s `index-perm` on NVMe. Time-to-first-record ≤ 50 ms for no-index modes.
- RSS bound of `active-chunks × chunk-size` (~128 MiB at defaults) holds only under `--io=pread` or `--io=direct`. `--io=mmap` mode calls `MADV_DONTNEED` on chunk retire to enforce it.
- Benchmarks live in `crates/shuflr/benches/` (criterion). A ≥ 5% regression requires a written waiver in the PR.

## Testing posture

- Unit tests alongside modules.
- Property tests (`proptest`) for shuffle correctness per 002 §11.1: determinism, permutation (index-perm), coverage, rank disjointness, intra-chunk uniformity, adjacency bias, seed-tree isolation, Vitter uniformity, buffer displacement bound.
- Chaos/integration tests for disconnect, file-mutation SIGBUS, slow-consumer eviction, rank disjointness (`tests/`).
- Fuzz targets (`cargo-fuzz`) for framing, JSON verify, `.shuflr-idx` parse, proto decode.
- Edge-case test matrix per 002 §11.2 (0-byte file, 1-line file, CRLF, embedded NULs, oversized line, file growth, file truncation, etc.).

## When working on this project

- **Read `docs/design/002-revised-plan.md` before changing algorithm or API behavior.** If a code change contradicts the spec, update the spec (write 003) before or with the code change. Never silently drift.
- **Prefer editing existing modules** over adding new top-level ones. The 2-crate boundary is deliberate.
- **Keep the CLI surface minimal.** 12 primary + 6 tuning flags in v1. New flags need a clear user story; push back when there isn't one.
- **Stdout is sacred.** Data to stdout; logs/progress/errors to stderr. Sinks take `impl Write`. Never `println!` in library code.
- **Benchmark before claiming a perf improvement.** `cargo bench` output beats intuition; commit the Criterion baseline alongside the code change.
- **Never skip hooks or bypass signing without explicit user instruction.** Pre-commit hook failures are to be fixed, not evaded.

## Current status

Through PR-28. Highlights since PR-14: PR-15 (visible WARN on silently-dropped oversized records), PR-16 (wire standalone `verify`), PR-17 (`shuflr man`), PR-18 (--threads=0 → physical cores on convert), PR-19 (JS + per-frame entropy in `analyze`), PR-20 (PyTorch IterableDataset example), PR-21 (entropy filter on convert), PR-22 (`index-perm` on seekable-zstd, no plain-file scratch), PR-23 (`.shuflr-idx-zst` sidecar — 1595× cache-hot speedup), PR-24 (`analyze --json`), PR-25 (progress bar during the zstd record-index cold build), PR-26 (parallel cold-cache record-index build — 4.13× speedup, 127 s → 32 s), PR-27 (parallel emit for `--shuffle=index-perm` — 2.39× at 10k sample), PR-28 (parallel emit for `--shuffle=chunk-shuffled` — 1.77× at 30k sample, byte-identical).

**179 tests green.** Both hot-path emit modes (`chunk-shuffled` and `index-perm` on seekable-zstd) now have prefetch-pipeline parallel variants. `--emit-threads=N --emit-prefetch=K` is shared across modes; default stays `--emit-threads=1` (no behavior change without opt-in).

Through **PR-33**: HTTP transport with rustls TLS 1.3 + bearer/mTLS auth (PR-30/31), Python client (PR-34a, HTTP-only), `shuflr-wire/1` codec crate (PR-32), and wire transport inside `serve` speaking TCP + optional TLS with `plain-batch` mode for all 5 shuffle modes (PR-33). `shuflr serve --wire 127.0.0.1:9443 --http 127.0.0.1:9000 ...` runs both listeners simultaneously under one Ctrl-C.

Remaining 005 PRs: **33a** credit-based flow control — **33b** raw-frame passthrough for chunk-shuffled (~6× wire savings) — **33c** zstd-batch compression — **33d** UDS listener — **34b** `shuflr-client` speaks wire — **35** gRPC — **36** observability.

Other known follow-ups: parallel-pread reader for convert, SIGBUS handler + `--require-immutable`, consistent `--log-level` across all subcommands.

## Upstream

Destined for `github.com/mjbommar/shuflr`. Local-only until pushed.
