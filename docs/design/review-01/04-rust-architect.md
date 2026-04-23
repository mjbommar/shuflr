# shuflr v1 scaffolding — Rust architect review

*Reviewer: crates.io maintainer hat. I've shipped a library that gets ~a million downloads/month; my bias is toward small published surfaces, short compile times, and semver conservatism. I'm going to be blunt about the workspace shape because the cheapest time to fix crate boundaries is before any code is written.*

## TL;DR

The scaffolding is going to **hurt velocity, modestly**. The core algorithmic bet is sound, the lint posture is tasteful, and the design doc is unusually disciplined. But the four-crate workspace, a `1.85` MSRV, and the absence of a feature-flag plan are going to impose friction during the exact phase where you want the least. Collapse to **two published crates + one internal binary** (`shuflr-core` as the publishable library, `shuflr` as the binary, optional `chunked-shuffle` extracted only once the algorithm is stable), wire up features now, and soften the MSRV. Everything else is fine.

## Strengths

- Workspace-level `unsafe_op_in_unsafe_fn = "warn"` and `print_stdout = "warn"` from day one — exactly right for a tool whose stdout *is* its payload.
- `resolver = "3"` and `edition = "2024"` across the workspace. No stragglers.
- `panic = "abort"` + `strip = "symbols"` + `lto = "thin"` in release — sensible for a CLI.
- CLAUDE.md nails the invariants that actually matter: seeded RNG only, no `println!` in libs, stdout is sacred.

## Concerns and gaps

### 1. Four crates before a line of algorithm code

`shuflr-core` / `shuflr-io` / `shuflr-sink` / `shuflr-cli` is a plausible end-state, but it's an expensive starting state. Every cross-crate function has to be `pub`, which means every rename needs a `cargo check -p` across three crates, and every internal refactor becomes a 3-PR trail. You get none of the benefits (faster incremental builds are marginal at this size, and no downstream consumer exists yet) and all of the costs.

**Recommendation:** merge to **one library crate + one binary** until the algorithm and I/O seams are exercised by real code. Split later — `cargo` makes that mechanical, and you'll split with evidence.

### 2. The `shuflr-io` / `shuflr-core` seam will leak

The "chunk pool" as drawn in the design owns both the `Mmap` (IO) and the offset index + shuffle state (core). That's a single data structure pretending to span two crates. Either `core` gets a trait `ByteSource` and `io` implements it, or the chunk pool lives in `io` and `core` sees only `&[u8]` + `Range<u64>`. The current split as described in the plan ("`shuflr-core/` chunk pool" *and* "`shuflr-io/` mmap") is ambiguous — someone will put it in both.

**Concrete seam I'd pick:**

```text
core::shuffle       — ChunkShuffler<Src: ByteSource>, algorithms, RNG plumbing
core::framing       — newline scanner, record iteration over &[u8]
core::traits        — ByteSource, RecordSink   (pure traits, no deps)
io::mmap, io::pread — impl ByteSource
sink::stdout,tcp,grpc — impl RecordSink
```

Every type that touches `Mmap` stays out of `core`. This also makes `core` trivially testable against a `Vec<u8>` source.

### 3. Sync core, async sinks — explicit bridge needed

"core is sync, tokio only for network" will bite the first time the gRPC stream backpressures. A blocking `crossbeam-channel::send` inside a `#[tonic::async_trait]` method stalls a tokio worker. The design doc doesn't say which side of the wall owns the bridge.

Pin this now: **one dedicated OS thread drives the sync pipeline; the tonic handler pulls from a `tokio::sync::mpsc` whose sender is fed by a `spawn_blocking` or a small bridge thread that calls `handle.blocking_send`.** Not `block_on` inside the sync core, ever. Document it in `001` as a constraint.

### 4. Feature flags — currently zero

With `tonic` + `prost` + `prometheus` + `tokio` all on by default, `cargo install shuflr` will compile ~400 crates and a protoc toolchain for users who just want `cat --shuffle`. That's the fastest way to get one-star issues on a CLI.

Proposed default feature set (on `shuflr-cli`):

```toml
[features]
default = ["stdout", "progress"]
stdout  = []
progress = ["dep:indicatif"]
grpc    = ["dep:tonic", "dep:prost", "dep:tokio"]
prom    = ["dep:prometheus"]
zstd    = ["dep:zstd"]
gzip    = ["dep:flate2"]
s3      = ["dep:aws-sdk-s3"]
gcs     = ["dep:google-cloud-storage"]
```

Wire `cargo hack --each-feature check` into CI on day one — it's cheap while the code is small and excruciating to retrofit.

### 5. Dep list review

Reasonable set overall. Adjustments:

- Add **`bytes`** — you'll regret not having `Bytes` once batching hits the network sink.
- Add **`io-uring`** (or `tokio-uring`) behind `cfg(target_os = "linux")` + a `uring` feature; document it as "v1.1, Linux only." Don't let it into the default path.
- **`serde_json`** is conspicuously absent. Either commit (behind a `validate` feature for `--verify`) or commit to *not* parsing JSON in the hot path and say so. Currently ambiguous.
- `prometheus` *or* `metrics`? Pick one. `metrics` + `metrics-exporter-prometheus` is more idiomatic in 2026; `prometheus` has a heavier API surface.
- `simd-json` / `sonic-rs` — only if `--verify` or `--filter` needs it. Don't speculatively add.
- `tonic` pulls `prost` transitively; you don't need both in `[dependencies]` unless you hand-write messages.

### 6. MSRV 1.85 is aggressive for a `cargo install` tool

Debian 12 (bookworm) ships rustc 1.63; Ubuntu 22.04 LTS ships 1.75; RHEL 9 is similar. Anyone on a stable distro toolchain can't `cargo install shuflr` without rustup. Edition 2024 genuinely requires 1.85+, so this is a real tradeoff — but state the policy: *"MSRV = latest stable; users are expected to use rustup."* Otherwise you'll get bug reports.

If you want wider reach, Edition 2021 + MSRV 1.75 buys you essentially everything except a handful of 2024 niceties (`gen`, updated capture rules). Worth considering.

### 7. Error shape

"anyhow at binaries, thiserror in libs" is the direction, not the design. Pick per-module errors with a single re-exported top-level `enum Error` in `shuflr-core`:

```rust
// core/src/error.rs
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("io: {0}")] Io(#[from] std::io::Error),
    #[error("framing: {0}")] Framing(#[from] framing::Error),
    #[error("shuffle: {0}")] Shuffle(#[from] shuffle::Error),
}
pub type Result<T> = std::result::Result<T, Error>;
```

Do **not** expose `anyhow::Error` from any `pub` function in `shuflr-core`. That's semver napalm.

### 8. The `print_stdout` lint vs. the data channel

Once records start flowing, you'll hit this lint on every write. The idiomatic escape is a single data-channel module with a localized allow:

```rust
// sink/src/stdout.rs
#[allow(clippy::print_stdout)] // this is the data channel
pub fn write_record(w: &mut impl Write, bytes: &[u8]) -> io::Result<()> { ... }
```

Better: don't write through `println!` / `print!` at all — take `impl Write` and have the CLI pass `io::stdout().lock()`. Then the lint never fires and you're trivially testable with a `Vec<u8>`. Make *this* the house rule.

### 9. `unwrap_used` / `expect_used` noise in tests

Put this in every crate's `lib.rs`:

```rust
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]
```

Or, equivalently, scope the workspace lint via a `[lints.clippy]` override in a `tests/` target. The inner-attribute form is cleaner and more portable.

### 10. Testing / benchmarking scaffolding

Create now (cheap), fill later: `crates/shuflr-core/benches/` with a single empty `throughput.rs` that `use criterion` and a `fn bench_noop(c: &mut Criterion)` — just so CI has a target to build. Same for `fuzz/` at the workspace root (`cargo fuzz init` produces the right layout; AFL++ can read the same corpora). Creating the directories now with `.gitkeep` isn't worth it; creating a buildable empty bench is — it catches breakage the first time you add a real one.

### 11. Is there a standalone-worthy crate?

Yes — **`chunked-shuffle`**. The algorithm is genuinely novel in the crates.io ecosystem and is useful without the CLI or the gRPC stack. But: *extract it after the algorithm is stable, not before.* Publishing it prematurely means you either version-lock `shuflr` to its own splinter crate or churn the splinter's API while iterating. Six-month rule: if the algorithm hasn't changed in six months, extract.

## Concrete Cargo.toml deltas

### Delta 1 — collapse to one library + one binary

**Before** (`Cargo.toml`, the four-crate workspace) → **after:**

```toml
[workspace]
resolver = "3"
members = ["crates/shuflr", "crates/shuflr-cli"]

# crates/shuflr/Cargo.toml  (was shuflr-core, absorbs io + sink modules)
[package]
name = "shuflr"
description = "Streaming shuffled JSONL: chunk pool, shuffle algorithms, I/O, sinks"
# ... workspace inheritance as today ...

[features]
default = []
grpc = ["dep:tonic", "dep:prost", "dep:tokio"]
prom = ["dep:prometheus"]
zstd = ["dep:zstd"]
gzip = ["dep:flate2"]

[dependencies]
memmap2 = "0.9"
memchr = "2"
rand = { version = "0.8", default-features = false, features = ["std"] }
rand_chacha = "0.3"
crossbeam-channel = "0.5"
bytes = "1"
thiserror = "2"
tracing = "0.1"
tokio = { version = "1", optional = true, features = ["rt-multi-thread", "sync", "macros"] }
tonic = { version = "0.12", optional = true }
prost = { version = "0.13", optional = true }
prometheus = { version = "0.13", optional = true }
zstd  = { version = "0.13", optional = true }
flate2 = { version = "1", optional = true }
```

The `shuflr-cli` crate keeps the binary, depends on `shuflr` with the features it needs (`grpc`, `prom`, …), owns `anyhow` and `clap`. Internal module paths (`shuflr::io::mmap::Source`) replace cross-crate paths. When a seam proves load-bearing, extract then.

### Delta 2 — workspace lints that don't punish tests

**Before:**

```toml
[workspace.lints.clippy]
unwrap_used = "warn"
expect_used = "warn"
```

**After:**

```toml
[workspace.lints.clippy]
unwrap_used = "warn"
expect_used = "warn"
dbg_macro = "warn"
print_stdout = "warn"
print_stderr = "allow"   # tracing-subscriber writes here; and the placeholder main
# Tighten later — these three catch real bugs without drowning you:
needless_collect = "warn"
redundant_clone = "warn"
manual_let_else = "warn"

[workspace.lints.rust]
unsafe_op_in_unsafe_fn = "warn"
unused_must_use = "deny"
missing_docs = "warn"    # only on published crates; override in shuflr-cli
```

And in each `lib.rs`:

```rust
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]
```

## Recommended crate-layout revision

```
shuflr/
├── Cargo.toml                 # workspace: 2 members
├── crates/
│   ├── shuflr/                # library (core + io + sink modules inside)
│   │   ├── src/
│   │   │   ├── lib.rs
│   │   │   ├── shuffle/       # algorithms
│   │   │   ├── framing/
│   │   │   ├── io/            # mmap, pread, stdin
│   │   │   └── sink/          # stdout, tcp, grpc (feature-gated)
│   │   └── benches/throughput.rs
│   └── shuflr-cli/            # binary only; depends on shuflr + clap + anyhow
└── docs/design/
```

Extract `shuflr-core::shuffle` into `chunked-shuffle` once it's stable (six-month rule). Until then, one crate.

## Open questions

1. **`serde_json` or not?** Commit in the design doc — "we do not parse JSON in the hot path; `--filter` / `--verify` are feature-gated and only they depend on `serde_json`" is fine, but say it.
2. **`metrics` vs `prometheus`?** I'd pick `metrics` + exporter. What does your ops target (k8s? bare metal?) prefer?
3. **io_uring strategy.** Opt-in feature, or auto-detect at runtime? Auto-detect bloats the default binary; feature-gate is cleaner but needs clear docs for Linux users who want it.
4. **Windows support.** Are you claiming it? `memmap2` works, but the plan says nothing about `CreateFileMapping` quirks, stdout binary-mode, or UNC paths. Easiest answer: "v1 is Linux/macOS; Windows is best-effort." Pick one.
5. **MSRV policy.** "Latest stable, users bring their own rustup" is a fine policy, but it needs to be in `README.md` and enforced in CI via `cargo +stable check`. Otherwise distro packagers will file bugs indefinitely.
