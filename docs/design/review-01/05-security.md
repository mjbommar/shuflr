# shuflr v1 — Security Review

*Reviewer: security engineer, threat-modeling hat on. "It's internal tooling" is where breaches start.*

## TL;DR

**Not green-lit as designed.** The CLI-only mode (stdout, single-file, trusted input) is defensible. The network-service mode as specified — `--listen=tcp://0.0.0.0:9000`, `--grpc=:50051`, no TLS, no auth, no connection caps — is a read-any-JSONL-the-process-can-reach oracle and must not ship. There are also three concrete crash/DoS surfaces (SIGBUS on truncated mmap, symlink/device-file globbing, unbounded deep JSON via `--verify`) that need to be nailed down before v1. Fix roughly eight items below and v1 is shippable on an internal cluster; leave them and the first real adversary walks off with the training corpus.

## Threat Model

**Adversaries.** Three realistic classes: (a) a *corpus producer* — the upstream pipeline writing JSONL that shuflr reads. Not malicious, but buggy: malformed records, truncated files, symlinks into `/proc`, oversized lines. (b) A *network peer* on the same VPC/k8s namespace as a shuflr server — can speak to `--listen`/`--grpc` sockets if they aren't firewalled, can exhaust fds, can exfiltrate data if no auth. (c) A *supply-chain adversary* — a yanked/typosquatted crate in the tonic/prost transitive tree, or a malicious update to a direct dep.

**Assets.** The JSONL payload itself (LLM training corpora are commercially sensitive and routinely contain PII), the host's filesystem read surface (shuflr reads whatever its uid can read — `/proc/self/environ`, `~/.aws/credentials`, etc. are all in scope if a glob reaches them), process liveness (a SIGBUS or OOM takes down the server), and the integrity of downstream training runs (a forged resumable cursor in v2 could let a client read from a different seed/offset than it's entitled to).

**Trust boundaries.** (1) Between the input file(s) and the parser — the JSONL on disk is *not* trusted. (2) Between the network socket and the service — any non-loopback bind crosses a trust boundary. (3) Between the process and `unsafe` mmap code — mmap wrappers can't assume the file is stable. (4) Between the release artifact and the operator — binary provenance is a trust edge.

## Findings

### High

**H1. Network service ships unauthenticated and unencrypted by default.**
*What's wrong.* `--listen=tcp://0.0.0.0:9000` and `--grpc=:50051` in the design have no accompanying `--tls`, `--auth`, or bind-scope flags. Anyone routable to the process reads the stream.
*Impact.* Training-data exfiltration on any shared network (office LAN, flat VPC, misconfigured k8s NetworkPolicy). The RSS chart and the data are indistinguishable to an attacker: open a gRPC stream, receive corpus.
*Recommendation.* Default-bind to `127.0.0.1`/UDS. Require `--bind-public` to expose beyond loopback, and refuse `--bind-public` unless `--tls <cert>` is also set. Ship a bearer-token mode (`--auth-token-file /path/to/secret`) with constant-time comparison; reject non-TLS bearer auth on non-loopback. UDS perms default `0600`, owner only.

**H2. mmap SIGBUS on truncation/replacement.**
*What's wrong.* The plan acknowledges "file mutation detection" but specifies nothing concrete. Any writer that truncates or replaces the backing file while shuflr holds a mapping will, on the next page touch, crash the process with SIGBUS.
*Impact.* Trivially weaponizable DoS — a co-tenant who can `truncate -s 0` the corpus takes down the server. Also a real operational hazard: cron job rotates logs, shuflr dies.
*Recommendation.* Three-layer defense. (a) At open time, record `(st_dev, st_ino, st_size, st_mtime)`; before remapping any chunk, re-stat and bail cleanly if the identity changed. (b) Install a `SIGBUS` handler in `shuflr-io` that `siglongjmp`s out of the read path to a typed `MappedFileTruncated` error — handler must be async-signal-safe, no allocation. (c) Document loudly that production deployments should serve from immutable snapshots (`cp --reflink`, a read-only bind mount, or `chattr +i`). Provide a `--require-immutable` flag that `fstat`s at start and refuses to run if the file is writable by the current user.

**H3. Glob/directory input can escape into devices and symlink-swapped paths.**
*What's wrong.* `--input '*.jsonl'` plus directory inputs with no symlink or file-type policy. `/dev/zero`, `/proc/self/mem` (readable for own-uid), FIFOs, and sockets will all be happily opened; a symlink that's swapped between `stat` and `open` is a classic TOCTOU.
*Impact.* Infinite read on `/dev/zero`, information disclosure via `/proc/*/environ` on multi-tenant boxes, TOCTOU-based file substitution.
*Recommendation.* Default policy: regular files only (reject `S_IFLNK`, `S_IFCHR`, `S_IFBLK`, `S_IFIFO`, `S_IFSOCK`). Open with `O_NOFOLLOW | O_CLOEXEC` and re-`fstat` the fd (not the path) before mmap. Add explicit opt-ins: `--follow-symlinks`, `--allow-devices`. Glob expansion must reject paths with `..` components after canonicalization and must refuse to cross filesystem boundaries unless `--cross-fs` is set.

**H4. No per-process or per-client resource caps on the network path.**
*What's wrong.* Plan mentions `--bounded-queue` but nothing about `--max-connections`, per-client stream caps, or fd budget. An attacker opens 10k concurrent gRPC streams.
*Impact.* fd exhaustion takes the whole process down; memory exhaustion from per-stream buffers does the same. Classic slowloris works here because gRPC streams can be opened and held idle.
*Recommendation.* `--max-connections N` (default 64), `--max-streams-per-peer K` (default 4), `--idle-timeout 30s`, `--handshake-timeout 5s`. Enforce tonic's `concurrency_limit_per_connection` and `tcp_keepalive`. Set `RLIMIT_NOFILE` defensively at startup and log the effective cap. Reject new connections with a typed error rather than accepting-then-closing.

### Medium

**M1. JSONL parser safety under `--verify` is unspecified.**
*What's wrong.* `--max-line` bounds bytes per record, but `--verify` implies running a JSON parser over records. `serde_json` with defaults will happily parse arbitrarily deep nesting and unbounded number literals. No mention of parser choice or limits.
*Impact.* Stack overflow on a crafted record (`{"a":{"a":{"a":…` 10k deep); algorithmic amplification from pathological number/string escapes.
*Recommendation.* Pin to `serde_json` with a configured `RecursionLimit` (e.g., 128) and reject records whose tokenization allocates more than `8·max_line` bytes. Consider `simd-json`'s tape parser which is stack-bounded by construction. Fuzz `--verify` specifically (AFL++ target over structured JSON inputs, not just framing).

**M2. UTF-8, BOM, NUL, CRLF defaults.**
*What's wrong.* The plan has `--validate-utf8` but doesn't say it's on by default, doesn't address UTF-8 BOM at file start, and doesn't say what happens to embedded NUL bytes or CR-before-LF.
*Impact.* A NUL byte in a record breaks downstream C tooling silently. A BOM makes the first record's JSON parse fail confusingly. CRLF records silently carry `\r` into every line.
*Recommendation.* Defaults-on: `--validate-utf8=strict`, strip leading UTF-8 BOM once per file, reject embedded NULs with `on-error` policy applied, treat CRLF as LF when `--framing=lf`. Document that framing is byte-precise; users who want `\r` preserved pass `--framing=raw`.

**M3. Logging leaks record content.**
*What's wrong.* The plan says `tracing` to stderr and has an `--on-error log` mode, but nowhere forbids logging record bytes. The obvious naive implementation of "log the malformed line" is a PII leak.
*Impact.* Training data (potentially with PII, credentials, customer text) ends up in container stdout/stderr aggregators, searchable by anyone with log access.
*Recommendation.* Hard rule, documented in `shuflr-io` and `shuflr-core`: error events carry `{byte_offset, length, xxhash64, reason}` only — never record bytes, never a prefix. Add a `clippy` lint or a `#[deny]` custom check that forbids `Display`/`Debug` of record payloads in tracing spans. `--on-error log` logs counters, not content.

**M4. Supply chain posture is absent.**
*What's wrong.* ~15 direct crates, tonic/prost pulling ~100 transitives. No `deny.toml`, no `cargo-vet` story, no mention of a frozen `Cargo.lock` for the released binary.
*Impact.* One compromised transitive, one yanked advisory, one incompatible license slipping in.
*Recommendation.* Ship `deny.toml` with: `licenses.allow = [MIT, Apache-2.0, BSD-3-Clause, Unicode-DFS-2016, ISC]`, `advisories.vulnerability = "deny"`, `advisories.yanked = "deny"`, `bans.multiple-versions = "warn"`, `sources.unknown-registry = "deny"`, `sources.unknown-git = "deny"`. CI job: `cargo deny check` + `cargo audit` on every PR, nightly against main. Commit `Cargo.lock` (binary crate, so yes). Adopt `cargo-vet` with a trust set including `rust-lang`, `dtolnay`, `tokio-rs`, `BurntSushi`. Use `cargo auditable` so the release binary embeds its SBOM.

**M5. `unsafe` block hygiene is understated.**
*What's wrong.* `unsafe_op_in_unsafe_fn = "warn"` is set, but ownership of the IO crate's unsafe code, the documentation burden, and per-crate `unsafe` policy aren't specified.
*Recommendation.* `#![deny(unsafe_code)]` at the top of `shuflr-core`, `shuflr-sink`, `shuflr-cli`. All `unsafe` lives in `shuflr-io`, gated by a module with `#![allow(unsafe_code)]` and a top-of-file invariants comment. Every `unsafe` block must carry a `// SAFETY:` comment linking to the invariants; enforce with a CI grep. Consider `cargo-geiger` in CI to track unsafe surface area.

### Low

**L1. Binary hardening and provenance.** `strip = "symbols"` is fine. Explicitly enable `lto = "thin"`, keep `panic = "abort"` for server builds (prevents unwind-across-FFI surprises and shrinks the unwind surface an attacker can exploit). Ship release artifacts with a Sigstore/cosign signature and a SHA-256 in the release notes; point the install docs at verification, not `curl | sh`.

**L2. Sandboxing.** The CLI doesn't need the network and the server doesn't need to open new files after startup. On Linux, ship optional `--sandbox` that applies a Landlock ruleset (read-only on the input paths, no write anywhere) plus a seccomp-bpf filter dropping `openat` after the open phase. No filter is better than a broken one — ship this off-by-default with good docs and a test matrix.

**L3. `/stats` endpoint.** `--stats ADDR` for Prometheus is another listener. Bind-localhost default, same rules as H1.

**L4. Resumable cursor integrity (v2 preview).** When cursors land, they must be opaque to the client and authenticated. Either encrypt `(seed, chunk_idx, offset_in_chunk, epoch)` with a server-side key (XChaCha20-Poly1305), or emit random token IDs and keep state server-side. A bare `{seed, offset}` JSON blob invites a client to hand back *another* stream's seed and walk data it wasn't served.

## Security-sensitive defaults v1 should ship with

- Bind `127.0.0.1` / UDS `0600`; refuse `0.0.0.0` without `--bind-public` and `--tls`.
- Regular files only; `O_NOFOLLOW | O_CLOEXEC`; `fstat` after open.
- `--validate-utf8=strict`, strip leading BOM, reject embedded NULs per `on-error` policy.
- `--max-line` default 16 MiB (already planned); JSON depth cap 128 when `--verify` is on.
- `--max-connections=64`, `--max-streams-per-peer=4`, `--idle-timeout=30s`.
- `--require-immutable` available; SIGBUS handler always installed.
- `--on-error log` logs offsets and hashes, never bytes.
- `panic = "abort"` in release; `cargo-auditable` SBOM embedded; release artifacts signed.
- `deny.toml` enforced in CI; `Cargo.lock` committed.

## What the plan explicitly gets right

Reproducible randomness via `rand_chacha::ChaCha20Rng` keyed on `--seed` is the right primitive for both determinism and future cursor MACs. Bounded `crossbeam-channel` queues give natural backpressure and remove one class of memory-exhaustion. `stdout` vs `stderr` discipline means logs can't accidentally leak into the data channel. `thiserror` in libraries keeps error types auditable. Fuzzing the framing parsers is the right call — extend it to the JSON-verify path.

## Open questions

1. **Threat scope for `--listen` TCP.** Is v1 network mode intended for localhost-only (sidecar pattern)? If yes, make the CLI refuse non-loopback binds and defer TLS/auth to v1.1. That's a cleaner story than half-built auth.
2. **Who owns the `unsafe` audit?** Name a reviewer for `shuflr-io` in `CODEOWNERS`. Unsafe code without an assigned auditor rots.
3. **`--filter` expression language.** A "minimal jq-like predicate" is a parser. Is it a hand-rolled DSL, `jaq`, or embedded Lua? Each has a different fuzz surface; pin before coding.
4. **Metrics cardinality.** If `--stats` labels include per-client identifiers, a hostile peer can blow up Prometheus memory. Cap label cardinality explicitly.
5. **Multi-tenant posture.** Is one shuflr process ever expected to serve multiple trust domains at once? If yes, the entire auth model needs tenancy, not just a bearer token; if no, document "single trust domain per process" in big letters.
