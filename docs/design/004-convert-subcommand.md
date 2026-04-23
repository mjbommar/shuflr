# shuflr — `convert` Subcommand (Design Amendment)

**Status:** Amendment to 002 + 003. Promotes seekable-zstd writer from v1.1 to **v1**.
**Date:** 2026-04-22
**Prior:**
- `002-revised-plan.md` §6.1 (subcommand list), §13 (scope summary)
- `003-compression-formats.md` (v1.1 seekable zstd, Open Question #3 about `shuflr compress`)

## 0. What this amendment changes

1. **Promote seekable-zstd writing to v1** as a first-class subcommand: `shuflr convert`.
2. **Promote seekable-zstd reading to v1** so `chunk-shuffled` and `index-perm` can operate on compressed corpora directly (was v1.1 in 003).
3. **Add record-aligned frame boundaries** as a shuflr-specific invariant — every seekable frame produced by `shuflr convert` ends on a `\n`, making each frame a standalone JSONL shard.
4. Make `shuflr convert` the canonical answer in docs to "how do I make my corpus seekable?" (replacing the external-tool instructions in 003).

**Rationale:** the user's immediate workflow is "I have gzipped EDGAR JSONL and want to shuffle it." Without `convert` in v1, they're stuck either (a) decompressing to flat `.jsonl` (~3 TB of scratch for the 610 GB file) or (b) hunting for a third-party `zstd-seekable` binary that may or may not produce newline-aligned frames. Shipping `convert` in v1 collapses that friction into one command and eliminates the ambiguity in how the output is framed.

## 1. CLI Surface

```
shuflr convert [OPTIONS] [INPUT...] -o OUTPUT

INPUT
  One or more input files, or '-' / omitted for stdin.
  Formats auto-detected by magic bytes: .jsonl, .gz, .zst, .bz2, .xz.
  Multiple inputs are concatenated in command-line order.

OPTIONS
  -o, --output PATH             required; '-' for stdout
  -l, --level N                 zstd level 1..22                          [3]
  -f, --frame-size BYTES        target frame size (record-aligned, approximate) [2MiB]
  -T, --threads N               parallel compress threads (0 = cpu count) [0]
      --long N                  zstd long-mode window = 2^N bytes         [27 = 128MiB]
      --no-checksum             disable XXH64 per-frame (default: on)
      --input-format FMT        override auto-detect: plain|gzip|zstd|bz2|xz [auto]
      --no-record-align         pack frames to exactly --frame-size (breaks shuflr
                                chunk-boundary invariant; users who don't care about
                                random-access shuffle can save ~3% on ratio)
      --verify                  after write, re-read + roundtrip-check
      --embed-metadata          embed shuflr metadata in a skippable frame (default: on)
      --progress[=WHEN]         never|auto|always                          [auto]
  -v, --verbose                 human-readable stats on stderr
```

**Examples:**

```bash
# primary path: gz → zstd-seekable, multi-threaded
shuflr convert edgar-0.05.jsonl.gz -o edgar-0.05.jsonl.zst

# pipeline: stdin (pipe from anything) → seekable
zcat corpus.jsonl.gz | shuflr convert -o corpus.jsonl.zst

# merge multiple shards into one seekable file
shuflr convert shard-*.jsonl.gz -o merged.jsonl.zst

# max ratio, slower
shuflr convert --level 19 huge.jsonl.zst -o huge.compact.jsonl.zst

# verify after write (integration test mode)
shuflr convert --verify sample.jsonl.zst -o sample.seekable.jsonl.zst
```

**Exit codes** (in addition to 002 §6.5):
- `0` — success
- `65` (EX_DATAERR) — input contained a malformed or truncated frame
- `73` (EX_CANTCREAT) — output write failed or `--verify` roundtrip mismatch

## 2. Output Format: zstd-seekable (Facebook)

shuflr produces standard **Facebook zstd-seekable** format: a sequence of regular zstd frames followed by one final `SEEKABLE` skippable frame containing the seek table. Any zstd decoder reads it as a normal `.zst` (streaming); seekable-aware decoders (including shuflr's reader) jump to any frame in O(log N).

### Frame layout (written by `convert`)

```
┌──────────────────────────────────────────────────────────┐
│ skippable frame 0: magic 0x184D2A50                      │   ← shuflr metadata, see §2.2
│ zstd frame 1: N records, ends on '\n'                    │
│ zstd frame 2: N records, ends on '\n'                    │
│ ...                                                      │
│ zstd frame K                                             │
│ skippable frame: magic 0x184D2A5E  (seek table)          │   ← zstd-seekable trailer
└──────────────────────────────────────────────────────────┘
```

### 2.1 Record-aligned frames — the shuflr invariant

Standard `zstd-seekable` tooling closes a frame when the output buffer reaches `--frame-size`. shuflr does **not**:

1. Feed input bytes into the current frame's compressor.
2. When accumulated *input* bytes ≥ `--frame-size` AND the last byte seen was `\n`, close the frame.
3. Write seek-table entry `(compressed_size, decompressed_size, xxh64)` and start a new frame.

Result: every frame is a complete, parseable JSONL shard. shuflr chunk boundaries can align to frame boundaries (`chunk_size = k × frame_size`) without any boundary reconciliation (002 §5). This is the tightest possible seekable framing for shuflr-style consumption.

**Pathological case:** a single record larger than `--frame-size`. Policy: the record is placed in its own frame, which exceeds the target size. Warn once on stderr. (Alternative: reject; but a corpus with 8 MB records like EDGAR should not be rejected by a conversion tool.)

**`--no-record-align` opt-out:** for users who want tightest ratio and don't plan to use shuflr's random-access modes. Documented as such — frames will split mid-record and consumers must stitch across frames.

### 2.2 Metadata skippable frame (shuflr-specific)

First frame in the output is a skippable frame (magic `0x184D2A50`) containing a length-prefixed CBOR blob:

```
{
  "shuflr_version": "0.1.0",
  "created": "2026-04-22T21:00:00Z",
  "source": [
    { "path": "edgar-0.05.jsonl.gz", "size": 33285734912, "blake3": "…" }
  ],
  "convert_params": {
    "level": 3,
    "frame_size": 2097152,
    "long": 27,
    "record_aligned": true,
    "checksum": true
  },
  "stats": {
    "input_records": 1243567,
    "input_bytes": 119384729471,
    "output_frames": 56893,
    "output_bytes": 28472849217
  }
}
```

Disabled with `--no-embed-metadata`. Standard zstd decoders ignore skippable frames; shuflr's reader surfaces the metadata via `shuflr info`.

## 3. Throughput and Parallelism

zstd level 3 compresses ~450–600 MB/s per core; seekable-format overhead is ≤ 2%. With `-T0` (all cores), expect **~5–8 GB/s** aggregate on modern server hardware.

**Pipeline:**

```
┌────────────┐   ┌────────────┐   ┌────────────┐   ┌────────────┐
│ Input      │──▶│ Decompress │──▶│ Frame      │──▶│ Compress   │──▶ output
│ reader     │   │ + newline  │   │ splitter   │   │ worker     │
│ (pread)    │   │ scan       │   │ (align \n) │   │ pool (T)   │
└────────────┘   └────────────┘   └────────────┘   └────────────┘
                      sync              sync           workers
```

- Decompression is serial (streaming decoders are single-threaded).
- Frame splitter buffers up to `--frame-size` bytes + next-newline, hands each chunk to an idle compress worker.
- Workers compress their frame in parallel; output frames are **reassembled in submission order** by a single writer thread so seek-table offsets are correct.
- Seek-table entries are recorded as frames complete; table flushed at end.

**Estimated wall times** on 32-core server:
- `edgar-0.05.jsonl.gz` (31 GB gz, ~110 GB uncompressed) → ~20 s
- `document_sample.dedupe.jsonl.gz` (610 GB gz, ~2 TB uncompressed) → ~6 min
- `sample.jsonl.zst` (1.2 GB zst, 5.15 GB uncompressed) → ~1 s

The decoder (decompression of the input) is often the bottleneck. A `--input-threads N` knob for parallel decompression of multi-file inputs is a v1.x addition.

## 4. Verify Mode (`--verify`)

After the writer completes, `--verify` re-opens the output and performs:

1. Parse seek table; assert frame count and sum(decompressed_size) match accumulated totals.
2. Decompress every frame and verify XXH64 per frame.
3. Decompress frame 0 and the last frame; assert the first and last *records* match the first and last records of the original input.
4. Optional (future): full content blake3 comparison — requires reading every byte of both input and output; costs O(input) extra I/O.

Exit 0 on success, 73 on any mismatch. `--verify` is off by default (adds ~30% runtime); on in CI integration tests.

## 5. `shuflr info` (new helper subcommand, v1)

Promoted for free from 003's metadata discussion:

```
shuflr info FILE.jsonl.zst

file:        edgar-0.05.jsonl.zst
format:      zstd-seekable (record-aligned)
frames:      56,893
compressed:  26.52 GiB
decompressed: 111.20 GiB  (ratio 4.19)
records:     1,243,567 (inferred from frame decompressed sizes + record-align)
frame size:  median 2.0 MiB, min 1.98 MiB, max 8.21 MiB (1 oversized record)
checksum:    XXH64 per frame
created:     2026-04-22T21:00:00Z by shuflr 0.1.0
source:      edgar-0.05.jsonl.gz (31.00 GiB, blake3 abc…)
seekable:    ✓ (direct chunk-shuffled / index-perm compatible)
```

Reads only the seek table and metadata skippable frame (~100 KB); runs in < 10 ms on any size file.

## 6. Integration With Existing Modes

### 6.1 Reader side (v1)

`shuflr stream --shuffle=chunk-shuffled file.jsonl.zst` where `file.jsonl.zst` is zstd-seekable **works in v1**. The reader:

1. Opens file, parses the trailing seek table skippable frame.
2. Optionally parses the leading metadata frame.
3. Treats each zstd frame as a unit of I/O. Chunk size rounds up to the next whole number of frames.
4. Decompresses frames into scan buffers on demand; M active scan buffers = M × average_frame_size of RAM.
5. Runs the normal shuffle-core pipeline (002 §1) over the decompressed frame contents.
6. Since frames are record-aligned, **boundary reconciliation is unnecessary for seekable-zstd inputs** — the shuffle-core treats each frame as a self-contained unit and never needs to cross frame boundaries.

`index-perm` on seekable-zstd: the index-build pass streams through all frames in order, recording `(frame_id, offset_within_frame)` for each record (still 8 bytes/record, or 12 for frame_id + offset). Index persisted next to the `.zst` as `.jsonl.zst.shuflr-idx`.

### 6.2 Writer side (v1)

`shuflr convert` is the writer. Users produce seekable files once; every subsequent `shuflr stream`, `shuflr serve`, `shuflr index`, `shuflr analyze` invocation gets full random-access modes.

### 6.3 CLI subcommand table (updated 002 §6.1)

| Command | Purpose | Changed |
|---|---|---|
| `shuflr [OPTIONS] [INPUT...]` | Default: `stream` to stdout | |
| `shuflr stream [...]` | Explicit form | |
| `shuflr serve [...]` | gRPC service | |
| `shuflr analyze [...]` | Source-order locality warning | |
| `shuflr index [...]` | Build `.shuflr-idx` for `index-perm` | |
| `shuflr verify [...]` | Validate JSONL framing | |
| **`shuflr convert [...]`** | **Re-encode to zstd-seekable** | **new in 004, v1** |
| **`shuflr info FILE`** | **Summarize a seekable file** | **new in 004, v1** |
| `shuflr completions <SHELL>` | Emit shell completions | |

## 7. Crate Dependencies

Adds to `crates/shuflr/Cargo.toml`:

```toml
[dependencies]
# already present:
# zstd (optional via `zstd` feature)

# Option A — pure-Rust manual seekable construction on top of the zstd crate.
#   Pros: full control over frame boundaries (newline alignment), no extra C dep.
#   Cons: we implement the seek-table framing ourselves (~200 LOC).

# Option B — wrap libzstd's zstd_seekable.h via bindgen.
#   Pros: reference implementation, well-tested.
#   Cons: libzstd_seekable is contrib-level, not always packaged, extra build step.

# Option C — use an existing crate if one is healthy (e.g., "zstd-seekable-s3", "zeekstd").
#   Pros: someone else's problem. Cons: unknown maintenance.
```

**Recommendation:** Option A. The seekable format is ~100 lines of framing around standard zstd frames, and we need newline-aware frame closure anyway (which a generic seekable writer doesn't offer). Implementation lives in `shuflr::io::zstd_seekable::{Writer, Reader}`.

**Feature flag:** existing `zstd` feature. No new flag; `convert` becomes available iff `zstd` feature is on. Default: `zstd` in the default feature set (revisit from 003 Open Question #1 — with `convert` in v1, `zstd` is load-bearing for the primary user flow, so default-on).

## 8. Updates to 002 and 003

### 8.1 002 §13 Scope Summary

Move to **Ships in v1:**
- `convert` subcommand: JSONL / gz / zstd / bz2 / xz → zstd-seekable, record-aligned frames
- `info` subcommand: summarize seekable files
- Seekable-zstd **reader**: direct `chunk-shuffled` / `index-perm` on `.jsonl.zst` inputs (previously v1.1)

Remove from **Deferred:**
- "Seekable zstd input (v1.1)" — now v1
- Drop 003's separate treatment of seekable-zstd as v1.1

### 8.2 003 §6 (v1 decision table)

Replace the "zstd streaming only" row with:

| Extension | v1 compatibility |
|---|---|
| `.jsonl.zst` (seekable, record-aligned) | **all modes** — stream + chunk-shuffled + index-perm |
| `.jsonl.zst` (monolithic/non-seekable) | stream-only modes (none/buffer/reservoir) |
| `.jsonl.gz`, `.bz2`, `.xz` | stream-only modes |

Detection at open time: parse trailing 17 bytes for seekable magic; fall back to streaming mode if not present.

### 8.3 003 §10 (corpus recommendation)

Rewrite step 2 of the EDGAR workflow:

```
Before any training run:
  shuflr convert document_sample.dedupe.jsonl.gz \
                 -o document_sample.dedupe.jsonl.zst \
                 -l 3 -f 2MiB -T0 --verify
```

One-command conversion. Expected wall time: ~10 min for the 610 GB gz on 32 cores. Output: ~550 GB `.jsonl.zst`, seekable, record-aligned, integrity-verified.

## 9. Implementation Order

First implementation PR (per 002 §16) was targeted at `shuffle::none` + `io::pread` + CLI skeleton. **Revised order with `convert` in v1:**

1. **PR-1** — CLI skeleton (`clap` argument parsing, subcommand dispatch, `--help`/`--version`, exit codes). No real work yet; every subcommand is a stub that returns `unimplemented!()`.
2. **PR-2** — `stream --shuffle=none` with plain `pread` on uncompressed JSONL. End-to-end passthrough. First throughput number.
3. **PR-3** — streaming decompressors behind `gzip` / `zstd` / `bz2` / `xz` features; `stream --shuffle=none corpus.jsonl.gz`. Validates the input-source abstraction.
4. **PR-4** — `shuflr convert` basic: plain JSONL → zstd-seekable, single-threaded, record-aligned. `shuflr info`.
5. **PR-5** — multithreaded compression in `convert`; `--verify` mode. First real perf number.
6. **PR-6** — `convert` with compressed inputs (gz/bz2/xz → zst-seekable).
7. **PR-7** — seekable-zstd **reader** + `stream --shuffle=chunk-shuffled corpus.jsonl.zst`. First random-access mode working end-to-end.
8. Subsequent PRs follow 002 §16's roadmap for `index-perm`, `buffer`, `serve`, etc.

PR-4 + PR-7 together complete the primary user flow: **convert once, shuffle forever.**

## 10. Open Questions

1. **Seek-table format: standard 32-bit or long 64-bit?** Standard zstd-seekable limits any individual frame to 4 GB compressed / 4 GB decompressed. With 2 MiB frames this is never reached. Lean standard format; switch to long on opt-in if a frame exceeds the limit (e.g., a single 8 MB record at level 1).
2. **Block-vs-frame terminology.** zstd uses "block" internally (within a frame) and "frame" as the seekable unit. shuflr should consistently say "frame" when referring to seek units to avoid collision with "chunk" (the shuffle unit). Adopted in this doc.
3. **`--verify` — full content check, or end-to-end sampling?** Full is slow; 3-sample (first/middle/last) is fast and catches 99% of bugs. Lean: 3-sample by default; `--verify=full` for CI.
4. **Symmetric `shuflr decompress` subcommand?** Users can `zstdcat`. Skip for v1; keep shuflr focused on shuffle + convert.
5. **Parallel decompression of multi-input conversion.** When the user gives `shuflr convert a.gz b.gz c.gz -o out.zst`, serial reading is simplest but leaves cores idle. v1.x can parallelize decompression across inputs; for v1, document that input-parallelism is sequential.
6. **Crate choice: rebuild seekable format or use existing crate?** Decide by PR-4. Default plan: implement inline in `shuflr::io::zstd_seekable` for control over newline alignment.
