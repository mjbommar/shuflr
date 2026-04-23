# shuflr — Compression Formats (Design Amendment)

**Status:** Amendment to 002. Supersedes §4.4 of 002.
**Date:** 2026-04-22
**Prior:** `docs/design/002-revised-plan.md` §4.4 (compression deferred to v1.1; seekable-zstd only)

## Why this amendment

002 §4.4 deferred all compression to v1.1 and committed only to `zstd_seekable`. That was correct-in-theory but wrong in practice for the primary test corpus: all three EDGAR files on the target machine are `.gz`, ranging 8.2 GB → 610 GB. Without any v1 compression story, every smoke test requires a full decompress-to-disk first — for the 610 GB file, that's ~3 TB of scratch storage and an hour+ of CPU before shuflr does anything.

This amendment:
1. Promotes **streaming decompression** of `gzip`, `zstd`, `bzip2`, `xz` to v1 (feature-gated, compatible with `--shuffle=buffer` and `--shuffle=none` only).
2. Keeps **seekable decompression** at v1.1, with `zstd_seekable` as primary and **BGZF** added as a secondary option.
3. Rules out post-hoc "seekable gzip" tooling entirely.

## Codec Comparison

Rough numbers for text/JSONL workloads on modern x86. Single-thread unless marked mt.

| Codec | Ratio (JSONL) | Compress MB/s | Decompress MB/s | Mem (decomp) | Seekable? | Rust crate |
|---|---|---|---|---|---|---|
| **gzip** (DEFLATE) | 25–35% | 50–100 | 300–500 | ~32 KiB | **No** (see BGZF) | `flate2` |
| **bzip2** | 25–32% | 5–10 | 30–50 | ~8 MiB | Block-recover (awkward) | `bzip2` |
| **xz / LZMA2** | 15–22% | 2–5 | 60–100 | ~65 MiB | Yes (multi-block) | `xz2` / `liblzma` |
| **zstd** (-3) | 22–30% | 400–600 | 1200–2000 | ~8 MiB | **Yes** (zstd_seekable) | `zstd` |
| **zstd** (-19) | 18–22% | 20–40 | 1200–2000 | ~64 MiB | Yes (zstd_seekable) | `zstd` |
| **zstd_seekable** (-3, 2 MiB frames) | 25–32% | 350–550 | 1200–2000 | ~8 MiB × frames | **Native** | `zstd-seekable` |
| **BGZF** (bgzip, 64 KiB blocks) | 28–38% | 80–200 mt | 400–800 | ~32 KiB | **Native** (.gzi sidecar) | `noodles-bgzf` |
| **lz4** | 45–55% | 500–800 | 2000–4000 | ~64 KiB | Via custom frame index | `lz4_flex` |
| **snappy** | 50–60% | 400–600 | 1500–3000 | ~64 KiB | Weak | `snap` |
| **brotli** (q=6) | 22–28% | 30–80 | 300–500 | ~16 MiB | No | `brotli` |

## Seekability Taxonomy

Seekable = "jump to decompressed byte offset N without decoding from 0". This is what `chunk-shuffled` and `index-perm` need.

### Natively seekable

- **`zstd_seekable`** (Facebook, official). Wraps zstd in frames with a trailing index (per-frame compressed + decompressed sizes). Seek = binary-search the index, decode one frame. Frame size configurable (default 2 MiB). Ratio cost: **~3–5% vs. monolithic zstd at the same level.** First-class integration with the `zstd` crate via `zstd-seekable`.
- **BGZF** (Blocked GZip Format). From `samtools`/genomics. Each ≤ 64 KiB chunk is a self-contained gzip stream with an 18-byte header marker. Sidecar `.gzi` index is 8 bytes × block-count. **Every BGZF file is a valid gzip file** — standard `gunzip` works. Ratio cost: ~5–10% vs. plain gzip. Pervasive in bioinformatics (BAM/VCF/CRAM).
- **xz with `--block-size`**. The `.xz` container is a sequence of independently-decodable blocks with a trailing index. Seek = find block, decode it. Works out of the box with xz-utils. Best ratio of any seekable format; slowest to compress.

### Seekable with external tooling (not natively)

- **bzip2 block recovery.** Blocks are implicit at 900 KiB granularity; `bzip2recover` can scan them. Fragile, rarely deployed. Not worth it.
- **pigz `--independent` / `gztool` / `rapidgzip`.** Build a post-hoc seek index over a plain gzip file by scanning it. The scan is ~full decompression cost. Useful for one-off re-use; not an architecture.
- **lz4 frame format with block index.** Supported by various custom tools; no standard format. Each implementation rolls its own.

### Not seekable at all

- **Plain gzip**. DEFLATE is a continuous bitstream. Fundamentally sequential.
- **brotli**. Designed for HTTP content encoding. No seek story.
- **snappy**. Has a frame format but no standard seek index.
- **lz4 streaming**. Sequential unless wrapped in custom framing.

## JSONL-Specific Benchmarks (representative)

For a 100 GB EDGAR-style corpus (documents + metadata + `text` field):

| Codec / level | Compressed size | Compress wall time (1 core) | Decompress wall time (1 core) |
|---|---|---|---|
| gzip -6 | ~28 GB | ~20 min | ~5 min |
| zstd -3 | ~26 GB | ~4 min | ~70 s |
| zstd -19 | ~19 GB | ~80 min | ~70 s |
| zstd_seekable -3 (2 MiB) | ~27 GB | ~5 min | ~70 s (any-offset) |
| xz -6 | ~17 GB | ~8 hr | ~20 min |
| BGZF (gzip, 64 KiB blocks) | ~31 GB | ~15 min mt | ~4 min |

**Punchline:** for LLM-corpus workloads, zstd at -3 is strictly better than gzip along every axis except ubiquity. zstd_seekable pays 3–5% ratio for O(1) random access. BGZF is the right choice when gzip compatibility is required (existing `.gz` consumers).

## Decision Matrix for shuflr

### v1 (streaming decompression only)

Input formats accepted via streaming decoder, **compatible only with `--shuffle=buffer`, `--shuffle=reservoir`, and `--shuffle=none`**. Random-access modes (`chunk-shuffled`, `chunk-rr`, `index-perm`) refuse compressed inputs with a clear error message.

| Extension | Feature | Crate | Notes |
|---|---|---|---|
| `.gz`, `.gzip` | `gzip` | `flate2` (default `miniz_oxide` backend) | Primary near-term target — matches the EDGAR corpus |
| `.zst`, `.zstd` | `zstd` | `zstd` | Streaming mode; not seekable in v1 |
| `.bz2`, `.bzip2` | `bzip2` | `bzip2` (libbzip2 wrapper) | Legacy corpora |
| `.xz` | `xz` | `xz2` (liblzma wrapper) | Legacy / archival |
| `.lz4` | `lz4` | `lz4_flex` | Rare for corpora |
| (no ext) or stdin | auto-detect | magic-bytes sniff | `zcat file.gz \| shuflr` keeps working |

Format auto-detection at open time via magic bytes (gzip: `1f 8b`, zstd: `28 b5 2f fd`, bzip2: `42 5a 68`, xz: `fd 37 7a 58 5a 00`, lz4 frame: `04 22 4d 18`). Extension is a hint only.

**CLI behavior:**
- `shuflr --shuffle=chunk-shuffled corpus.jsonl.gz` → error: `shuflr: chunk-shuffled requires uncompressed or zstd_seekable input; found gzip. Decompress first, or re-compress with 'zstd --seekable -B2M'.`
- `shuflr --shuffle=buffer:200000 corpus.jsonl.gz` → works.
- `shuflr --shuffle=none corpus.jsonl.gz | head` → works.
- `zcat corpus.jsonl.gz | shuflr --shuffle=reservoir:100000` → works (stdin auto-detects).

**Implementation footprint:** ~150 LOC for the dispatcher + one `BufReader<GzDecoder<File>>` wrapper per codec. Each codec is behind its own feature flag; `gzip` on by default, others opt-in.

### v1.1 (seekable decompression)

Adds random-access compatibility for two formats:

| Extension | Feature | Crate(s) | Sidecar |
|---|---|---|---|
| `.zst` with seekable frames | `zstd-seekable` | `zstd-seekable` (+ `zstd`) | None (index is in-file trailer) |
| `.gz` with BGZF framing + `.gzi` | `bgzf` | `noodles-bgzf` | `<file>.gzi` (~8 bytes/block) |

**Detection:**
- zstd_seekable: scan trailing 17 bytes for the seek-table magic `b1 ea 92 8f` + check trailer frame. Transparent upgrade — any zstd_seekable file is also a valid regular zstd stream, and any regular zstd input falls back to streaming mode.
- BGZF: each block starts with a gzip header containing the `BC` extra-field subfield. Detect by reading the first gzip member's extra fields. Without a `.gzi`, treat as plain gzip stream.

**Chunk-boundary invariant:** for seekable compressed inputs, shuflr chunk boundaries must align to frame/block boundaries. This tightens `--chunk-size` to be rounded up to the next frame/block boundary at open time (logged as an info message). Boundary reconciliation (002 §5) still works but its extra `pread` ranges are also frame-aligned.

**xz multi-block** is a candidate for v1.2 — the `.xz` container already has an index; decoder throughput (~80 MB/s) is the only reason not to ship it now. Users who need archival-grade compression will appreciate it eventually.

### Never

**Post-hoc seekable-gzip.** Tools like `gztool` and `rapidgzip` build seek indexes by scanning an existing plain-gzip file. The scan is O(decompressed size) — you paid the full decompression cost once to avoid paying it later. Not worth the support burden, the edge cases, or the user confusion. The shuflr answer to "how do I make my .gz file seekable" is always **"re-compress once with `zstd --seekable -B2M` or `bgzip`"**.

## Corpus Recommendation (for the EDGAR test data)

Given `edgar-0.05.jsonl.gz` (31 GB gz), `index.jsonl.gz` (8.2 GB gz), `document_sample.dedupe.jsonl.gz` (610 GB gz):

1. **Right now, for smoke tests and benchmarks:** use the v1 streaming decompressor path (`gzip` feature). `shuflr --shuffle=buffer:200000 edgar-0.05.jsonl.gz` works once the first implementation PR lands.
2. **Before any training run:** one-time re-compress to `zstd_seekable -3` with 2 MiB frames. Rough cost on the 610 GB gz file: ~5 hours of 1-core time (sequential decompress-and-recompress), ~2 hours if pipelined to a fast disk. Output is ~550 GB of `.jsonl.zst` that decompresses 3× faster than gz, seekable, and works with `chunk-shuffled` + `index-perm`.
3. **Going forward, new corpora:** standardize on `.jsonl.zst` (seekable). Drop gzip for archival storage; keep it only as an input format for compatibility.

Recompress command (pipelined, single command):

```bash
pigz -dc document_sample.dedupe.jsonl.gz \
  | zstd --long=27 -B2M --adapt=min=1,max=3 -T0 \
         -o document_sample.dedupe.jsonl.zst
# Note: zstd with --seekable requires the zstd-seekable tool (separate binary);
# the main zstd CLI does NOT produce seekable frames by default. Use
# `zstdmt --seekable` (build with seekable support) or the shuflr helper:
#   shuflr compress --zstd-seekable -B2M -l3 -T0 -o out.jsonl.zst in.jsonl
# (helper subcommand to be added in v1.1 alongside the seekable reader.)
```

Add a `shuflr compress` subcommand in v1.1 that wraps `zstd-seekable` so users don't need to hunt for the right tool.

## Updates to 002

Rewrite 002 §4.4 as:

```
### 4.4 Compression

v1 streaming decompression — compatible with --shuffle ∈ {none, buffer, reservoir} only.
  Auto-detect via magic bytes; fail clearly on random-access modes.
  Feature flags: gzip (default, flate2), zstd (streaming only), bzip2, xz, lz4.

v1.1 seekable decompression — compatible with all shuffle modes.
  Primary: zstd_seekable. Official format, 2 MiB frames, ~3–5% ratio cost.
  Secondary: BGZF (.gz + .gzi sidecar). Gzip-compatible, ~5–10% ratio cost.
  Plain gzip is never made seekable post-hoc. Users re-compress.
  Chunk boundaries align to frame/block boundaries on compressed inputs.

Never: post-hoc seekable-gzip via scan indexing.
```

Also update 002 §13 Scope Summary:
- Move "Seekable zstd input" → mostly-deferred; keep as v1.1
- Move "Gzip input" from v1.1 → v1 (streaming-only).
- Add explicit "non-seekable zstd/bzip2/xz/lz4 streaming" to v1 behind feature flags.
- Add new v1.1 line: "BGZF seekable input" alongside zstd_seekable.

Update 002 §10.2 feature-flag table: `gzip` on by default (was deferred). `zstd` feature now means "streaming zstd in v1, seekable zstd in v1.1" — keep one feature flag.

## Open Questions

1. **Default feature set for `cargo install shuflr`.** 002 had `default = []`. With this amendment, `gzip` probably deserves default-on (very common, tiny binary-size cost with `miniz_oxide` backend). Others stay opt-in. Confirm.
2. **`miniz_oxide` vs `zlib-ng` backend for `flate2`.** `miniz_oxide` is pure Rust (no C dep); `zlib-ng` is ~1.5× faster but requires a C compiler. Lean `miniz_oxide` for default; add a `gzip-native` feature for `zlib-ng`. Decide before first implementation.
3. **Does `shuflr compress` belong in v1 or v1.1?** If we ship streaming decompressors in v1, symmetric compression is cheap (~50 LOC per codec). But it expands the tool's scope (shuflr is a shuffler, not an encoder). Lean: v1.1, alongside the seekable zstd reader, as a helper for the "re-compress to make seekable" workflow.
4. **BGZF auto-detection without a `.gzi` sidecar.** BGZF files without `.gzi` are decodable as plain gzip but not seekable. Do we (a) treat missing `.gzi` as "BGZF in stream-only mode", (b) auto-generate `.gzi` on first open, or (c) require `.gzi` and fail loudly? Lean (c) for v1.1 — auto-generation is a surprise behavior.
5. **`.lz4` — worth the feature flag?** Extremely rare for text corpora. Could be cut entirely from v1 and added later on demand. Lean: cut.
