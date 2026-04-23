# Benchmark 001 — EDGAR corpus, 31 GB gzip

**Date:** 2026-04-23
**Commit:** `37504f8` (PR-14, --limit / --sample-rate on convert)

## Host

- CPU: AMD Ryzen 7 7840HS (16 logical cores)
- RAM: 27 GiB total, ~16 GiB free at bench start
- Kernel: Linux 7.0.0-13-generic
- Rust: 1.96.0-nightly (48cc71ee8 2026-03-31), release build (LTO thin, codegen-units 1, panic=abort, strip=symbols)

## Storage

- **Corpus location**: `/nas3/data/corpus/us/edgar/edgar-0.05.jsonl.gz` on a network-mounted filesystem.
  - Observed read rate during full convert: **45–100 MB/s** (the real bottleneck for any operation that reads the raw gz).
- **Local scratch**: `./tmp/` on `/dev/nvme0n1p1` (NVMe, 855 GB free). All downstream ops (chunk-shuffled, analyze, info, verify) run from local NVMe.

## Input

| Property | Value |
|---|---|
| File | `edgar-0.05.jsonl.gz` |
| Gzipped size | 31 GB |
| Decompressed size | **192.68 GB** |
| Record count | **1,198,631** |
| Mean record size | **161 KB** |
| Max frame (single-record) | 427 MiB |

(EDGAR filings — highly variable record size; some single records >100 MB.)

## Results

All times include the convert's record sampling, frame splitting, multi-thread compress, seek-table write, and on-disk write.

| # | Operation | Wall | User | CPU-cores | Output | Decomp throughput | Notes |
|---|---|---|---|---|---|---|---|
| 1 | `convert --limit=100000` (head) | **9.55 s** | 29.6 s | 3.1 | 1.17 GB zstd | 553 MB/s | Early-EOF — reads ~5% of input |
| 2 | `convert --sample-rate=0.01 --limit=10000` | 202.7 s | 107.7 s | 0.5 | 113 MB zstd (10k rec) | 2.59 MB/s out | ~50% of input scanned before limit fires |
| 3 | `convert --sample-rate=0.001` (full scan) | 287.1 s | 196.4 s | 0.7 | 45 MB zstd (~1k rec) | 1.64 MB/s out | Entire input scanned, Bernoulli 0.1% |
| 4 | **`convert`** (full, --threads=0, 16 workers) | **700.4 s** (11.7 min) | 1094.6 s | 1.56 | 30.92 GB zstd | 262 MB/s decomp | **NFS-bottlenecked**, workers starved |
| 5 | `info ./tmp/edgar-0.05.jsonl.zst` | **0.01 s** | — | — | — | — | Reads seek-table trailer only |
| 6 | `stream --shuffle=chunk-shuffled --sample=10000` | 0.77 s | 0.50 s | 0.65 | 10k records | **1599 MB/s** decomp | local NVMe input |
| 7 | `stream --shuffle=chunk-shuffled --sample=1000000` | 106.8 s | 64.2 s | 0.60 | 1M records | 1428 MB/s decomp | local NVMe input |
| 8 | `stream --shuffle=chunk-shuffled` (full 1.2M) | 154.4 s | 80.6 s | 0.52 | 1,198,631 records | 1190 MB/s decomp | Reads entire 30 GB zstd |
| 9 | `analyze --sample-chunks=32` | 0.25 s | — | — | — | — | Verdict: **UNSAFE** |
| 10 | `convert --verify` (re-read local zstd → reconvert + verify) | 552.2 s | 1015.1 s | 1.84 | 30 GB zstd | 349 MB/s decomp | Full round-trip, all on local NVMe |

## Live snapshot during operation #4 (full convert)

At ~2 min into the run, 18 GB written:

- Aggregate CPU ≈ 173% (≈1.7 cores). 16 zstd compress workers mostly idle-waiting on the reader thread.
- Memory: RSS 859 MB, peak 2.7 GB, 18 threads.
- Disk: NVMe bursty writes 696 MB/s during frame flushes, ~29 MB/s steady output-file growth.
- NFS read: ~45–100 MB/s sustained (from `/proc/<pid>/io rchar` delta).

This is the signature of an I/O-bound single-threaded feeder: the 16 zstd-compress workers are ready but can't fire because the gz decoder on the reader thread runs out of bytes. **On local storage the full convert would likely hit 1–2+ GB/s decompressed throughput** — matches operation #8's chunk-shuffled rate of 1.19 GB/s reading the converted output.

## `analyze` output (EDGAR is extremely clustered)

```
analyze:       ./tmp/edgar-0.05.jsonl.zst
total frames:  28,695
sampled:       32
records seen:  1,242 (mean 178,812 B/record)
byte-KL max:   1.6095 nats  (SKEWED)
reclen CV:     3.965        (SKEWED)
verdict:       UNSAFE — source-order locality detected.
recommendation: use `shuflr index` + --shuffle=index-perm
                for a provably uniform shuffle.
```

**Interpretation**: the per-frame byte distributions carry ≈2.32 bits of per-byte information about *which frame* they're from — EDGAR frames cluster by filing type and chronology. chunk-shuffled on this file would preserve those clusters. For training data, `index-perm` (not available on compressed input; decompress the file to plain .jsonl first) or an explicit pre-shuffle is indicated.

## What we learned

1. **NFS is the ceiling, not the compress pipeline.** On local NVMe, the same convert should run 4–10× faster. The full-file chunk-shuffled hitting 1.19 GB/s reading the converted output confirms the I/O-side ceiling is much higher than 262 MB/s.
2. **`--limit N` is genuinely fast** — 100k records from a 31 GB gz in under 10 seconds. Stop-at-N means we read only as much input as we need.
3. **Bernoulli without `--limit` costs the full scan** — necessarily, since we have to touch every record to roll its dice. `--sample-rate=0.001` took 4.8 min on the 31 GB gz.
4. **Combined `--sample-rate + --limit` gives you "roughly P%, but stop early if you hit the cap first"** — the cap fires proportionally to your rate. 1% rate with 10k cap = ~1M records processed = ~50% of input.
5. **Post-convert `info` / `analyze` / `chunk-shuffled` are instant** relative to convert. Downstream operations benefit from the seek-table (`info` in 10 ms, `chunk-shuffled --sample=10k` in 0.77 s).
6. **Full-file `chunk-shuffled` of 30 GB zstd runs at 1.19 GB/s decompressed** — the zstd decode + frame-order Fisher-Yates + intra-frame Fisher-Yates + writev pipeline is not CPU-bound either (0.52 cores avg). We could parallelize frame decoding (currently single-threaded) to push this higher.
7. **EDGAR is an adversarial case for chunk-shuffled** — our cheapest signals (byte-KL per frame, record-length CV) fire at 1.61 nats / 3.97× mean respectively. This is both good news (the tool catches real clustering) and a warning for anyone training on EDGAR without a prior uniform shuffle.

## Open follow-ups

- Run the same matrix with input on **local NVMe** to isolate the NFS bottleneck (requires decompressing the 31 GB gz to a local 193 GB .jsonl once, ~5 min at 580 MB/s gz decode).
- Add a parallel frame-decode path to `chunk-shuffled` to push past the current 1.2 GB/s ceiling on large files.
- Add **Jensen-Shannon divergence** + per-frame **entropy** to `analyze` — both cheap additions to the existing 256-bin histogram pipeline; JS is symmetric and bounded [0, ln 2] so thresholds transfer across corpora.
- Extend `index-perm` to seekable-zstd inputs (today it only works on plain .jsonl) so we can recommend it without asking users to decompress first.
