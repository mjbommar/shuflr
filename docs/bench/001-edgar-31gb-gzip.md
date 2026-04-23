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

## Bench extension: local-NVMe input (isolating the NFS bottleneck)

Decompressed `edgar-0.05.jsonl.zst` via `zstdcat` to `./tmp/edgar-0.05.jsonl` on local NVMe. **Size: 192.68 GB exact, 1,199,612 records** (`wc -c` / `wc -l` confirm).

**Important finding during diagnosis**: `shuflr stream --shuffle=none` on the seekable file initially produced 82 GB / 1,198,631 records — missing 981 records and 110 GB of data vs. `zstdcat`'s 192.68 GB / 1,199,612. This is not a decoder bug: it's the default `--max-line=16MiB --on-error=skip` policy silently dropping 981 oversized records averaging 112 MB each. EDGAR has some filings that large. Behavior is correct by design but poorly-surfaced for benchmarking — consider upgrading `skip` to log a count at INFO level (currently goes to the per-stats counter only).

### Operation #11 — full convert from LOCAL plain JSONL (16 threads)

| Metric | Value |
|---|---|
| Wall | **346.77 s** (5.78 min) |
| User | 868.0 s (≈ 2.5 cores avg) |
| Sys | 82.4 s |
| Max RSS | 1.91 GB |
| Input | 192.68 GB plain JSONL (local NVMe) |
| Output | 30.92 GB zstd-seekable |
| Throughput | 530 MB/s decompressed |

**2.0× speedup over the NFS-input convert** (347 s vs 700 s). So removing NFS unblocked half the wall time. Still at 530 MB/s aggregate — the single-threaded reader + frame-splitter + drain on a 192 GB input is the next bottleneck (not NVMe itself, which sustains 3+ GB/s).

### Operation #12 — thread scaling (fixed --limit=200000, local plain input)

200k records ≈ 13.75 GB decompressed. Results:

| Threads | Wall | User | Speedup | Efficiency | Effective throughput |
|---|---|---|---|---|---|
| 1 | 37.35 s | 32.66 s | 1.00× | 100% | 368 MB/s |
| 2 | 16.61 s | 36.06 s | 2.25× | 112% | 828 MB/s |
| 4 | 8.88 s | 37.77 s | 4.21× | 105% | **1.55 GB/s** |
| 8 | **5.79 s** | 46.08 s | 6.45× | 81% | **2.37 GB/s** |
| 16 | 6.61 s | 52.94 s | 5.65× | 35% | 2.08 GB/s |

Key findings:

- **Scaling is near-linear up through 4 threads**; ≥4× at 4 threads, 6.4× at 8 threads.
- **16 threads is slower than 8.** The host is a Ryzen 7 7840HS (8P/16T) — hyperthreaded logical cores fight each other on compute-heavy zstd compression. For this workload, **8 threads is optimal**.
- **Peak local pipeline throughput: 2.37 GB/s decompressed** at 8 threads. That's the "real ceiling" on this host.
- The user-time barely scales past 4 threads (37 s → 46 s), confirming the reader/writer threads are the serialized choke point at higher thread counts.

### Revised bottleneck picture

| Input location | Wall | Limit |
|---|---|---|
| NFS gz (31 GB) | 700 s | NFS read (45-100 MB/s) |
| Local plain (192 GB) | 347 s | Reader thread on 192 GB file |
| Local plain, 8-thread optimum (200k records) | 5.79 s | Pipeline (~2.37 GB/s decomp) |

To push past 2.37 GB/s we'd need: parallel file reads (io_uring or multi-thread pread), wait-free frame-boundary detection, or both. None are in scope for v1.

## Open follow-ups (updated)

- **Surface `oversized_skipped` counters more loudly** — e.g. as a WARN log at end-of-run when non-zero, so users don't silently miss 100 GB of data under default settings.
- Add a parallel frame-decode path to `chunk-shuffled` to push past the current 1.2 GB/s ceiling on large files.
- Add **Jensen-Shannon divergence** + per-frame **entropy** to `analyze` — both cheap additions to the existing 256-bin histogram pipeline; JS is symmetric and bounded [0, ln 2] so thresholds transfer across corpora.
- Extend `index-perm` to seekable-zstd inputs (today it only works on plain .jsonl) so we can recommend it without asking users to decompress first.
- **Default `--threads=0` may be suboptimal on hyperthreaded CPUs.** Consider defaulting to `num_physical_cpus()` instead of `available_parallelism()` when the host reports more logical than physical cores.
- Parallel reader path: split a seekable input file into regions, `pread` in parallel, reassemble. Would push the local-plain ceiling higher than 2.37 GB/s.
