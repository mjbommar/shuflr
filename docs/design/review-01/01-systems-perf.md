# shuflr v1 Design Review — Systems / I-O Performance

*Reviewer: senior systems perf engineer. Lens: page cache, TLB, prefetcher, syscalls, NVMe queue depth, NUMA, SIMD.*
*Reviewing: `docs/design/001-initial-plan.md` (iteration 1).*

## TL;DR

- **mmap is the wrong default** for a once-read, once-emit workload on modern NVMe. It pollutes the page cache, pays a minor fault per 4 KiB, costs TLB, and caps you well below 1 GB/s on a single thread. Default should be `pread` with a user-space read-ahead ring; keep `--mmap` as an opt-in for small files and boundary-inspection modes. `--direct` (O_DIRECT + io_uring) is what actually hits and exceeds 1 GB/s.
- **256 MiB × M=8 = 2 GiB of resident mapping is too large and too coarse.** It defeats the kernel's adaptive read-ahead window (`/sys/block/*/queue/read_ahead_kb`, typically 128 KiB), wrecks NUMA locality on dual-socket hosts, and chokes page faults under a cold cache. A 16–32 MiB chunk with M = 4–8 matches NVMe optimal QD and the L2/L3 working-set better.
- **The "10 GB/s memchr" claim is cache-bound, not realistic end-to-end.** `memchr3` sustains ~10 GB/s from L1/L2, not from a cold mmap. Overlapping the newline scan with the *previous* chunk's emission in a pipelined prefetch thread is the correct architecture; the design hand-waves it.
- **"RSS bounded by active-chunks × chunk-size" is false** for mmap without explicit `MADV_DONTNEED` after emission. The page cache retains every touched page until reclaim. Users *will* see RSS drift to tens of GiB.
- **Zero-copy for shuffled output is achievable** with `vmsplice(SPLICE_F_GIFT)` → `splice` to socket, or `io_uring` send with `IORING_OP_SEND_ZC` on registered buffers. The design dismisses it too quickly.

## Strengths

- Chunked riffle + boundary jitter is a sensible, pragmatic default — avoids the index build entirely while staying within O(M) memory. Good call.
- Bounded crossbeam channels between chunk-pool / shuffle-core / sink give natural backpressure without explicit flow control. Correct shape.
- `rand_chacha::ChaCha20Rng` seeded everywhere is the right reproducibility posture; no `thread_rng()` landmines.
- The "defer seekable zstd to v1.1" call is right — `.zst` framed indexing is a real project in itself.
- `--shuffle=none` as a fast-cat is the right escape hatch when a user just wants throughput.

## Concerns and Gaps

### 1. mmap as the default access mode — `001-initial-plan.md:27,74`

The doc lists `--mmap` / `--pread` / `--direct` with default `--mmap`. For a workload that reads each byte exactly once and should *not* evict other processes' hot pages, this is the wrong default.

- mmap page faults are synchronous minor faults at 4 KiB granularity. On a cold file, even with `MADV_SEQUENTIAL` + `MADV_WILLNEED`, you still incur a trap and a TLB insert per page. At ~250 K pages/GiB that's overhead you don't pay with a 1 MiB `pread`.
- `MADV_SEQUENTIAL` on Linux just *doubles* the file's read-ahead window (see `mm/madvise.c`); it does not turn mmap into async I/O. `MADV_WILLNEED` queues blocking readahead on the calling thread — not free.
- `MADV_DONTNEED` after emission is required to bound RSS. The plan doesn't mention it. Without it, the page cache keeps every page until reclaim, and on a machine also running training jobs you'll thrash their working sets.
- On NVMe with QD=32, a single thread doing `pread` into a pipelined ring of 1–2 MiB buffers tops ~3–4 GB/s. io_uring with `IORING_SETUP_SQPOLL` + `O_DIRECT` + registered buffers + fixed files gets to device line rate (7 GB/s on a PCIe 4.0 x4 drive).

**Recommendation:** make `--pread` with a 16-entry ring of 2 MiB buffers the default. Keep `--mmap` for introspection and small files (< 256 MiB). Add `--direct` (io_uring + `O_DIRECT`) as the opt-in fast path with a clear note that it requires sector-aligned buffers and bypasses the page cache (a feature, not a bug, for this workload). Call out macOS: no `O_DIRECT`, io_uring absent — fall back to `pread` + `fcntl(F_NOCACHE)`.

### 2. 256 MiB chunks × 8 active = 2 GiB — `001-initial-plan.md:87,172`

256 MiB × 8 is not a principled choice. Concerns:

- Linux default `read_ahead_kb` is 128 KiB; some distros bump to 1–2 MiB. A 256 MiB region does not *help* the prefetcher — it just means when you fault page N, the kernel reads N..N+32 pages. The chunk size should match the I/O unit the scheduler submits, not the logical partition.
- 2 GiB of mappings blows past any reasonable L3 (typically 32–64 MiB on server parts, 128 MiB on Genoa). The newline scan will run entirely off DRAM and the "10 GB/s memchr" number collapses.
- On a 2-socket box, a single-threaded mapper with 2 GiB touched will drift allocations across NUMA nodes. No `mbind`/`set_mempolicy` mentioned.
- 256 MiB is awkward for THP: Linux THP is 2 MiB (4 KiB × 512). `mmap` of file-backed pages does not get THP on most filesystems (only anonymous or with FS_HUGE support). The large chunk gives zero THP benefit.

**Recommendation:** default chunk size = **16 MiB**, `--active-chunks` = **8**, total resident ≈ **128 MiB**. Separate the *logical* shuffle chunk (where interleaving happens — can still be larger, e.g. 256 MiB) from the *I/O chunk* (2 MiB reads). Document both. Add an `--io-block` knob. Expose a "tuning" section that says: match `--io-block` to `cat /sys/block/$dev/queue/optimal_io_size` × 2.

### 3. memchr throughput is cache-bound — `001-initial-plan.md:38,184`

Open-question #1 cites "~25 ms per 256 MiB chunk" for the scan, implying ~10 GB/s. That's correct for `memchr` out of L2, but **not** for a just-faulted-in mmap region. End-to-end you'll measure 2–4 GB/s because the scan is the thing pulling pages from the device — the scan *is* the read.

The right architecture is a two-stage pipeline:

1. I/O thread: issues readahead (`posix_fadvise(POSIX_FADV_WILLNEED)` or io_uring read) for chunk N+1 while the consumer is scanning chunk N.
2. Scan thread(s): `memchr` on warm pages, produces offset array.
3. Emit thread: interleaves and writes.

With that pipeline the scan *can* run at 10 GB/s because by the time it touches chunk N+1, the pages are in cache. The doc says "scheduler prefetches" in a diagram caption but doesn't spell this out as a hard requirement.

**Recommendation:** spec the prefetch → scan → emit pipeline in the design doc, not just in an ASCII box. Name the exact syscall (`io_uring_prep_read` on `O_DIRECT` fd, or `posix_fadvise(WILLNEED)` + pread). Add a benchmark that measures *scan throughput under cold cache* vs. *warm cache*; the gap is the prefetch budget you need to hide.

### 4. Zero-copy to sockets beyond pure passthrough — `001-initial-plan.md` (no line, gap)

The doc mentions `sendfile(2)` only for passthrough. For shuffled output, zero-copy is still reachable:

- **`vmsplice` + pipe + `splice` to socket**: gift pages from the mmap into a pipe, splice to TCP. Works for any sub-range; you just `vmsplice` each line's slice. Saves one copy.
- **`io_uring` `IORING_OP_SEND_ZC`** with registered buffers and `MSG_ZEROCOPY` semantics: kernel holds a reference to your buffer until the NIC DMAs it; you get a completion when safe to reuse. Linux 6.0+. Combined with a ring of scan-owned buffers this is the cleanest path to 40 GbE without memcpy.
- **Batched `writev`** into a fixed set of iovecs pointing into the mapped chunk — not zero-copy but one syscall per batch and no memmove.

**Recommendation:** add an `--output-zc` (zero-copy) flag gated on Linux ≥ 6.0, implemented via io_uring `SEND_ZC`. For portability fall back to `writev`. Document that `sendfile` is useless for shuffled output (non-contiguous) — so the current "called out as passthrough only" is correct *as far as it goes*, but misses the better primitives.

### 5. Threading model and 40 GbE sink — `001-initial-plan.md:137`

crossbeam MPMC bounded channels are fine for MPSC use at ~1 M msgs/s. At 40 GbE with batch=128 and avg line=1 KiB, you're 40 Gbps / 1 KiB = 5 M msgs/s batched → 40 K batches/s. Channel throughput is not the bottleneck; **syscall rate on the sink is**.

- Single-threaded stdout sink with `println!` or line-by-line `write` at 40 Gb won't keep up. Must batch with `writev` or `io_uring`.
- False sharing: if the shuffle-core emits a `Vec<RecordRef>` and the sink thread dequeues, the `Vec`'s pointer, len, cap sit in one cache line with the next producer's metadata. Pad channel entries to 64 B, or use `crossbeam_utils::CachePadded`.
- Producer/consumer imbalance under mixed record sizes: one chunk with 10 M short records starves another with 10 K long ones. Use a `batch_bytes` driver, not `batch_count`, for the emit queue.
- gRPC at 40 Gb will be CPU-bound in `prost` serialization and TLS. Call this out. A raw length-prefixed framing via `--listen tcp://` will always beat gRPC; position gRPC as the *integration* path, not the *performance* path.

**Recommendation:** use `crossbeam_utils::CachePadded` on channel payloads. Add a batch-bytes trigger and a max-flush-interval timer. Explicitly document the stdout path as `writev` / `BufWriter::with_capacity(1 MiB)` flushed on batch boundary.

### 6. RSS accounting — `CLAUDE.md:50`, `001-initial-plan.md:172`

"RSS bounded by active-chunks × chunk-size (~2 GiB)" is wrong with mmap unless you:

1. `madvise(MADV_DONTNEED, addr, len)` on each chunk after emission, **or**
2. Use `mmap` with `MAP_POPULATE` off and explicit `munmap`, **or**
3. Use `pread`/`O_DIRECT` which bypasses the page cache entirely.

With the current plan's `MADV_SEQUENTIAL | MADV_WILLNEED`, RSS will climb to the smaller of (file size, free RAM) as the kernel caches everything you touch. For a 200 GiB file on a 256 GiB box you'll get >100 GiB RSS before reclaim kicks in, and `ps`/cgroup memory accounting will trigger OOM killers in containerized setups.

**Recommendation:** add `MADV_DONTNEED` on chunk retire as a mandatory step in the chunk-pool. Document that RSS bound holds only in `--pread` or `--direct` mode, or in `--mmap` with explicit retire. Add a `--rss-bound BYTES` safety flag that triggers `MADV_DONTNEED` proactively when approached.

### 7. Linux vs. macOS — design is silent

- `MADV_WILLNEED` exists on both; `MADV_DONTNEED` on macOS is *not* equivalent — it's a hint, and the Darwin kernel often ignores it. Use `MADV_FREE_REUSABLE` on macOS.
- `O_DIRECT` does not exist on macOS. Equivalent is `fcntl(fd, F_NOCACHE, 1)`.
- io_uring is Linux ≥ 5.1; macOS has no equivalent. Fall back to blocking `pread` pool or `kqueue` + aio.
- `posix_fadvise` is Linux-only.

**Recommendation:** add an OS-support matrix table to the design doc. Guard io_uring behind a `linux` cfg and a `io-uring` Cargo feature. Ship macOS with `pread` + `F_NOCACHE` as the performance mode.

### 8. Seekable zstd — `001-initial-plan.md:53`

"Seekable-zstd is v1.1" is the right deferral *if* you adopt the Facebook `zstd_seekable` format (frame-indexed). Caveats the design should pin now:

- Seekable frames cost ~3–5% compression ratio vs. monolithic zstd at level 3.
- Seek granularity = frame size; pick to match your chunk I/O block (e.g. 2 MiB frames).
- Decompression is ~1.5 GB/s per core with `zstd` level 3; at line rate the *decoder*, not I/O, becomes the bottleneck. Plan for one decoder thread per active chunk.
- Chunk boundaries must align to frame boundaries or you lose seekability; this tightens the boundary-jitter design (can only jitter by whole frames).

**Recommendation:** commit now to `zstd_seekable` frame-indexed format with 2 MiB frames. Document the 3–5% compression penalty and per-chunk decoder threading. This shapes v1 APIs (chunk boundaries must be frame-aligned if compressed).

## Recommendations to Adopt

1. **Switch default access mode** from `--mmap` to `--pread` with a 16-entry × 2 MiB ring. Keep `--mmap` for small files. Add `--direct` (io_uring + `O_DIRECT`) as the documented fast path.
2. **Reduce default chunk size** from 256 MiB to 16 MiB; separate logical-shuffle-chunk from I/O-block. Default `--active-chunks` stays at 8, resident ≈ 128 MiB.
3. **Spec the prefetch → scan → emit pipeline** explicitly in the design doc. Bench cold vs. warm scan throughput; treat the gap as the prefetch budget.
4. **Add `MADV_DONTNEED` on chunk retire** in `--mmap` mode. Document that the RSS bound claim only holds with this or with `--pread`/`--direct`.
5. **Add `--output-zc`** flag using io_uring `IORING_OP_SEND_ZC` with registered buffers, behind a Linux cfg and Cargo feature. Fall back to `writev`.
6. **Cache-pad channel payloads** with `crossbeam_utils::CachePadded`. Use byte-based batch triggers, not record-count only.
7. **Add an OS-support matrix** to the design doc covering madvise, O_DIRECT, io_uring, posix_fadvise. macOS gets `pread` + `F_NOCACHE` as perf mode.
8. **Commit v1.1 compression** to `zstd_seekable` with 2 MiB frames; make chunk-boundary jitter frame-aligned.

## Open Questions (need data)

- What is the target deployment: single-socket workstation, dual-socket server, or container with cgroup limits? NUMA pinning and RSS policy depend on this.
- What is the expected line-size distribution in the primary corpus (LLM training data)? If p99 ≫ mean, the fixed chunk-byte design needs a line-count safety valve.
- What kernel version floor is acceptable? io_uring `SEND_ZC` needs 6.0+; `IORING_SETUP_SQPOLL` (unprivileged) needs 5.13+.
- Are consumers typically on the same NUMA node / same host (UDS) or remote (TCP)? Changes the zero-copy calculus — UDS + `SCM_RIGHTS` + mmap handoff is a fourth option not considered.
- What fraction of users will actually run this on HDD / network FS? Those destroy every assumption above; the design should say "NVMe-optimized, HDD supported but untuned."
