# Review 06 — Correctness & Statistics

*Reviewer persona: probability-minded correctness reviewer. I distinguish "uniform permutation" (Fisher-Yates on the full index) from "approximately shuffled" (lines from the same region co-occur more often than chance). I want quantitative models, coverage guarantees, and deterministic reproduction.*

## TL;DR

The plan's default `chunk-shuffled` is **not** a uniform permutation, and the current wording in §Core Algorithm ("qualitatively similar to a very large shuffle buffer") is slightly too generous. It is a *block-interleaved local shuffle* whose residual structure is bounded by chunk count `N` and active-chunks `M`, not by the total line count. The plan should (a) state this quantitatively, (b) stop conflating reservoir sampling with buffer shuffling, (c) pin down the PRF hierarchy for determinism, and (d) replace "recovered probabilistically" with a hard coverage guarantee. None of these are blockers; all are specification gaps.

## Strengths

- Seed discipline is correct: `rand_chacha::ChaCha20Rng`, explicit `--seed`, no implicit `thread_rng`. This is the hard thing to retrofit; it's in from day one.
- Epoch seed derivation is specified (`sha256(seed, epoch)`), which is the right primitive.
- Property-test discipline is declared up front (determinism, coverage within N epochs).
- Fisher-Yates is named for intra-chunk shuffling; no ad-hoc "swap N random pairs" nonsense.
- Three-mode menu (`chunk-shuffled`, `reservoir`, `index-perm`, future `2pass`) correctly acknowledges the quality/cost frontier.

## Algorithmic analysis

### 1. `chunk-shuffled` quality — a concrete model

Let `S` = file size, `L` = mean line length, `N = S/C` chunks of size `C`, `M` active chunks, `c = C/L` lines per chunk. Intra-chunk Fisher-Yates is uniform *within* a chunk. Interleaving across `M` chunks is a convex combination of per-chunk streams.

- **Displacement distribution.** A line at input position `i` lands in output position `j` drawn from: a uniform slot in its chunk (variance ≈ `c²/12`) plus a round-robin or weighted offset determined by interleave position. Expected absolute displacement is `Θ(c)`, not `Θ(S/L)`. **A line cannot move more than roughly `M·c` positions from its chunk-origin.** For defaults (`C=256 MiB`, `M=8`, `L=1 KiB`), `c ≈ 2.6·10⁵`, so a line moves at most ~2M positions. On a 100B-line corpus, that is ~0.002% of the file — orders of magnitude short of a uniform permutation.
- **Adjacency probability.** For originally-adjacent lines `i, i+1` in the same chunk, `P(adjacent in output) ≈ 1/(M·c) · M = 1/c` (they land adjacent iff both picked as consecutive from the same chunk's queue during interleave). For a uniform permutation this would be `2/(S/L)`. Ratio: `(S/L)/(2c) = N/2`. With defaults and 100 GiB input, `N ≈ 400`, so originally-adjacent lines are **~200× more likely to stay adjacent** than under a uniform shuffle.
- **Mutual information.** Input position and output position remain highly correlated through the chunk identifier. `I(input_pos ; output_pos) ≈ log₂ N` bits leak — that's exactly the chunk index. For `N=400`, that is ~8.6 bits of positional leakage per record.
- **Pathological inputs.** If the input is sorted by an attribute (document length, timestamp, source URL, language), each chunk contains a near-homogeneous slice and the interleave merely braids `M` homogeneous streams. A training loss curve will see this as non-stationarity in minibatch composition. The plan should warn users explicitly: **chunk-shuffled is not safe on sorted inputs; pre-randomize once with `index-perm` or `2pass`, then use `chunk-shuffled` for subsequent epochs.**

**Recommended wording change** in §Core Algorithm, Properties:

> Shuffle quality: *not uniform*. Output is a block-interleaved local shuffle. Lines are displaced by at most ~`M · lines_per_chunk` positions; originally-adjacent lines co-occur at roughly `1/lines_per_chunk` vs. `~2/total_lines` under uniform. Adequate for SGD on inputs that are already roughly in-order-random; **inadequate for inputs sorted by any attribute**.

### 2. Boundary loss and jitter

A line straddling a chunk boundary is truncated on both sides and dropped. Expected drops per epoch = `N - 1` (one per interior boundary), independent of line length distribution (so long as `L ≪ C`). For `N = 400` and 100B records, that's ~1 ppm loss per epoch — negligible *in aggregate* but **not zero for any specific line**.

Model jitter as: per epoch `e`, boundary `k` is displaced by `δ_{e,k}` drawn uniformly from `[-J, +J]` for some jitter window `J` (plan doesn't specify `J`). A specific boundary-straddling line of length `ℓ` at original boundary `k` is recovered in epoch `e` iff the jittered boundary does not fall inside its byte range. `P(recovery in one epoch) ≈ 1 - ℓ/(2J)` for `J ≫ ℓ`. After `K` independent epochs, `P(never recovered) ≈ (ℓ/(2J))^K`.

**This is a probabilistic guarantee, not a coverage guarantee.** For a training-data tool, I recommend stronger semantics: **every epoch should deterministically recover every line dropped by the previous epoch's boundaries.** Cheapest implementation: after intra-chunk emission, read `max_line` bytes past each interior boundary, find the first complete line that starts in chunk `k` and ends in chunk `k+1`, emit it at a deterministic interleave position. Cost: one extra `pread` of ≤ 16 MiB per boundary = `N·max_line` bytes = `≤ 6.4 GB` worst case at defaults. Almost always a single cache hit because `MADV_WILLNEED` already fetched the neighboring chunk head. Call this **deterministic boundary reconciliation**; document it alongside jitter.

### 3. Reservoir vs. buffer shuffle — the plan conflates two algorithms

The plan lists `reservoir` with `--reservoir-size K`. But Vitter's **Algorithm R** (1985) draws a uniform `K`-subset from an `N`-stream and **emits exactly `K` records**. That's a sampler, not a shuffler. A *buffer shuffle* (HuggingFace-style) keeps a `K`-sized ring, emits one record per input record by swap-and-evict, and emits all `N` records with approximately-local randomization (displacement bounded by `K`).

These produce different outputs for the same input. **Rename:**

- `--shuffle=reservoir --reservoir-size K` → *emits K records*, uniform subset, Vitter R.
- `--shuffle=buffer --buffer-size K` → *emits all N records*, local shuffle, displacement ≤ `K`.

If the intent was the latter, the current flag name is actively misleading to anyone who knows Vitter.

### 4. `index-perm`

A full Fisher-Yates over the offset table is the only mode that produces a **provably uniform permutation**. The plan should state this explicitly as the quality baseline.

RAM: `8N` bytes for `u64` offsets (plus per-line length if stored; `16N` if so). Crossovers at defaults:

| Lines | Index RAM |
|---|---|
| 1e7 | 80 MB — trivial |
| 1e9 | 8 GB — fine on a training box |
| 1e10 | 80 GB — needs big box or `2pass` |
| 1e11 | 800 GB — index-perm is unusable |

State this table (or equivalent prose) in the design doc so users can pick modes without benchmarking. Also: at 10B lines, the *build* cost of the index is a full sequential scan (~100 s at 1 GB/s) — that's the actual TTFB, not 50 ms.

### 5. `2pass` (Jane Street external-memory shuffle)

Correctly cited but **deferred**. The plan should make the implication explicit: *for files too large for `index-perm` and where `chunk-shuffled` quality is insufficient (e.g., sorted inputs), v1 has no solution.* Users in that regime must pre-shuffle with `terashuf` or wait for v1.x. Flag this as a known gap in §Scope rather than leaving readers to infer it.

### 6. Determinism across restarts

The plan nowhere states how to recompute record `k`'s output position from `(seed, k)` without replaying the RNG. **Essential requirement:** derive every random choice from a PRF rooted at `seed`, so any sub-computation is addressable.

Recommended hierarchy (all via `sha256` or `blake3` truncated to 32 bytes):

```
master_seed
├── epoch_seed        = H(master_seed, "epoch",  epoch_id)
│   ├── chunk_seed    = H(epoch_seed,  "chunk",  chunk_id)
│   │   └── (drives intra-chunk Fisher-Yates for that chunk)
│   ├── interleave_seed = H(epoch_seed, "interleave")
│   │   └── (drives round-robin / weighted interleave order)
│   └── jitter_seed   = H(epoch_seed, "jitter")
│       └── (drives boundary offsets)
├── shard_seed        = H(master_seed, "shard", shard_id)    # for partitioned inputs
└── client_seed       = H(master_seed, "client", client_id)  # for --independent
```

With this tree, a consumer restarting at `(epoch=3, chunk=17, intra_chunk_pos=42000)` recomputes exactly the needed permutation without replaying epochs 0–2. The plan currently mentions only `epoch_seed`; extend it.

## Testable properties (proptest-style, v1)

1. **Determinism.** `∀ input, seed, config: run(input, seed, config) == run(input, seed, config)` byte-for-byte, over ≥ 100 random (input, seed) pairs.
2. **Permutation (index-perm only).** `multiset(output_records) == multiset(input_records)` exactly; `len(output) == len(input)`; no duplicates; no drops.
3. **Coverage (chunk-shuffled, with deterministic boundary reconciliation).** For every line `ℓ` in the input, `ℓ` appears in every epoch's output. `∀ epoch: multiset(output_epoch) == multiset(input)`.
4. **Coverage (chunk-shuffled, jitter-only fallback).** For every line `ℓ`, `P(ℓ appears in at least one of K epochs) ≥ 1 - ε` for `ε = 10⁻⁶`, `K` computable from `(max_line, jitter_window)`. Proptest asserts empirical coverage over 1000 seeds stays within two-sided binomial CI.
5. **Epoch independence.** For two epochs `e₁ ≠ e₂` of the same input, the permutation distance (Kendall tau or Spearman footrule) exceeds a threshold with probability ≥ 1 - ε. Protects against a broken `epoch_seed_fn` that forgets to mix in the epoch id.
6. **Intra-chunk uniformity.** For a chunk of `c` lines over many seeds, the empirical distribution of "line 0's output slot within the chunk" is uniform on `[0, c)` by chi-squared (`p > 0.01`). Validates Fisher-Yates is actually implemented correctly.
7. **Adjacency chi-squared.** For a small synthetic input (`c = 100`, `N = 10`, `M = 4`), assert `P(line i adjacent to line i+1)` in the empirical output distribution over 10,000 seeds matches the closed-form `~1/c` to within ±10%. Fails loudly if interleave is broken.
8. **Seed-tree isolation.** Changing `chunk_seed` for one chunk id alters only that chunk's emission order; all other chunks emit byte-identical sequences. Validates the PRF hierarchy is wired correctly.
9. **Reservoir (Vitter R) uniformity.** Output size exactly `K`; each input line included with probability exactly `K/N` (Kolmogorov-Smirnov over many seeds).
10. **Buffer-shuffle displacement bound.** Every output line's displacement from its input position is ≤ `K` (hard bound; violating it is a bug).

## Edge cases that need explicit handling

- **0-byte file.** Empty output, exit 0. Don't panic on `stat → size=0` or zero chunks.
- **1-line file, no trailing `\n`.** Emit the line once; define canonical policy for missing terminator (I recommend: accept and emit as-is; warn on stderr with `--verify`).
- **File smaller than one chunk.** Degenerate to single-chunk Fisher-Yates; suppress interleave. Don't create an empty second chunk.
- **One line ≥ chunk size.** Two failure modes: line straddles a boundary and exceeds `--max-line`. Document policy: if `line_length > max_line`, skip with `on-error` policy; if `max_line < line_length ≤ chunk_size`, the line is emitted intact from whichever chunk contains its start. Add a test.
- **`\r\n` line endings.** `memchr(b'\n')` finds the `\n`; the `\r` stays in the record. Document that shuflr is newline-framed, not line-ending-normalized. A `--strip-cr` flag is cheap.
- **Embedded NULs.** JSONL forbids unescaped NULs but real-world files contain them. Pass through; do not null-terminate anywhere internally.
- **File grows during read.** mmap past original `stat` size is UB-ish across platforms. Snapshot size at open, ignore tail growth. Document.
- **File truncated during read.** SIGBUS on Linux. Trap and exit cleanly with an error rather than segfaulting.

## `--sample` × `--shuffle=chunk-shuffled` semantics

**This combination is currently misleading.** Taking the first `N` records of a chunk-shuffled stream is **not** a uniform random `N`-subset: early emissions are drawn from the first `M` chunks, so records from late chunks are under-represented in the prefix.

If the user wants a uniform `N`-subset, route them to `--shuffle=reservoir --reservoir-size=N` (true Vitter R). If they want "the first `N` of a shuffle" (i.e., prefix of a block-interleaved stream), document that it is **not** a uniform subset and is biased toward early chunks under the round-robin interleave. Recommend: make `--sample N --shuffle=chunk-shuffled` a warning unless `--sample-biased-ok` is set.

## Open questions

1. **Jitter window `J`.** Not specified. Proposal: `J = 2 · max_line` (guarantees no line straddles two consecutive jittered boundaries with high prob) or just mandate deterministic reconciliation and retire jitter.
2. **Weighted interleave.** Plan mentions "round-robin or weighted random." A weighted interleave (probability proportional to remaining lines in each chunk) is closer to uniform over the `M`-chunk union than round-robin; round-robin adds further structure. Recommend weighted-random as default and measure.
3. **Multi-file globs.** How does chunking interact with input concatenation? Treating files as one logical stream (virtual offsets) is simplest; per-file chunking biases against small files. State the choice.
4. **gRPC flow-control interaction.** If a slow client stalls the stream, does `chunk-shuffled` hold its `M` mmaps indefinitely? RSS accounting in the perf table assumes forward progress; a wedged client breaks it. Recommend an idle-timeout that munmaps and reopens on resume.
5. **Verification oracle.** For `index-perm`, generate a permutation, serialize it, and assert `output == input[permutation]`. For `chunk-shuffled`, there is no closed-form oracle — only distributional tests. Acknowledge this in the test strategy.

---

Net: the design is sound in its ambitions and accurate about what it defers. The main specification debt is honest naming of shuffle quality, a clean PRF hierarchy, deterministic boundary recovery, and disentangling reservoir-vs-buffer. All four are low-cost edits to the design doc before any algorithm code ships.
