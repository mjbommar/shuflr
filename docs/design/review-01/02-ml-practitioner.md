# shuflr Design Review — ML Practitioner Perspective

*Reviewer: senior ML engineer, shipped production LLM pretraining and midtraining pipelines. Has written dataloader code for multi-thousand-GPU runs, debugged MosaicML Streaming shard issues at 2am, and rewritten `IterableDataset.shuffle` more than once.*

## TL;DR

**Would I use shuflr in my next training run? No — not v1 as specified.** As a pre-training data feeder it is missing three table-stakes properties: (a) resumable, deterministic cursors; (b) a distributed-sharding story where each of N ranks sees a disjoint partition of the epoch; (c) a rigorous epoch/coverage accounting. The headline chunk-shuffled algorithm is fine for log processing, EDA, and single-GPU fine-tuning on clean corpora, but it does not unbias the source ordering of real pretraining corpora, and that alone rules it out as a default. With three specific changes (see below) a v1.1 could become genuinely useful, occupying a real niche between HF `IterableDataset` and MosaicML Streaming.

## Strengths

- **Right engineering posture.** `ChaCha20Rng` + explicit seeds, stdout-is-sacred, bounded memory, criterion benches, property tests for coverage. This is how a trustworthy tool is built.
- **No mandatory preprocessing.** For the "I have a 2TB JSONL and I want a mostly-shuffled stream in 50ms" use case, this is genuinely novel versus Mosaic's MDS-convert or WebDataset's tar-sharding step.
- **Clean separation of transport from shuffle.** The gRPC/stdout/UDS split is the right shape; dataloaders can consume whichever fits.
- **Honest scope.** `--shuffle=index-perm` and `--shuffle=2pass` are in the vocabulary even if deferred. That's the right long-term design.

## Concerns and Gaps

### 1. Chunk-shuffled is not IID, and the design undersells the risk (§ "Core Algorithm")

The plan describes the shuffle as "qualitatively similar to a very large shuffle buffer." That is true only if the input JSONL is already roughly order-independent. In real corpora it rarely is. A CommonCrawl+code+books concatenation written in source order means the first ~250MB chunk is *all* CommonCrawl; shuffling within it and interleaving M=8 active chunks gives a stream where the first few hundred thousand records are still ~100% CommonCrawl. HuggingFace `IterableDataset.shuffle(buffer_size=10_000)` has exactly this failure mode, and it is a well-known cause of early-training loss spikes and mixture drift.

**Recommendation:** (a) in docs, state explicitly that chunk-shuffled preserves source-order locality at scales larger than `active_chunks × chunk_size`; (b) emit a `tracing::warn!` on startup for any file larger than, say, 64 × (`active_chunks × chunk_size`) recommending `--shuffle=index-perm` or a one-time offline `2pass`; (c) ship a `shuflr analyze` subcommand that samples, hashes, and reports estimated source-heterogeneity (KL of chunk-local token/byte distributions vs global). For training, the right default is not chunk-shuffled — it is `index-perm` with a persisted `.idx`, even if the index takes 10 minutes to build once.

### 2. Resumable cursors are table-stakes, not v2 (§ "Explicitly deferred")

Every non-trivial pretraining run is preempted. SLURM requeue, spot instance, node drain, OOM, loss spike rollback. If a rank reconnects to shuflr after a checkpoint, it *must* resume at the exact next record of the exact same seeded permutation. Deferring "resumable cursors" to v2 means v1 is unusable for any run longer than the mean time to preemption.

This is not expensive to build. Given `(seed, epoch_number, chunk_order_seed, within_chunk_cursor)` the state is ~32 bytes per stream. The design should add a `--cursor-file` or a `ResumeToken` in the gRPC `StartStream` request, and every N records flush a cursor update to stderr or a sidecar. MosaicML Streaming's `StreamingDataset.state_dict()` is the reference implementation to copy. Without this, every practitioner will wrap shuflr in their own checkpoint-aware shim, silently reintroducing nondeterminism.

**Recommendation:** promote resumable cursors to v1. Scope: deterministic `(shard, chunk_idx, within_chunk_offset, epoch)` token, emitted every K records, accepted on gRPC reconnect.

### 3. Distributed sharding is also table-stakes (§ "Non-Goals", `--fanout`/`--partition`)

"Consumers handle sharding" is not a real answer. If 64 ranks each run their own shuflr process, they either (a) each scan the entire file and skip 63/64 of it — wasting 64× I/O — or (b) reimplement partitioning in userland. If they connect to one shuflr server, the design doesn't say whether fanout means broadcast or partition.

The minimum viable story: `--rank R --world-size W` on the CLI. Ranks partition chunks by `(chunk_idx + seed) mod W == R`, with independent per-rank intra-chunk shuffles seeded from `(seed, R)`. No gRPC broker needed; each rank runs its own process against the same file. This is how WebDataset does it with `nodesplitter=split_by_node`, and it works well enough that Mosaic explicitly copied the pattern. Without this, shuflr is a single-GPU tool.

**Recommendation:** add `--rank`/`--world-size` to v1. Document the disjointness guarantee. Property-test it.

### 4. Mix weights: define the semantics (§ "I/O", `--input a:0.7 --input b:0.3`)

"Repeatable for weighted mixes" is ambiguous. The three common semantics:

- **Per-record Bernoulli:** at each emit, sample source ∝ w. Simple, but with finite files the realized ratio depends on file sizes and you will overshoot small files / stall on exhausted sources.
- **Upsample-to-exhaustion:** cycle each source independently, sample ∝ w forever. The right default for training.
- **Per-chunk:** pick a source per chunk, shuffle within. Higher variance, worse mixing.

Real practitioners want upsample-to-exhaustion with a `--epoch-policy={largest,smallest,weighted}` describing what "one epoch" means when sources are unevenly sized. Megatron-Energon and Mosaic both expose this explicitly. The plan does not.

**Recommendation:** pick upsample-to-exhaustion as default; document clearly; expose `--mix-policy`.

### 5. Epoch semantics are currently unrigorous (§ "Correctness Strategy")

With boundary jitter, "one epoch" is no longer "each line exactly once." The plan says boundary-lost lines are recovered *probabilistically* in later epochs. That's fine for analytics, not fine for "trained on 3.7 epochs of 1.2T tokens" reporting. Practitioners need to cite exact counts.

**Recommendation:** expose a `--strict-epochs` mode that uses `index-perm` under the hood and guarantees exact coverage per epoch. Under chunk-shuffled, emit end-of-epoch metrics: `{records_emitted, records_expected, boundary_losses, duplicates}`. Log as structured JSON.

### 6. Comparison to existing tools (honest)

- **MosaicML Streaming.** Mosaic does cloud-native sharding, compression, per-rank deterministic shuffle with `py1b`/`py1s`/`py1br` algorithms, and resumable state. It requires an MDS convert step. shuflr's advantage: no convert step, works on raw JSONL. Disadvantage: everything else. Until shuflr has resume + rank sharding, Mosaic wins.
- **WebDataset.** Tar-based, shuffle buffer + shard shuffle. The v1 shuflr actually beats it on single-machine throughput and on "no preprocessing" but loses on cloud, on multi-node, and on the maturity of `nodesplitter`. Real sites already have WebDataset pipelines; shuflr won't displace them.
- **HuggingFace `IterableDataset`.** The bar is low. HF's `.shuffle(buffer_size=10_000)` on a 1B-example corpus gives a *terrible* shuffle — effectively local, and with `num_workers>1` it becomes nondeterministic across restarts. shuflr could clearly beat this for the "I just want a better shuffle than HF gives me, feeding my DataLoader" segment, provided items 2 and 3 above land.
- **Megatron-Energon / LitData.** Both assume their own storage format. shuflr is upstream of them. Not a comparison.

### 7. PyTorch integration story is missing (§ "Consumption / Output")

Nothing in the plan tells a user what `DataLoader` code looks like. The likely pattern:

```python
class ShuflrDataset(IterableDataset):
    def __iter__(self):
        rank = dist.get_rank(); world = dist.get_world_size()
        stub = shuflr_pb2_grpc.ShufflrStub(grpc.insecure_channel("localhost:9000"))
        for batch in stub.Stream(StartStreamRequest(rank=rank, world_size=world, seed=SEED, resume=self.state)):
            for rec in batch.records: yield json.loads(rec)
```

That wants (a) a published `.proto`, (b) a Python client example in `docs/`, (c) a documented `DataLoader(num_workers=0)` contract (workers + gRPC stream = a footgun).

**Recommendation:** ship an `examples/pytorch/` directory with a working `IterableDataset` wrapper. Without this, nobody will figure out the integration; they'll shell-pipe and lose determinism.

### 8. What's missing entirely

- **Deterministic sample IDs.** Training reports ("sample X was in loss spike at step Y") require a stable per-record identifier. shuflr should emit a `record_id = hash(file_path, byte_offset)` alongside each record (feature-flagged to avoid payload bloat).
- **Cloud storage.** `s3://`, `gs://` with Range GET. Deferred to v1.1 per the plan, which is defensible, but note: most pretraining data lives on object storage.
- **Tokenized-view caching.** Out of scope, but worth noting that Mosaic/Energon/LitData all integrate tokenization. shuflr emitting raw JSONL means every rank re-tokenizes. Not a v1 problem; flag it.
- **fsspec compatibility.** Not required, but a Python `fsspec` entry point would make shuflr adoptable in 10 lines instead of 100.

## Minimum Bar for "Trustworthy in Training"

1. Deterministic, seeded permutation with **resume tokens** surviving reconnect.
2. `--rank`/`--world-size` with property-tested disjointness.
3. `--shuffle=index-perm` available (index persisted, reusable across runs).
4. End-of-epoch metrics: expected vs emitted vs lost.
5. Documented `.proto` and a working PyTorch `IterableDataset` example.
6. A `shuflr analyze` that warns when source-order locality would make chunk-shuffled unsafe.

Ship that and I will try it. Ship v1 as specified and I will keep using Mosaic.

## Nice-to-have (v2+)

- Seekable zstd (lots of our corpora are zstd already).
- Parquet/Arrow IPC input.
- Multi-consumer `--fanout` with partition semantics (pinned now: `partition` = disjoint, `broadcast` = duplicated).
- Tokenizer passthrough (`--tokenizer tiktoken:cl100k_base` → emit token IDs, skip re-tokenization on every rank).
- Histogram-based record-length-aware chunking (so a 1GB record doesn't poison a 256MB chunk).
- On-the-fly checksum/dedup (xxhash) with configurable windowing.

## Open Questions

1. How does chunk-shuffled interact with **gradient accumulation batch composition**? If a global batch spans 8 ranks × 8 micro-batches × 4 grad-accum = 256 samples, and chunks are 256MB ≈ 500k records, you'll see correlated samples across a global batch early in a chunk. Is that acceptable?
2. Is **reservoir=K** ever actually useful here, or is it a historical footnote? For stdin it's fine; for a file, `index-perm` dominates.
3. Will the **gRPC service** be deployed as a sidecar per node, or a central service per cluster? The sharding and resume design differ materially. I'd pick sidecar and optimize for that.
4. How does shuflr reason about **dataset versioning**? If the underlying JSONL is appended to between runs, the permutation is silently invalidated. Surface a file-content hash in the resume token.
5. What is the **failure mode when a consumer stalls** longer than gRPC keepalive? Backpressure per the diagram is good; make sure the resume token is flushed at disconnect so we don't lose position.
