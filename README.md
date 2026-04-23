# shuflr

**Stream large JSONL files in shuffled order without loading them into memory.**

`shuflr` is a Rust CLI for LLM training-data workflows and anything else that
wants random access over a JSONL corpus too big to fit in RAM. Convert your
file to a record-aligned zstd-seekable format once; shuffle out of it in any
order, forever.

```
$ shuflr convert corpus.jsonl.gz -o corpus.jsonl.zst          # one-time
$ shuflr stream --shuffle=chunk-shuffled --seed=42 corpus.jsonl.zst | ./train.py
```

## Highlights

- **Five shuffle modes**: `none`, `buffer:K`, `chunk-shuffled`, `index-perm`
  (provably uniform), `reservoir` (Vitter R sampler).
- **Distributed partitioning**: `--rank R --world-size W` gives each of W
  processes a disjoint 1/W share, deterministic by seed. No coordinator.
- **Transparent compressed input**: `.gz` / `.zst` / `.bz2` / `.xz` auto-detected
  via magic bytes and decoded on the fly.
- **Record-aligned zstd-seekable output**: every frame ends on `\n`, so each
  is a standalone JSONL shard. Standard `zstdcat` decodes it; the seek-table
  trailer matches Facebook's format.
- **Multi-threaded convert**: 4.56× speedup on 16 cores, byte-identical to the
  single-threaded output.
- **Post-write `--verify`**: reopens the output, decodes every frame, checks
  seek-table consistency.
- **Corpus analyzer**: `shuflr analyze FILE.jsonl.zst` measures per-frame byte
  distribution vs. global and flags source-order locality that would make
  `chunk-shuffled` a bad choice (nudges toward `index-perm`).

Everything is seed-deterministic via a BLAKE3-derived PRF hierarchy
(`master → epoch → {perm, frame, chunk}`), so same inputs + same seed → same
bytes out, reproducibly, across machines.

## Install

Requires a recent stable Rust (edition 2024; MSRV 1.85).

```sh
git clone https://github.com/mjbommar/shuflr
cd shuflr
cargo install --path crates/shuflr-cli
```

Default features include `gzip` + `zstd` decoders; add `--features bzip2,xz`
if you need those codecs, or `--features full` for everything.

## Quick start

```sh
# Pipe-friendly: implicit `stream` subcommand on a bare path
shuflr file.jsonl | jq -r '.text' | head

# Explicit modes (any --shuffle flag works from top-level too)
shuflr --shuffle=none     file.jsonl | head -n 1000
shuflr --shuffle=buffer:200000 file.jsonl.gz | ./train.py

# "Convert once, shuffle forever" — the primary workflow
shuflr convert corpus.jsonl.gz -o corpus.jsonl.zst
shuflr info    corpus.jsonl.zst
shuflr stream --shuffle=chunk-shuffled --seed=42 --epochs=0 corpus.jsonl.zst

# Provably uniform shuffle on plain JSONL (auto-builds .shuflr-idx sidecar)
shuflr stream --shuffle=index-perm --seed=42 flat.jsonl

# Distributed training: rank 3 of 8, disjoint 1/8 share
shuflr stream --shuffle=chunk-shuffled \
    --seed=42 --rank=3 --world-size=8 \
    corpus.jsonl.zst | ./train.py

# Sample exactly 10,000 uniform-random records
shuflr --shuffle=reservoir --reservoir-size=10000 --seed=1 huge.jsonl.gz

# Detect source-order locality before you commit to chunk-shuffled
shuflr analyze corpus.jsonl.zst

# Shell completions
shuflr completions bash > /usr/local/etc/bash_completion.d/shuflr
```

## Shuffle modes

| Mode | What it does | Uniform? | Use when |
|---|---|---|---|
| `none` | Pass-through | — | Fast `cat` with `--sample N` cap |
| `buffer:K` | K-slot ring + random evict | No (local) | Sequential input, acceptable quality |
| `chunk-shuffled` | Fisher-Yates within each seekable-zstd frame + frame-order permute | No (block-local) | Big corpora you've `convert`ed |
| `index-perm` | Fisher-Yates on byte-offset index | **Yes** | Training on sorted/clustered corpora |
| `reservoir` | Vitter Algorithm R; emits exactly K | Uniform K-subset | One-shot sampling |

When in doubt for training data: **`convert` first, then `analyze`; if analyze
says UNSAFE, use `index-perm`.**

## Performance (real EDGAR corpus)

Measured on `sample.jsonl.zst` (1.2 GiB compressed / 5.15 GiB decompressed,
100k records, 16-core host):

| Operation | Wall time | Throughput |
|---|---|---|
| `shuflr stream --shuffle=none` (native zstd) | 3.9 s | 1.36 GB/s |
| `shuflr stream --shuffle=buffer:5000 --sample=10000` | 0.9 s | 829 MB/s |
| `shuflr convert` (single-threaded, level 3) | 22.1 s | 233 MB/s |
| `shuflr convert` (16-thread) | **4.8 s** | **1.07 GB/s** |
| `shuflr convert --verify` (adds post-write check) | 8.4 s | — |
| `shuflr info` | < 10 ms | — |
| `shuflr stream --shuffle=chunk-shuffled --sample=1000` (after convert) | 51 ms | 1.06 GB/s |
| `shuflr stream --shuffle=index-perm --sample=100` (after index) | 5 ms | — |
| `shuflr analyze` (32-frame sample) | ~2 s | — |

Synthetic micro-bench (2 MiB reused buffer, `io::sink`):
passthrough hits 5.35 GiB/s on 128 MiB × 8 KiB records.

## Architecture

- **`crates/shuflr`** — library: error, seed (PRF hierarchy), framing, io
  (plain + streaming decoders + zstd-seekable reader/writer/parallel), pipeline
  (passthrough / buffer / chunk_shuffled / index_perm / reservoir), index,
  analyze.
- **`crates/shuflr-cli`** — binary: clap-derive CLI, subcommand dispatch,
  indicatif progress, sysexits.h exit codes.
- **`docs/design/`** — numbered design iterations (001 initial, 002 revised
  authoritative spec, 003 compression formats, 004 convert subcommand) plus
  seven Opus reviews in `review-01/` that drove 002.

See `CLAUDE.md` for project conventions (error posture, lint rules, perf
targets, testing strategy, "stdout is sacred" rule).

## Testing

```sh
cargo fmt --all -- --check        # style
cargo clippy --workspace --all-targets -- -D warnings   # lint
cargo test --workspace --all-targets                    # ~130 tests
cargo bench --bench throughput    # criterion baselines
```

Real-corpus integration happens via dogfooding against
`/nas3/data/corpus/us/edgar/sample.jsonl.zst`; not included here.

## License

MIT OR Apache-2.0.
