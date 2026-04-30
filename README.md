# shuflr

**Stream large JSONL files in shuffled order, without loading them into memory.**

`shuflr` is a single Rust binary that does one job well: emit records from a
JSONL corpus in seed-deterministic shuffled order. Convert your data to
record-aligned seekable-zstd once; shuffle out of it forever, on the CLI or
over the network.

```sh
shuflr convert corpus.jsonl.gz -o corpus.jsonl.zst                 # one-time
shuflr stream --shuffle=chunk-shuffled --seed=42 corpus.jsonl.zst | ./train.py
```

## Production status

`shuflr serve` has powered live LLM training without intervention:

- **4d 14h continuous uptime** under a heavy production workload — zero
  crashes, zero data corruption, zero observed errors.
- **~2.4 TB streamed** from 178 GB of on-disk source data (~6 MB/s avg,
  >50 MB/s peak), at **0.7 % CPU** — bottleneck is disk I/O, not compute.
- **62 dataset shards** served simultaneously: MLM corpora, contrastive
  pair sources, templated synth, OpenGloss data, eval benchmark pairs.
- **156 concurrent TCP connections** sustained (training + eval workers
  each open one keepalive socket per source stream).
- **16+ training runs** drew from this single `shuflr serve` process,
  often two pretraining jobs in parallel on different GPUs.

The whole architecture is "throw seekable-zstd files at the binary, point
Python at HTTP URLs." No databases, no caches, no Spark, no Hadoop.

## Highlights

- **Five shuffle modes** — `none`, `buffer:K`, `chunk-shuffled`, `index-perm`
  (provably uniform), `reservoir` (Vitter Algorithm R).
- **Seed-deterministic** — same inputs + same seed → same bytes out, across
  machines. BLAKE3 PRF hierarchy (`master → epoch → {perm, frame, chunk}`)
  isolates every randomized step.
- **Distributed partitioning** — `--rank R --world-size W` gives each of W
  workers a disjoint 1/W share with no coordinator.
- **Transparent input** — `.gz` / `.zst` / `.bz2` / `.xz` auto-detected by
  magic bytes; with `--features parquet`, also reads local `.parquet`,
  parquet shard directories, and `hf://user/repo` from HuggingFace Hub.
- **Record-aligned zstd-seekable output** — every frame ends on `\n`, so
  each is a standalone JSONL shard. `zstdcat` decodes it; the seek-table
  trailer matches Facebook's format. ~4.5× faster convert on 16 cores.
- **Two transports in one process** — `shuflr serve` speaks HTTP/1.1
  chunked NDJSON and `shuflr-wire/1` binary on the same dataset pool.
  Wire's raw-frame passthrough mode shrinks a chunk-shuffled stream to
  ~disk size on the network (~3.6× wire savings vs NDJSON).

## Install

```sh
# From source (until 0.1.0 ships on crates.io)
git clone https://github.com/mjbommar/shuflr
cd shuflr
cargo install --path crates/shuflr-cli
```

Default features include `gzip` + `zstd`. Other features:

| Feature | Adds |
|---|---|
| `serve` | `serve` subcommand, HTTP + `shuflr-wire/1` listeners, TLS, auth |
| `parquet` | parquet + HuggingFace Hub input for `convert` |
| `bzip2`, `xz` | extra streaming-input codecs |
| `uring` | Linux io\_uring fast path |
| `full` | everything currently implemented |

Requires a recent stable Rust (edition 2024, MSRV 1.85).

The Python client is `pip install shuflr-client` (publishing soon; for now,
build from `crates/shuflr-client/` with `maturin develop --release`).

## Subcommands

`shuflr` has nine subcommands. The most common three are:

```sh
shuflr stream  ...   # emit shuffled records to stdout (the default)
shuflr convert ...   # ingest JSONL/parquet/HF Hub → seekable-zstd
shuflr serve   ...   # network service (HTTP + shuflr-wire/1)
```

Plus utilities: `info`, `analyze`, `index`, `verify`, `completions`, `man`.

A bare `shuflr file.jsonl.zst` is shorthand for `shuflr stream file.jsonl.zst`.

### `stream` — emit shuffled records

```sh
# Default (chunk-shuffled) — requires seekable-zstd input
shuflr stream --seed=42 corpus.jsonl.zst | ./train.py

# Pass-through (replaces `cat` in pipelines)
shuflr stream --shuffle=none file.jsonl.gz | jq -r .text | head

# K-buffer local shuffle on a streaming input
shuflr stream --shuffle=buffer --buffer-size=200000 file.jsonl.gz

# Provably uniform — auto-builds .shuflr-idx[-zst] sidecar on first run
shuflr stream --shuffle=index-perm --seed=42 corpus.jsonl.zst

# Distributed: rank 3 of 8 workers
shuflr stream --shuffle=chunk-shuffled \
    --seed=42 --rank=3 --world-size=8 \
    corpus.jsonl.zst

# Sample exactly K uniform-random records (Vitter R)
shuflr stream --shuffle=reservoir --reservoir-size=10000 huge.jsonl.gz
```

Tuning flags worth knowing:

- `--epochs N` — passes over the input. `0` = infinite.
- `--sample N` — stop after N records (per epoch).
- `--max-line BYTES` — per-record cap (default 16 MiB).
- `--on-error skip|fail|passthrough` — what to do with oversized records.
- `--emit-threads N --emit-prefetch K` — parallel emit pipeline for
  `chunk-shuffled` and `index-perm` on seekable-zstd. Default
  `--emit-threads=1` (single-threaded, no behaviour change without opt-in).

### `convert` — make a corpus seekable

`convert` is the one-time preprocessing step. It reads JSONL (plain or
gzip/zstd/bz2/xz), parquet, or HF Hub and writes record-aligned
seekable-zstd in a single pass.

```sh
# JSONL (any compression) → seekable-zstd
shuflr convert corpus.jsonl.gz -o corpus.jsonl.zst

# Multi-threaded; --threads=0 uses physical cores
shuflr convert corpus.jsonl.gz -o corpus.jsonl.zst --threads=0 --verify

# Parquet → seekable-zstd, projecting only two columns
shuflr convert /data/shards/ -o merged.jsonl.zst --parquet-project text,id

# Pull a parquet dataset from HuggingFace Hub
shuflr convert hf://BAAI/legal-corpus -o legal.jsonl.zst

# Bernoulli sample 1 % of input as you convert
shuflr convert big.jsonl.zst -o sample.jsonl.zst --sample-rate=0.01 --seed=1

# Drop boilerplate by Shannon entropy (typical text ≥ 4 bits/byte)
shuflr convert mixed.jsonl.zst -o clean.jsonl.zst --min-entropy=2.5
```

The output is a normal `.jsonl.zst` file — `zstdcat` decompresses it.
Adding the seek-table trailer is what makes it usable for `chunk-shuffled`
and `index-perm` modes.

### `serve` — HTTP + wire listeners on one process

```sh
# Loopback HTTP, two datasets
shuflr serve --http 127.0.0.1:9000 \
    --dataset mlm.cfr=/data/cfr.jsonl.zst \
    --dataset pairs.alllaw=/data/alllaw.jsonl.zst

# HTTP + binary wire, public bind on a trusted VPC (no TLS)
shuflr serve --http 0.0.0.0:9000 --wire 0.0.0.0:9443 \
    --bind-public --insecure-public \
    --dataset corpus=/data/corpus.jsonl.zst

# HTTPS with bearer auth, reloadable token list
shuflr serve --http 0.0.0.0:9000 \
    --tls-cert server.crt --tls-key server.key \
    --auth bearer --auth-tokens /etc/shuflr/tokens \
    --dataset corpus=/data/corpus.jsonl.zst
# `kill -HUP <pid>` reloads --auth-tokens without restarting.

# mTLS (client-cert auth)
shuflr serve --http 0.0.0.0:9000 \
    --tls-cert server.crt --tls-key server.key \
    --tls-client-ca clients-ca.crt --auth mtls \
    --dataset corpus=/data/corpus.jsonl.zst
```

A request to `GET /v1/streams/{dataset_id}` returns chunked NDJSON. The
shuflr-wire/1 transport on the same process exposes the same datasets at
`shuflr://host:port/v1/streams/{dataset_id}` with raw-frame passthrough
(the server hands the client compressed frames it then re-shuffles
locally with the server-derived seed — same record order, much less wire).

Bind defaults to loopback. Non-loopback bind requires either TLS or
the explicit `--insecure-public` opt-in (which logs a WARN per request).

### Utilities

```sh
shuflr info corpus.jsonl.zst              # < 10 ms — read seek-table only
shuflr info corpus.jsonl.zst --json       # machine-readable

shuflr analyze corpus.jsonl.zst           # per-frame entropy + locality check
shuflr analyze corpus.jsonl.zst --strict  # exit 3 if chunk-shuffled would be bad
shuflr analyze corpus.jsonl.zst --json    # machine-readable

shuflr index flat.jsonl                   # build .shuflr-idx for index-perm
shuflr verify corpus.jsonl.zst --deep     # framing + JSON parse, exit 65 on bad

shuflr completions bash > /etc/bash_completion.d/shuflr
shuflr man         > /usr/local/share/man/man1/shuflr.1
shuflr man convert > /usr/local/share/man/man1/shuflr-convert.1
```

When in doubt for training data: **`convert` first, then `analyze`; if
analyze says UNSAFE, use `index-perm`.**

## Python client

`shuflr-client` is a maturin-built wheel — Rust core, pyo3 bindings, no
async runtime. It speaks both HTTP(S) and `shuflr-wire/1`.

```python
import shuflr_client

ds = shuflr_client.Dataset(
    "http://127.0.0.1:9000/v1/streams/pairs.alllaw",
    seed=42,
    shuffle="chunk-shuffled",   # or "index-perm" for provably uniform
    epochs=0,                   # 0 = infinite
    sample=None,                # or N to cap
    rank=0, world_size=4,       # distributed partitioning
    auth_token=None,            # bearer for protected servers
    tls_ca_cert=None,           # path to PEM bundle for private CAs
)
for record_bytes in ds:
    ...                         # one JSONL record, no trailing newline
```

Drop straight into PyTorch:

```python
from shuflr_client import IterableDataset
import torch

ds = IterableDataset(
    "http://localhost:9000/v1/streams/training",
    seed=42, shuffle="index-perm",
)
loader = torch.utils.data.DataLoader(ds, batch_size=128, num_workers=4)
```

When `IterableDataset` is invoked from inside a PyTorch DataLoader worker
it auto-fills `rank` and `world_size` from `torch.utils.data.get_worker_info()`,
so each worker reads a disjoint slice without coordination.

| URL scheme | Transport | Status |
|---|---|---|
| `http://`  | HTTP/1.1 chunked NDJSON | shipped |
| `https://` | HTTPS (TLS via ureq)    | shipped — `tls_ca_cert=` for private CAs |
| `shuflr://`  | `shuflr-wire/1` over plain TCP | shipped |
| `shuflrs://` | `shuflr-wire/1` over TLS       | parses, lands next |
| `shuflr+unix://` | `shuflr-wire/1` over UDS   | parses, lands next |

## Shuffle modes

| Mode | What it does | Uniform? | Use when |
|---|---|---|---|
| `none` | Pass-through | — | Fast `cat` with `--sample N` cap |
| `buffer` | K-slot ring + random evict | No (local) | Streaming input, acceptable quality |
| `chunk-shuffled` | Fisher-Yates within each frame + frame-order permute | No (block-local) | Big corpora you've `convert`ed |
| `index-perm` | Fisher-Yates over byte-offset index | **Yes** | Training on sorted/clustered corpora |
| `reservoir` | Vitter Algorithm R; emits exactly K | Uniform K-subset | One-shot sampling |

## Performance

Measured on a 1.2 GiB compressed (5.15 GiB decompressed) EDGAR sample,
100k records, 16-core host:

| Operation | Wall time | Throughput |
|---|---|---|
| `stream --shuffle=none` (zstd) | 3.9 s | 1.36 GB/s |
| `stream --shuffle=buffer --buffer-size=5000 --sample=10000` | 0.9 s | 829 MB/s |
| `convert` (single-thread, level 3) | 22.1 s | 233 MB/s |
| `convert --threads=0` (16-thread) | **4.8 s** | **1.07 GB/s** |
| `convert --verify` (single-thread + post-write check) | 8.4 s | — |
| `info` | < 10 ms | — |
| `stream --shuffle=chunk-shuffled --sample=1000` | 51 ms | 1.06 GB/s |
| `stream --shuffle=index-perm --sample=100` (warm sidecar) | 5 ms | — |
| `analyze` (32-frame sample) | ~2 s | — |

Synthetic micro-bench (2 MiB reused buffer, `io::sink`): passthrough
hits 5.35 GiB/s on 128 MiB × 8 KiB records.

A full-scale run on a 31 GB EDGAR gzip (192.68 GB decompressed,
1.2M records) is in `docs/bench/001-edgar-31gb-gzip.md`.

## Architecture

```
crates/
  shuflr/         library — error, seed (PRF hierarchy), framing, io
                  (plain + streaming decoders + zstd-seekable
                  reader/writer/parallel), pipeline (passthrough /
                  buffer / chunk_shuffled / index_perm / reservoir),
                  index, analyze, service edge (feature-gated).
  shuflr-cli/     binary — clap-derive CLI, subcommand dispatch,
                  indicatif progress, sysexits.h exit codes.
                  Produces the `shuflr` binary.
  shuflr-wire/    `shuflr-wire/1` codec — framing, parsing, xxh3.
  shuflr-client/  Python wheel (Rust + pyo3 + maturin).
                  Speaks HTTP + `shuflr-wire/1`.
docs/design/      numbered append-only design iterations.
docs/bench/       end-to-end performance reports.
```

The 4-crate split is deliberate: the library never depends on tokio
(async lives only behind the `serve` feature flag at the gRPC/HTTP
edge); the wire codec is sync + tiny; the Python wheel is sync +
blocking so it composes with PyTorch's multiprocessing workers.

## Spec

`docs/design/002-revised-plan.md` is the v1 authoritative spec. Read it
before changing algorithm or API behavior. Amendments live in
later-numbered docs:

- `003-compression-formats.md` — streaming `.gz` / `.zst` / `.bz2` / `.xz`
  policy (amends 002 §4.4).
- `004-convert-subcommand.md` — `convert` + `info` + seekable-zstd
  reader (defines record-aligned frames as a shuflr invariant).
- `005-serve-multi-transport.md` — HTTP + `shuflr-wire/1` + (later)
  gRPC, plus the `shuflr-client` Python wheel.

`review-01/` holds the seven Opus reviews that drove 002.

## Testing

```sh
cargo fmt --all -- --check                              # style
cargo clippy --workspace --all-targets -- -D warnings   # lint
cargo test --workspace --all-targets                    # default tests
cargo test -p shuflr-cli --tests --features serve       # serve tests
cargo test -p shuflr --features parquet                 # parquet tests
cargo bench --bench throughput                          # criterion
crates/shuflr-client/scripts/test.sh                    # Python tests
```

234+ tests green at the default feature set; `serve` and `parquet`
features each add their own.

## License

MIT OR Apache-2.0 — your choice.
