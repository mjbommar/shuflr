# shuflr-client

Python client for [shuflr](https://github.com/mjbommar/shuflr) —
streaming shuffled JSONL records over the network for LLM training and
analytics.

Rust core with pyo3 bindings, shipped as a single maturin-built abi3
wheel (Python 3.9+). No tokio, no async — blocking sockets compose
cleanly with PyTorch's multiprocessing DataLoader workers.

```sh
pip install shuflr-client
```

## Usage

```python
import shuflr_client

ds = shuflr_client.Dataset(
    "http://127.0.0.1:9000/v1/streams/corpus",
    seed=42,
    shuffle="chunk-shuffled",   # or "index-perm" for provably uniform
    epochs=0,                   # 0 = infinite
    sample=None,                # or N to cap
    rank=0, world_size=4,       # distributed partitioning, no coordinator
    auth_token=None,            # bearer for protected servers
    tls_ca_cert=None,           # path to PEM bundle for private CAs
)
for record_bytes in ds:
    record = orjson.loads(record_bytes)
    ...
```

Each `__next__` returns one `bytes` record (no trailing newline). The
stream opens lazily on first `iter()` and closes when the server runs
out of records or `--sample` is exhausted.

### PyTorch

```python
from shuflr_client import IterableDataset
import torch

ds = IterableDataset(
    "http://localhost:9000/v1/streams/training",
    seed=42, shuffle="index-perm",
)
loader = torch.utils.data.DataLoader(ds, batch_size=128, num_workers=4)
```

When invoked from inside a DataLoader worker, `IterableDataset` reads
`torch.utils.data.get_worker_info()` and auto-fills `rank` +
`world_size` so each worker reads a disjoint slice of the shuffled
stream — no per-worker config required.

A typical training data layer opens many `Dataset` instances at once
(one per source corpus) and weights them with a wrapper layer; in
production we sustain ~150 concurrent shuflr streams from a single
training process this way.

## Transports

| Scheme | Transport | Status |
|---|---|---|
| `http://`  | Plain HTTP/1.1 chunked NDJSON | shipped |
| `https://` | HTTPS (ureq TLS) | shipped — `tls_ca_cert=` for private CAs |
| `shuflr://`  | `shuflr-wire/1` over plain TCP | shipped |
| `shuflrs://` | `shuflr-wire/1` over TLS | parses, lands next |
| `shuflr+unix://` | `shuflr-wire/1` over UDS | parses, lands next |

HTTP is the universal fallback — works through any proxy, every
firewall, every LB. The `shuflr-wire/1` transport adds explicit
framing and ordered delivery beyond what TCP gives you, plus
**raw-frame passthrough** for `chunk-shuffled` mode: the server hands
the client compressed seekable-zstd frames it then re-shuffles
locally with the server-derived seed. Wire size shrinks to ~disk size
(~3.6× less than NDJSON) at no quality cost.

## Server

A `shuflr serve` instance hosts one or more named datasets:

```sh
shuflr serve --http 127.0.0.1:9000 \
    --dataset corpus=/data/corpus.jsonl.zst \
    --dataset pairs=/data/pairs.jsonl.zst
```

TLS, bearer / mTLS auth, and reloadable token files are all supported
on the same listener. See the [shuflr README](https://github.com/mjbommar/shuflr)
for the full server-side surface.

## Development

```sh
cd crates/shuflr-client
python3 -m venv .venv && source .venv/bin/activate
pip install maturin pytest
cargo build -p shuflr-cli --features serve   # tests spawn a real server
maturin develop --release
pytest tests/
```

Or run the bundled script: `crates/shuflr-client/scripts/test.sh`.

## License

MIT OR Apache-2.0.
