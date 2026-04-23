# shuflr-client

Python client for [shuflr](https://github.com/mjbommar/shuflr) — streaming
shuffled JSONL over the network.

Rust core with pyo3 bindings, shipped as a maturin-built wheel. Works with
any server running `shuflr serve`.

## Install

```sh
pip install shuflr-client
```

## Usage

```python
import shuflr_client

ds = shuflr_client.Dataset(
    "http://127.0.0.1:9000/v1/streams/corpus",
    seed=42,
    shuffle="index-perm",
    sample=1000,
)
for record_bytes in ds:
    record = orjson.loads(record_bytes)
    ...
```

### PyTorch

```python
from shuflr_client import IterableDataset
import torch

ds = IterableDataset(
    "http://localhost:9000/v1/streams/training",
    seed=42,
    shuffle="index-perm",
)
loader = torch.utils.data.DataLoader(ds, batch_size=None, num_workers=4)
```

Each DataLoader worker opens its own stream with a distinct
`(rank, world_size)` so records don't duplicate across workers.

## Transports

| Scheme | Transport | Available in |
|---|---|---|
| `http://` | Plain HTTP/1.1 chunked NDJSON | **PR-34a (now)** |
| `https://` | HTTPS (rustls) | PR-34a |
| `shuflr://` | Custom binary, TCP | PR-36 |
| `shuflrs://` | Custom binary, TLS | PR-36 |
| `shuflr+unix://` | Custom binary, UDS | PR-36 |

The HTTP transport is the universal fallback — works through any proxy,
every firewall, every LB. The custom `shuflr-wire/1` transport will
add ~6× wire-size savings on `chunk-shuffled` via compressed-frame
passthrough (005 §3.5).

## Development

```sh
cd crates/shuflr-client
python3 -m venv .venv && source .venv/bin/activate
pip install maturin pytest
maturin develop --release
pytest tests/
```

Requires a shuflr binary built with `--features serve` on `$PATH` or in
`../../target/{debug,release}/shuflr`.
