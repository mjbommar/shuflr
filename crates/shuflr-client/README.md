# shuflr-client

Python client for [shuflr](https://github.com/mjbommar/shuflr) — streaming
shuffled JSONL over the network.

Rust core with pyo3 bindings, shipped as a maturin-built wheel. Works with
`shuflr serve` HTTP(S) listeners and plain `shuflr-wire/1` listeners.

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
    epochs=1,
    shuffle="index-perm",
    sample=1000,
    auth_token=None,
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

| Scheme | Transport | Status |
|---|---|---|
| `http://` | Plain HTTP/1.1 chunked NDJSON | shipped (PR-34a) |
| `https://` | HTTPS (ureq TLS) | shipped; use `tls_ca_cert=` for private CAs |
| `shuflr://` | `shuflr-wire/1` over plain TCP | **shipped (PR-34b)** |
| `shuflrs://` | `shuflr-wire/1` over TLS | parses, not yet wired |
| `shuflr+unix://` | `shuflr-wire/1` over UDS | parses, not yet wired |

The HTTP transport is the universal fallback — works through any proxy,
every firewall, every LB. The `shuflr-wire/1` transport adds explicit
framing and ordered delivery beyond what TCP gives you; raw-frame passthrough
lets `chunk-shuffled` streams shrink to ~disk size on the wire (~6× vs NDJSON).

## Development

```sh
cd crates/shuflr-client
python3 -m venv .venv && source .venv/bin/activate
pip install maturin pytest
cargo build -p shuflr-cli --features serve
maturin develop --release
pytest tests/
```

Requires a shuflr binary built with `--features serve` on `$PATH` or in
`../../target/{debug,release}/shuflr`.
