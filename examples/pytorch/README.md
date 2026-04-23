# PyTorch `IterableDataset` example

Minimal reference implementation for consuming a `shuflr` stream from a PyTorch training loop.

## Files

| File | Purpose |
|---|---|
| `shuflr_dataset.py` | `ShuflrDataset(IterableDataset)` — shells out to `shuflr stream` and yields Python dicts via `json.loads`. |

## Quick use

```python
from shuflr_dataset import ShuflrDataset
from torch.utils.data import DataLoader

# Single-rank
ds = ShuflrDataset("corpus.jsonl.zst", shuffle="chunk-shuffled", seed=42)
loader = DataLoader(ds, batch_size=128, num_workers=0)
for batch in loader:
    ...
```

Distributed training (each rank emits a disjoint 1/W share):

```python
import torch.distributed as dist
ds = ShuflrDataset(
    "corpus.jsonl.zst",
    shuffle="chunk-shuffled",
    seed=42,
    rank=dist.get_rank(),
    world_size=dist.get_world_size(),
    epochs=0,  # infinite
)
```

## Notes

- **`num_workers=0` is required** when using the subprocess-pipe backend. PyTorch's `DataLoader` worker fork corrupts the `subprocess.Popen` pipe. For throughput, use distributed ranks (`--rank` / `--world-size`) instead.
- **All `shuflr stream` flags are accessible** via `ShuflrDataset(... extra_args=["--max-line", "128MiB"])`.
- **Pipe backend today, gRPC tomorrow.** Once `shuflr serve` lands (002 §7 proto is frozen), this class will gain a `.via_grpc(...)` constructor for in-process streaming without the fork-safety caveats.

## Smoke test (no PyTorch needed)

```bash
python3 shuflr_dataset.py /path/to/file.jsonl
```

Prints the first three records.
