"""
shuflr + PyTorch integration.

A tiny `IterableDataset` that shells out to `shuflr stream`, decodes each
emitted JSONL record with `json.loads`, and yields Python dicts. This is
the recommended pattern today (PR-19); once `shuflr serve` lands with the
`shuflr.v1` gRPC proto (002 §7), the same class will gain a `.via_grpc()`
constructor that uses the generated Python stubs instead of a pipe.

Usage
-----

    from shuflr_dataset import ShuflrDataset
    from torch.utils.data import DataLoader

    ds = ShuflrDataset(
        "/data/corpus.jsonl.zst",
        shuffle="chunk-shuffled",       # or "index-perm" / "buffer" / "reservoir" / "none"
        seed=42,
        rank=0, world_size=8,           # distributed partitioning
        epochs=0,                       # 0 = infinite
    )
    # IMPORTANT: num_workers must be 0. See the DataLoader notes below.
    loader = DataLoader(ds, batch_size=128, num_workers=0)
    for batch in loader:
        ...

DataLoader caveats (002 §8.2)
-----------------------------

- `num_workers > 0` forks the DataLoader worker, and forking a process
  that owns an active `subprocess.Popen` corrupts its pipes. Keep
  `num_workers=0` when using a pipe-based dataset. Use several
  independently-initialized `ShuflrDataset` objects across DDP ranks
  (`--rank` / `--world-size`) for parallelism instead.
- Record parsing happens in the Python process. For CPU-bound parsing
  on big records, wrap `for rec in ds: batch.append(transform(rec))`
  in your own thread or process pool.
- If `shuflr` exits nonzero mid-stream, `__iter__` raises
  `ShuflrProcessError` with the captured stderr.

Determinism
-----------

Given the same `seed`, the same `(rank, world_size)`, and the same
input fingerprint (see `shuflr info`), every ShuflrDataset invocation
emits byte-identical records in the same order. This is what lets you
resume a training run at a specific step by re-constructing the dataset
with the same args and a `--sample` / skip-count on the caller side.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterator, Optional

try:
    # torch is optional at import time so this file is usable in
    # environments that only want to iterate JSON records.
    from torch.utils.data import IterableDataset  # type: ignore
except ImportError:  # pragma: no cover

    class IterableDataset:  # type: ignore
        """Stub when torch isn't installed."""


class ShuflrProcessError(RuntimeError):
    """Raised when the `shuflr` subprocess exits nonzero mid-iteration."""

    def __init__(self, returncode: int, stderr: str):
        super().__init__(f"shuflr exited {returncode}: {stderr.strip()}")
        self.returncode = returncode
        self.stderr = stderr


@dataclass
class ShuflrDataset(IterableDataset):
    """
    Yield JSONL records from a `shuflr stream` subprocess.

    All options correspond directly to `shuflr stream --help` flags;
    see `shuflr man stream` for the full list.
    """

    path: str
    shuffle: str = "chunk-shuffled"
    seed: Optional[int] = None
    rank: Optional[int] = None
    world_size: Optional[int] = None
    sample: Optional[int] = None
    epochs: int = 1
    # Advanced: buffer size for --shuffle=buffer
    buffer_size: Optional[int] = None
    reservoir_size: Optional[int] = None
    # Where to find the shuflr binary (falls back to $PATH).
    shuflr_bin: Optional[str] = None
    # Extra args passed through verbatim — e.g. ["--max-line", "128MiB"].
    extra_args: list[str] = field(default_factory=list)

    def __iter__(self) -> Iterator[dict]:
        cmd = self._build_cmd()
        env = os.environ.copy()
        # Keep our own stderr clean: let shuflr log at warn unless caller overrode.
        env.setdefault("SHUFLR_LOG", "warn")

        with subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
            bufsize=1 << 20,  # 1 MiB pipe buffer — amortizes Python read overhead
        ) as proc:
            assert proc.stdout is not None
            assert proc.stderr is not None
            try:
                for line in proc.stdout:
                    yield json.loads(line)
            finally:
                proc.stdout.close()
                returncode = proc.wait()
                stderr = proc.stderr.read().decode("utf-8", errors="replace")
                proc.stderr.close()
                if returncode not in (0,):
                    raise ShuflrProcessError(returncode, stderr)

    def _build_cmd(self) -> list[str]:
        bin_path = self.shuflr_bin or shutil.which("shuflr")
        if bin_path is None:
            raise FileNotFoundError(
                "shuflr binary not on $PATH; install with `cargo install shuflr-cli` "
                "or pass shuflr_bin=/absolute/path"
            )

        cmd = [
            bin_path,
            "stream",
            "--shuffle",
            self.shuffle,
        ]
        if self.seed is not None:
            cmd += ["--seed", str(self.seed)]
        if self.sample is not None:
            cmd += ["--sample", str(self.sample)]
        if self.epochs != 1:
            cmd += ["--epochs", str(self.epochs)]
        if self.rank is not None and self.world_size is not None:
            cmd += [
                "--rank",
                str(self.rank),
                "--world-size",
                str(self.world_size),
            ]
        if self.buffer_size is not None and self.shuffle == "buffer":
            cmd += ["--buffer-size", str(self.buffer_size)]
        if self.reservoir_size is not None and self.shuffle == "reservoir":
            cmd += ["--reservoir-size", str(self.reservoir_size)]
        cmd += self.extra_args
        cmd += [str(Path(self.path))]
        return cmd


if __name__ == "__main__":
    # Smoke test: print the first 3 records from any input you pass.
    import sys

    if len(sys.argv) != 2:
        print("usage: shuflr_dataset.py <path.jsonl(.zst)>", file=sys.stderr)
        sys.exit(2)
    ds = ShuflrDataset(sys.argv[1], shuffle="none", sample=3)
    for i, rec in enumerate(ds):
        print(f"[{i}] {json.dumps(rec)[:200]}")
