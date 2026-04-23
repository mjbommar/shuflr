"""
Compare HTTP / wire plain-batch / wire raw-frame end-to-end from a
real `shuflr serve` subprocess into the `shuflr_client` Python
package. Reports:

  - wall time to iterate N records
  - bytes received by the client (application-layer — excludes TCP
    framing, includes wire message headers)
  - payload bytes (sum of record bodies)
  - records-per-second
  - compression ratio relative to NDJSON baseline

Usage:
  # Quick smoke on a built-in fixture (generated on the fly).
  .venv/bin/python benches/bench_protocols.py

  # Point at a real seekable-zstd corpus (e.g. the EDGAR sample).
  .venv/bin/python benches/bench_protocols.py \\
      --dataset /nas3/data/corpus/us/edgar/sample.jsonl.zst \\
      --sample 10000

  # Full-epoch EDGAR (no --sample):
  .venv/bin/python benches/bench_protocols.py \\
      --dataset /path/to/corpus.jsonl.zst

Requires the `shuflr` binary built with `--features serve` either on
PATH or discoverable under target/{debug,release}/.
"""

from __future__ import annotations

import argparse
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional

import shuflr_client  # type: ignore

# --- paths ---------------------------------------------------------

HERE = Path(__file__).resolve().parent
CRATE = HERE.parent
WORKSPACE = CRATE.parents[1]


def _find_shuflr_bin() -> Path:
    env = os.environ.get("SHUFLR_BIN")
    if env:
        return Path(env)
    for mode in ("release", "debug"):
        p = WORKSPACE / "target" / mode / "shuflr"
        if p.exists():
            return p
    w = shutil.which("shuflr")
    if w:
        return Path(w)
    raise SystemExit(
        "could not find shuflr binary; build with `cargo build --features serve --release`"
        " or set SHUFLR_BIN"
    )


def _free_port() -> int:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


# --- server lifecycle ---------------------------------------------


class Serve:
    """Context manager for a `shuflr serve` subprocess with both
    --http and --wire listeners bound to loopback."""

    def __init__(self, dataset_path: Path, *, bin_path: Path):
        self.dataset_path = dataset_path
        self.bin_path = bin_path
        self.http_port = _free_port()
        self.wire_port = _free_port()
        self.proc: Optional[subprocess.Popen] = None

    def __enter__(self) -> "Serve":
        args = [
            str(self.bin_path),
            "serve",
            "--http",
            f"127.0.0.1:{self.http_port}",
            "--wire",
            f"127.0.0.1:{self.wire_port}",
            "--dataset",
            f"corpus={self.dataset_path}",
            "--log-level",
            "info",
        ]
        self.proc = subprocess.Popen(
            args,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            bufsize=1,
            text=True,
        )
        # Wait for both listeners to bind, streaming stderr to our own
        # stderr so the user sees any server-side warnings.
        http_ready = threading.Event()
        wire_ready = threading.Event()

        def _drain():
            for line in self.proc.stderr:
                if "serve(http) bound" in line:
                    http_ready.set()
                if "serve(wire) bound" in line:
                    wire_ready.set()
                if os.environ.get("BENCH_VERBOSE"):
                    sys.stderr.write(f"[serve] {line}")

        threading.Thread(target=_drain, daemon=True).start()
        deadline = time.time() + 20.0
        while time.time() < deadline:
            if http_ready.is_set() and wire_ready.is_set():
                break
            if self.proc.poll() is not None:
                raise SystemExit(f"shuflr serve died early (rc={self.proc.returncode})")
            time.sleep(0.02)
        else:
            self.proc.kill()
            raise SystemExit("shuflr serve never reported both listeners ready")
        return self

    def __exit__(self, *exc):
        if self.proc is None:
            return
        self.proc.terminate()
        try:
            self.proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait()


# --- a single benchmark run ---------------------------------------


@dataclass
class Result:
    label: str
    transport: str
    shuffle: str
    wall_s: float
    records: int
    wire_bytes: int
    payload_bytes: int

    def records_per_s(self) -> float:
        return self.records / self.wall_s if self.wall_s > 0 else 0.0

    def wire_mb_per_s(self) -> float:
        return self.wire_bytes / self.wall_s / 1e6 if self.wall_s > 0 else 0.0

    def payload_mb(self) -> float:
        return self.payload_bytes / 1e6

    def wire_mb(self) -> float:
        return self.wire_bytes / 1e6


def run_one(
    label: str,
    url: str,
    *,
    shuffle: str,
    seed: int,
    sample: Optional[int],
) -> Result:
    ds = shuflr_client.Dataset(
        url,
        seed=seed,
        shuffle=shuffle,
        sample=sample,
    )
    records = 0
    payload_bytes = 0
    t0 = time.perf_counter()
    for rec in ds:
        records += 1
        payload_bytes += len(rec)
    wall = time.perf_counter() - t0
    return Result(
        label=label,
        transport=ds.transport,
        shuffle=shuffle,
        wall_s=wall,
        records=records,
        wire_bytes=ds.bytes_received,
        payload_bytes=payload_bytes,
    )


# --- fixture generation -------------------------------------------


def _generate_fixture(
    dst_plain: Path, dst_zstd: Path, *, n_records: int, bin_path: Path
) -> None:
    """Synthesize a corpus similar in shape to EDGAR records: a
    realistic mix of short-to-medium JSON lines with some variance."""
    import json
    import random

    random.seed(1234)
    with dst_plain.open("w", encoding="utf-8") as f:
        for i in range(n_records):
            body = {
                "i": i,
                "filer": f"FILER-{random.randint(0, 9999):04d}",
                "type": random.choice(["10-K", "10-Q", "8-K", "DEF 14A"]),
                "text": "x" * random.randint(64, 2048),
            }
            f.write(json.dumps(body))
            f.write("\n")
    # Convert to seekable-zstd via the real `shuflr convert`.
    subprocess.run(
        [
            str(bin_path),
            "convert",
            "--log-level",
            "warn",
            "-o",
            str(dst_zstd),
            str(dst_plain),
        ],
        check=True,
    )


# --- printing ------------------------------------------------------


def print_results(results: Iterable[Result]) -> None:
    results = list(results)
    # baseline for compression ratio: the first NDJSON entry.
    baseline = next((r for r in results if r.transport == "http"), None)
    header = f"{'label':<28} {'transport':<18} {'shuffle':<16} {'records':>10} {'wall_s':>8} {'rec/s':>10} {'payload_MB':>12} {'wire_MB':>10} {'wire_MB/s':>10} {'wire/http':>10}"
    print(header)
    print("-" * len(header))
    for r in results:
        ratio = ""
        if baseline is not None and baseline.wire_bytes > 0:
            ratio = f"{r.wire_bytes / baseline.wire_bytes:>10.3f}"
        print(
            f"{r.label:<28} "
            f"{r.transport:<18} "
            f"{r.shuffle:<16} "
            f"{r.records:>10d} "
            f"{r.wall_s:>8.2f} "
            f"{r.records_per_s():>10.0f} "
            f"{r.payload_mb():>12.2f} "
            f"{r.wire_mb():>10.2f} "
            f"{r.wire_mb_per_s():>10.2f} "
            f"{ratio}"
        )


# --- main ----------------------------------------------------------


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--dataset",
        type=Path,
        help="seekable-zstd corpus (produced by `shuflr convert`). "
        "If omitted, a synthetic corpus is generated.",
    )
    ap.add_argument(
        "--n-records",
        type=int,
        default=5000,
        help="Size of the synthetic corpus (ignored with --dataset).",
    )
    ap.add_argument(
        "--sample",
        type=int,
        default=None,
        help="Cap records emitted per protocol run (mirrors --sample on stream).",
    )
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    bin_path = _find_shuflr_bin()

    cleanup_tmp: Optional[tempfile.TemporaryDirectory] = None
    if args.dataset is not None:
        dataset_path = args.dataset.resolve()
        if not dataset_path.exists():
            raise SystemExit(f"--dataset {dataset_path} does not exist")
    else:
        cleanup_tmp = tempfile.TemporaryDirectory(prefix="shuflr-bench-")
        tmp = Path(cleanup_tmp.name)
        plain = tmp / "fixture.jsonl"
        dataset_path = tmp / "fixture.jsonl.zst"
        print(
            f"[bench] generating synthetic fixture ({args.n_records} records)…",
            file=sys.stderr,
        )
        _generate_fixture(
            plain, dataset_path, n_records=args.n_records, bin_path=bin_path
        )

    try:
        with Serve(dataset_path, bin_path=bin_path) as srv:
            # For a fair comparison of raw-frame, we want one config
            # that actually triggers it (chunk-shuffled on seekable
            # zstd), one that forces plain-batch on wire (buffer), and
            # HTTP with the same shuffle.
            runs = [
                (
                    "http / chunk-shuffled",
                    f"http://127.0.0.1:{srv.http_port}/v1/streams/corpus",
                    "chunk-shuffled",
                ),
                (
                    "wire plain-batch / buffer",
                    f"shuflr://127.0.0.1:{srv.wire_port}/corpus",
                    "buffer",
                ),
                (
                    "wire raw-frame / chunk-shuffled",
                    f"shuflr://127.0.0.1:{srv.wire_port}/corpus",
                    "chunk-shuffled",
                ),
            ]
            results = []
            for label, url, shuffle in runs:
                print(f"[bench] {label}…", file=sys.stderr)
                r = run_one(
                    label,
                    url,
                    shuffle=shuffle,
                    seed=args.seed,
                    sample=args.sample,
                )
                results.append(r)
                print(
                    f"[bench]   {r.records} records in {r.wall_s:.2f}s  "
                    f"wire={r.wire_mb():.2f} MB  "
                    f"payload={r.payload_mb():.2f} MB",
                    file=sys.stderr,
                )
    finally:
        if cleanup_tmp is not None:
            cleanup_tmp.cleanup()

    print()
    print_results(results)
    print()
    # Summary headline.
    http = next((r for r in results if r.label.startswith("http")), None)
    raw = next((r for r in results if "raw-frame" in r.label), None)
    if http and raw and http.wire_bytes > 0:
        ratio = http.wire_bytes / raw.wire_bytes
        speedup = raw.records_per_s() / http.records_per_s() if http.records_per_s() > 0 else float("nan")
        print(
            f"summary: raw-frame is {ratio:.2f}× smaller on the wire "
            f"and {speedup:.2f}× records/s vs HTTP NDJSON."
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
