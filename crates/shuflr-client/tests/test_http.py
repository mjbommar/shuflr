"""
End-to-end tests for the Python client over HTTP.

Each test spawns a real `shuflr serve` subprocess and drives it from
Python. The HTTP server is started with --log-level info so we can
synchronize on the "serve(http) bound" line rather than racing a fixed
sleep.
"""

from __future__ import annotations

import os
import shutil
import socket
import subprocess
import sys
import time
from pathlib import Path

import pytest

import shuflr_client


HERE = Path(__file__).resolve().parent
# Walk up to the workspace root, then into target/. Allows CI to
# override with $SHUFLR_BIN.
WORKSPACE = HERE.parents[2]


def _find_shuflr_bin() -> Path:
    env = os.environ.get("SHUFLR_BIN")
    if env:
        return Path(env)
    # Prefer the locally built debug binary that `cargo test` already
    # produced in the parent shell; fall back to release.
    for mode in ("debug", "release"):
        p = WORKSPACE / "target" / mode / "shuflr"
        if p.exists():
            return p
    which = shutil.which("shuflr")
    if which:
        return Path(which)
    raise RuntimeError(
        "could not find the shuflr binary; build with `cargo build --features serve` "
        "or set SHUFLR_BIN"
    )


def _free_port() -> int:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


@pytest.fixture
def serve_factory(tmp_path):
    """Factory that spawns `shuflr serve` and tears it down cleanly."""
    procs = []

    def spawn(datasets: dict[str, Path]) -> dict:
        port = _free_port()
        args = [
            str(_find_shuflr_bin()),
            "serve",
            "--http",
            f"127.0.0.1:{port}",
            "--log-level",
            "info",
        ]
        for name, path in datasets.items():
            args.extend(["--dataset", f"{name}={path}"])
        proc = subprocess.Popen(
            args,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            bufsize=1,
            text=True,
        )
        procs.append(proc)

        # Wait for "serve(http) bound" on stderr.
        deadline = time.time() + 10.0
        while time.time() < deadline:
            line = proc.stderr.readline()
            if not line:
                rc = proc.poll()
                if rc is not None:
                    raise RuntimeError(
                        f"shuflr serve exited with {rc} before ready"
                    )
                continue
            sys.stderr.write(f"[serve] {line}")
            if "serve(http) bound" in line:
                break
        else:
            proc.kill()
            raise RuntimeError("shuflr serve never printed 'bound' in time")

        return {"port": port, "proc": proc, "base": f"http://127.0.0.1:{port}"}

    yield spawn

    for p in procs:
        p.terminate()
        try:
            p.wait(timeout=5)
        except subprocess.TimeoutExpired:
            p.kill()
            p.wait()


def test_dataset_iterates_records(serve_factory, tmp_path):
    ds_path = tmp_path / "corpus.jsonl"
    records = [f'{{"i":{i:03d}}}' for i in range(50)]
    ds_path.write_text("\n".join(records) + "\n")
    server = serve_factory({"corpus": ds_path})

    ds = shuflr_client.Dataset(
        f"{server['base']}/v1/streams/corpus",
        seed=7,
        shuffle="buffer",
    )
    out = list(ds)
    assert len(out) == len(records)
    # Multiset matches.
    assert sorted(out) == sorted(r.encode() for r in records)


def test_dataset_respects_sample(serve_factory, tmp_path):
    ds_path = tmp_path / "c.jsonl"
    ds_path.write_text("\n".join(f"r{i}" for i in range(200)) + "\n")
    server = serve_factory({"c": ds_path})

    ds = shuflr_client.Dataset(
        f"{server['base']}/v1/streams/c",
        seed=1,
        shuffle="none",
        sample=13,
    )
    out = list(ds)
    assert len(out) == 13


def test_transport_property():
    ds = shuflr_client.Dataset("http://x:9000/v1/streams/foo")
    assert ds.transport == "http"
    assert ds.dataset_id == "foo"

    ds_s = shuflr_client.Dataset("https://x:9000/v1/streams/foo")
    assert ds_s.transport == "https"


def test_wire_transport_raises_not_implemented():
    ds = shuflr_client.Dataset("shuflr://host:9000/corpus")
    assert ds.transport == "shuflr-wire/1"
    # We only see NotImplementedError on iteration, because construction
    # is lazy / URL-only.
    with pytest.raises(NotImplementedError):
        list(ds)


def test_unknown_scheme_rejected():
    with pytest.raises(ValueError):
        shuflr_client.Dataset("ftp://x/y")


def test_rank_requires_world_size():
    with pytest.raises(ValueError):
        shuflr_client.Dataset(
            "http://x:9000/v1/streams/foo",
            rank=0,
        )
