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


def _spawn_serve(
    datasets: dict[str, Path],
    transport: str = "http",
    extra: list[str] | None = None,
) -> dict:
    """Spawn `shuflr serve` with the requested transport, wait for
    ready, and return {'port', 'proc', 'base'}. Reader thread stays
    running to keep stderr drained (otherwise the pipe fills and the
    server blocks on its next log write — see PR-33 commit message)."""
    port = _free_port()
    listener_flag = {
        "http": "--http",
        "wire": "--wire",
    }[transport]
    args = [
        str(_find_shuflr_bin()),
        "serve",
        listener_flag,
        f"127.0.0.1:{port}",
        "--log-level",
        "info",
    ]
    for name, path in datasets.items():
        args.extend(["--dataset", f"{name}={path}"])
    if extra:
        args.extend(extra)
    proc = subprocess.Popen(
        args,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        bufsize=1,
        text=True,
    )

    ready_marker = f"serve({transport}) bound"
    deadline = time.time() + 10.0
    ready = False
    # Drain stderr continuously in a background thread; only the ready
    # marker unblocks us.
    import threading

    ready_evt = threading.Event()

    def _drain():
        for line in proc.stderr:
            sys.stderr.write(f"[serve-{transport}] {line}")
            if ready_marker in line:
                ready_evt.set()

    threading.Thread(target=_drain, daemon=True).start()
    while time.time() < deadline:
        if ready_evt.wait(timeout=0.05):
            ready = True
            break
        if proc.poll() is not None:
            raise RuntimeError(f"shuflr serve exited early (rc={proc.returncode})")
    if not ready:
        proc.kill()
        raise RuntimeError(f"shuflr serve never logged '{ready_marker}' in time")

    base = {
        "http": f"http://127.0.0.1:{port}",
        "wire": f"shuflr://127.0.0.1:{port}",
    }[transport]
    return {"port": port, "proc": proc, "base": base}


@pytest.fixture
def serve_factory(tmp_path):
    """Spawn one or more `shuflr serve` instances; tear them down
    cleanly at test end."""
    procs = []

    def spawn(datasets: dict[str, Path], transport: str = "http", extra=None) -> dict:
        info = _spawn_serve(datasets, transport=transport, extra=extra)
        procs.append(info["proc"])
        return info

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


def test_wire_tls_and_unix_raise_not_implemented():
    """shuflrs:// (TLS) and shuflr+unix:// (UDS) parse but aren't wired yet."""
    for url in ("shuflrs://host:9000/corpus", "shuflr+unix:///tmp/sock/corpus"):
        ds = shuflr_client.Dataset(url)
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


def test_bearer_token_missing_raises(serve_factory, tmp_path):
    """Client hits a bearer-auth'd server without a token → the
    sentinel surfaces as OSError (via IOError) on iteration."""
    ds_path = tmp_path / "c.jsonl"
    ds_path.write_text("x\n")
    # Reuse the existing loopback-no-TLS server fixture but force it
    # behind bearer auth. Since serve_factory doesn't take auth args,
    # spawn directly here via the same idiom.
    import socket as _s
    import subprocess as _sp
    import time as _t

    s = _s.socket(_s.AF_INET, _s.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()

    tokens = tmp_path / "tok.txt"
    tokens.write_text("realtoken\n")

    bin_path = _find_shuflr_bin()
    proc = subprocess.Popen(
        [
            str(bin_path),
            "serve",
            "--http",
            f"127.0.0.1:{port}",
            "--auth",
            "bearer",
            "--auth-tokens",
            str(tokens),
            "--dataset",
            f"c={ds_path}",
            "--log-level",
            "info",
        ],
        stdout=_sp.PIPE,
        stderr=_sp.PIPE,
        text=True,
    )
    try:
        deadline = _t.time() + 10
        while _t.time() < deadline:
            line = proc.stderr.readline()
            if not line:
                if proc.poll() is not None:
                    raise RuntimeError("server died early")
                continue
            if "bound" in line:
                break

        # No token → server returns 401 at the HTTP layer before any
        # body. Our client surfaces that as OSError / IOError.
        ds = shuflr_client.Dataset(
            f"http://127.0.0.1:{port}/v1/streams/c", shuffle="none"
        )
        with pytest.raises(OSError):
            list(ds)
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except _sp.TimeoutExpired:
            proc.kill()
            proc.wait()


# ---------------- shuflr-wire/1 (PR-34b) ----------------


def test_wire_dataset_iterates_records(serve_factory, tmp_path):
    """Bare shuflr://host:port/{dataset} end-to-end through the Rust
    wire client against a real `shuflr serve --wire` subprocess."""
    ds_path = tmp_path / "c.jsonl"
    records = [f'{{"i":{i:03d}}}' for i in range(40)]
    ds_path.write_text("\n".join(records) + "\n")
    server = serve_factory({"c": ds_path}, transport="wire")

    ds = shuflr_client.Dataset(
        f"{server['base']}/c",
        seed=7,
        shuffle="buffer",
    )
    assert ds.transport == "shuflr-wire/1"
    got = [r.decode() for r in ds]
    assert sorted(got) == sorted(records)


def test_wire_dataset_respects_sample(serve_factory, tmp_path):
    ds_path = tmp_path / "c.jsonl"
    ds_path.write_text("\n".join(f"r{i:03d}" for i in range(500)) + "\n")
    server = serve_factory({"c": ds_path}, transport="wire")

    ds = shuflr_client.Dataset(
        f"{server['base']}/c",
        seed=1,
        shuffle="none",
        sample=23,
    )
    got = list(ds)
    assert len(got) == 23


def test_wire_rejects_unknown_dataset(serve_factory, tmp_path):
    ds_path = tmp_path / "c.jsonl"
    ds_path.write_text("x\n")
    server = serve_factory({"real": ds_path}, transport="wire")
    ds = shuflr_client.Dataset(f"{server['base']}/missing")
    with pytest.raises(OSError):
        list(ds)
