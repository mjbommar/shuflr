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

TEST_TLS_CA = """-----BEGIN CERTIFICATE-----
MIIDIzCCAgugAwIBAgIUNMrG6JoZiwm20ubiX7LQJIFXSpQwDQYJKoZIhvcNAQEL
BQAwGTEXMBUGA1UEAwwOc2h1ZmxyLXRlc3QtY2EwHhcNMjYwNDI0MDExOTMzWhcN
MzYwNDIxMDExOTMzWjAZMRcwFQYDVQQDDA5zaHVmbHItdGVzdC1jYTCCASIwDQYJ
KoZIhvcNAQEBBQADggEPADCCAQoCggEBAMl0E95WnYouI17R4Aa76rYk7e1hraz5
tSKkVADhQRftlIkSx6tHE2s3OZGIeH0xGqUnnOxUD992jPKIZL8G+/y7RFpiRv1S
kcymjLNnNiX6TZwhfzMe8AJcfI5jWCafBDWv5fh0FM8CRUnvxJ3AMIiewZipdoUk
GofdrIYIruOLosbbXh/bn7OvqpXN7WX9HaUdan/Tv6/sE9ZdQeMj5zkIW196AruX
ngyhOVvBnPT2Eet+IuPvJlIPIFGLXI1mnUjVCLpHrSHarOyp1PJ7M6L6MmlfdKFx
KVvSK/LMnDwh3eGr4N6QNMj5v8UJzxIdUZbdDlbvsMoTnjyPfVFrdrkCAwEAAaNj
MGEwHQYDVR0OBBYEFHugwqAC4KBa8Fub2876AknkG3+bMB8GA1UdIwQYMBaAFHug
wqAC4KBa8Fub2876AknkG3+bMA8GA1UdEwEB/wQFMAMBAf8wDgYDVR0PAQH/BAQD
AgEGMA0GCSqGSIb3DQEBCwUAA4IBAQAjEZhOltG7hG4lrHFCExoljNeesvVjlO6x
3ee4Syz5rlaFbsCVseHRZgP/b1BHix1ZbfpnLyVZQe/+j+qTcBBbr5aUepHw8iOd
ARcVGRdxSSbWXiiW9JZl1VfsWAUBUO3aSakkHlxYxJsN4lXAOTGReHnU4CQnnW9u
Iu435wlVW83HEwHlIlDLX+at6NxTmeD4d86qCddyWY+vddwz75C4ZfrDxTXtBAfm
rpwFbdmVzRYKojz8M3jT93QYdvK3A5UVERQ5eMbJtPcbFn+JMbolH0QwsKG7xAw3
1skYVikvaZwNGH86Ju3h5UaNxctER5pAmGI9Xiq0aIqzMBxrUwFO
-----END CERTIFICATE-----
"""

TEST_TLS_CERT = """-----BEGIN CERTIFICATE-----
MIIDTjCCAjagAwIBAgIUZfo147MuF+0xvY3mtuFxgmdM49QwDQYJKoZIhvcNAQEL
BQAwGTEXMBUGA1UEAwwOc2h1ZmxyLXRlc3QtY2EwHhcNMjYwNDI0MDExOTMzWhcN
MzYwNDIxMDExOTMzWjAUMRIwEAYDVQQDDAlsb2NhbGhvc3QwggEiMA0GCSqGSIb3
DQEBAQUAA4IBDwAwggEKAoIBAQDGRsgmivYErWiNudwBR1i5931ZBgWgyZcsxpg3
uingOBazRmcLxaRJyuARCjmodKyv4RlH97Bc1jcoSNA2wyQ7jpeBtQyZWKk+ITcy
L26uYf5iF9oEdVjBJwL/jowAXEC9DSMnS33INqefStuzYbwBq6xdEgfK1GG7r02F
VKU1S5Ao5C+V14XRpxDfcKXyhXnOFAgQ8mQBwQwtI0uFGePWFl+9QyJ3AdPSYa3s
m6IPRm9UY7P1jAI/DJ1bYT7kCFsKqsfJq45ciroUoyT9JSw+Vbny+g9WdHf/D8jn
WfnWR5t3eb+sPHMHcbSSSEJcEUh9u5x8JBSZLva08+6YE9RJAgMBAAGjgZIwgY8w
DAYDVR0TAQH/BAIwADAOBgNVHQ8BAf8EBAMCBaAwEwYDVR0lBAwwCgYIKwYBBQUH
AwEwGgYDVR0RBBMwEYIJbG9jYWxob3N0hwR/AAABMB0GA1UdDgQWBBQKcm3QIfNN
10Wy+85ysCxrJ0N6PjAfBgNVHSMEGDAWgBR7oMKgAuCgWvBbm9vO+gJJ5Bt/mzAN
BgkqhkiG9w0BAQsFAAOCAQEAm5BP9+IxSneE8XMRcthGO+N3sKDkzW6/cFRJPC/E
97FLWVE5dY1NyaPZLJnCkBIRqTgkoSVXp016/t5BqqzJayIZVkfSh3HHYGnS1TDt
S1JLKIH7hzBURlRtwE561mfJ3nI9jNxxccM0oC4FIb5WkezowyqCc987CBTr6TnF
6+XcjcMAoKyOLijOU5BFqUV3ILj/rhDC3qyxv77XgqIq0P0KxEtIc65wuejcVDK8
QIiJwuVjX+WMbLKt8miGsaeJNKNFxEyN0K7G+tSb1MrsD2RrnCLek9v8HEfUw1SH
wUCLAWVbmQ6fMn2ouUGdwrfnj93CVL0H4VcFPOYn3jrdYw==
-----END CERTIFICATE-----
"""

TEST_TLS_KEY = """-----BEGIN PRIVATE KEY-----
MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDGRsgmivYErWiN
udwBR1i5931ZBgWgyZcsxpg3uingOBazRmcLxaRJyuARCjmodKyv4RlH97Bc1jco
SNA2wyQ7jpeBtQyZWKk+ITcyL26uYf5iF9oEdVjBJwL/jowAXEC9DSMnS33INqef
StuzYbwBq6xdEgfK1GG7r02FVKU1S5Ao5C+V14XRpxDfcKXyhXnOFAgQ8mQBwQwt
I0uFGePWFl+9QyJ3AdPSYa3sm6IPRm9UY7P1jAI/DJ1bYT7kCFsKqsfJq45ciroU
oyT9JSw+Vbny+g9WdHf/D8jnWfnWR5t3eb+sPHMHcbSSSEJcEUh9u5x8JBSZLva0
8+6YE9RJAgMBAAECggEAJLIscG+3BRoX9+T+WhE7k12B6jRCXeX5b+zIdsXiYrBW
yTK+NxBK7epZimXPUoVXgYyiWm61yhTObAr/2CbJIamsEjIxJP9jAU/x8HyVWjpi
M4LxE3KpbMQc2rHl6NoqFCMmjqr62k99Oe7Hlx0/R7rfYXFJFmm0teEFsqe+FMz2
MfGSKjbzYQltthSCByAecwmpg6BNKw9U6l55hpYg1ZArwWebGAxmfntpbNISO29w
HlmybFtCvujOwC4fFfFUFFWXXtXOM+n1ubL6M+PCDWrFAvB5BIO8qKDfSqqZz2fn
d3eMTCx3Cl557GhHP3kkiPg9OuCEiUbuNJvSBYUgRQKBgQDnjltLxxLOLf9R4vHN
8F0VJEPsVunvtLTcwJK6mUCoB+8hU/OW4Y3UX4VCLWsuAfWXL+7LTgeR5YX9wueo
wOHGNgVtyp5+DudBi3uX1JwT9qZfM2THVFWfHGLyXhtB2lcvKdp/yobgRdqv1P5s
wRdFJ5tddKisFNXaQZP22LV99QKBgQDbNRE4yPThF3vZ0omS655mjIwOYbFFDYYY
E5xJlOo7HCqPrwg0/OYnb71D3g6PrfyoFqhBAhxpFe1wHNVZimXXQ320cpeWPMLI
sA8jnCpkt4zxUcjlnMObidWEKfFvLG5StFAiJNeImOBGzGKwT6ANxV6zLwTY3QEr
BgeGjIFUhQKBgBh3rWPzdCQ/LgRsE5rsNBnAzECT6oI+uQG+g4KeIPvgYr9FzK57
xO3U7hLRE8s3v8iq8vOemiQreZ4X0zy8rN6x5J72UwsE7iC56WRgveFKJchXeOWr
HqUCbd4oXX724FGGfaUVNG1MVFBSFFRPjvLqvXsBkbUlOnemiEkEGyFlAoGBAMCe
nQd87lFsxVPegS0tBf+uuNNaXN8ExzQY51hxFnHiijO/5kJJiCRXN8SPN9RhWoaJ
gke9hyGANygw7fjEeEDz+V265CEMO00GCAeOjmH6OEtFRncdjXT7ZTfBc3nxXPAn
qdKW9R3+1/TeXEn5bfcr288wHd5CNWiM38gHZw3RAoGBAMJ/N1Inf5g/pyEK4bmG
ZZ+2ZEgs2b7Vxp6kHzBpU/TTj7Ip/SkJPPPXHJWRID3VGdbhR6ykT10rpuoSdO+E
RON9SGU501fQ1znYDjfXWqjBabAJr/6osY3hjC/eMe74ZHUBwABG7T7dHPbGi9Ct
YSALHKYajrsQqgvQKFb0/4VI
-----END PRIVATE KEY-----
"""


def _write_test_tls_files(tmp_path: Path) -> tuple[Path, Path, Path]:
    ca = tmp_path / "test-ca.pem"
    cert = tmp_path / "localhost-cert.pem"
    key = tmp_path / "localhost-key.pem"
    ca.write_text(TEST_TLS_CA)
    cert.write_text(TEST_TLS_CERT)
    key.write_text(TEST_TLS_KEY)
    return ca, cert, key


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


def test_dataset_respects_epochs(serve_factory, tmp_path):
    ds_path = tmp_path / "c.jsonl"
    ds_path.write_text("a\nb\nc\n")
    server = serve_factory({"c": ds_path})

    ds = shuflr_client.Dataset(
        f"{server['base']}/v1/streams/c",
        shuffle="none",
        epochs=2,
    )
    assert list(ds) == [b"a", b"b", b"c", b"a", b"b", b"c"]


def test_https_dataset_connects_with_custom_ca(serve_factory, tmp_path):
    ds_path = tmp_path / "c.jsonl"
    ds_path.write_text("secure\n")
    ca, cert, key = _write_test_tls_files(tmp_path)
    server = serve_factory(
        {"c": ds_path},
        extra=["--tls-cert", str(cert), "--tls-key", str(key)],
    )

    ds = shuflr_client.Dataset(
        f"https://localhost:{server['port']}/v1/streams/c",
        shuffle="none",
        tls_ca_cert=str(ca),
    )
    assert list(ds) == [b"secure"]


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


def test_http_bearer_token_authenticates(serve_factory, tmp_path):
    ds_path = tmp_path / "c.jsonl"
    ds_path.write_text("x\n")
    tokens = tmp_path / "tok.txt"
    tokens.write_text("realtoken\n")
    server = serve_factory(
        {"c": ds_path},
        extra=["--auth", "bearer", "--auth-tokens", str(tokens)],
    )

    ds = shuflr_client.Dataset(
        f"{server['base']}/v1/streams/c",
        shuffle="none",
        auth_token="realtoken",
    )
    assert list(ds) == [b"x"]


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


def test_wire_dataset_respects_epochs(serve_factory, tmp_path):
    ds_path = tmp_path / "c.jsonl"
    ds_path.write_text("a\nb\nc\n")
    server = serve_factory({"c": ds_path}, transport="wire")

    ds = shuflr_client.Dataset(
        f"{server['base']}/c",
        shuffle="none",
        epochs=2,
    )
    assert list(ds) == [b"a", b"b", b"c", b"a", b"b", b"c"]


def test_wire_bearer_token_authenticates(serve_factory, tmp_path):
    ds_path = tmp_path / "c.jsonl"
    ds_path.write_text("x\n")
    tokens = tmp_path / "tok.txt"
    tokens.write_text("realtoken\n")
    server = serve_factory(
        {"c": ds_path},
        transport="wire",
        extra=["--auth", "bearer", "--auth-tokens", str(tokens)],
    )

    ds = shuflr_client.Dataset(
        f"{server['base']}/c",
        shuffle="none",
        auth_token="realtoken",
    )
    assert list(ds) == [b"x"]


def test_wire_rejects_unknown_dataset(serve_factory, tmp_path):
    ds_path = tmp_path / "c.jsonl"
    ds_path.write_text("x\n")
    server = serve_factory({"real": ds_path}, transport="wire")
    ds = shuflr_client.Dataset(f"{server['base']}/missing")
    with pytest.raises(OSError):
        list(ds)


# ---------------- raw-frame passthrough (PR-33b) ----------------


def _convert_to_zstd(plain_path: Path, out_path: Path) -> None:
    """Run `shuflr convert` to produce a seekable-zstd file."""
    bin_path = _find_shuflr_bin()
    subprocess.run(
        [
            str(bin_path),
            "convert",
            "--log-level",
            "warn",
            "-o",
            str(out_path),
            str(plain_path),
        ],
        check=True,
    )


def test_wire_raw_frame_preserves_multiset(serve_factory, tmp_path):
    """Raw-frame mode is selected automatically when shuffle=chunk-
    shuffled on a seekable-zstd corpus. Server ships compressed frames;
    client decompresses + Fisher-Yates locally. Output multiset must
    match the input."""
    plain = tmp_path / "c.jsonl"
    seekable = tmp_path / "c.jsonl.zst"
    records = [f'{{"i":{i:04d}}}' for i in range(800)]
    plain.write_text("\n".join(records) + "\n")
    _convert_to_zstd(plain, seekable)
    server = serve_factory({"corpus": seekable}, transport="wire")

    ds = shuflr_client.Dataset(
        f"{server['base']}/corpus",
        seed=42,
        shuffle="chunk-shuffled",
    )
    got = [r.decode() for r in ds]
    assert sorted(got) == sorted(records), (
        f"multiset mismatch: got {len(got)} records"
    )
    # Output is shuffled (probabilistically !=). Use a sentinel
    # record count high enough to make equal order vanishingly unlikely
    # under a real shuffle.
    assert got != records


def test_wire_raw_frame_respects_sample(serve_factory, tmp_path):
    """sample=N must hold on the wire raw-frame path. The server
    trims at frame boundaries so it can ship a whole 800-record
    frame even when the user asked for 5; the client has to enforce
    the cap to honor the API contract. Regression for the 0.1.0
    behavior where the iterator over-emitted on chunk-shuffled +
    wire."""
    plain = tmp_path / "c.jsonl"
    seekable = tmp_path / "c.jsonl.zst"
    records = [f'{{"i":{i:04d}}}' for i in range(800)]
    plain.write_text("\n".join(records) + "\n")
    _convert_to_zstd(plain, seekable)
    server = serve_factory({"corpus": seekable}, transport="wire")

    for n in (1, 5, 17, 200):
        ds = shuflr_client.Dataset(
            f"{server['base']}/corpus",
            seed=42,
            shuffle="chunk-shuffled",
            sample=n,
        )
        got = list(ds)
        assert len(got) == n, f"sample={n} on wire raw-frame: got {len(got)}"


def test_wire_raw_and_plain_multiset_match(serve_factory, tmp_path):
    """Raw-frame path (chunk-shuffled on seekable-zstd) and plain-
    batch path (shuffle=buffer) both see the same input, so their
    multisets must match. Order differs because the two paths derive
    RNG streams at different points; that's fine."""
    plain = tmp_path / "c.jsonl"
    seekable = tmp_path / "c.jsonl.zst"
    records = [f'r{i:04d}' for i in range(300)]
    plain.write_text("\n".join(records) + "\n")
    _convert_to_zstd(plain, seekable)
    server = serve_factory({"corpus": seekable}, transport="wire")

    ds_raw = shuflr_client.Dataset(
        f"{server['base']}/corpus",
        seed=11,
        shuffle="chunk-shuffled",
    )
    raw_records = [r.decode() for r in ds_raw]
    assert sorted(raw_records) == sorted(records)

    ds_plain = shuflr_client.Dataset(
        f"{server['base']}/corpus",
        seed=11,
        shuffle="buffer",
    )
    plain_records = [r.decode() for r in ds_plain]
    assert sorted(plain_records) == sorted(records)
