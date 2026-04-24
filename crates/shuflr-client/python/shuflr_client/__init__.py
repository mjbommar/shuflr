"""
shuflr-client — Python client for the `shuflr serve` HTTP / wire transports.

Supports HTTP(S) and plain `shuflr://` wire URLs. `shuflrs://` and
`shuflr+unix://` are parsed for forward compatibility but raise
`NotImplementedError` at open time.

Typical use:

    import shuflr_client

    ds = shuflr_client.Dataset(
        "http://127.0.0.1:9000/v1/streams/corpus",
        seed=42,
        shuffle="index-perm",
        sample=1000,
    )
    for record_bytes in ds:
        ...

For PyTorch training, wrap the Dataset with `shuflr_client.IterableDataset`
or hand it straight to `torch.utils.data.DataLoader(ds)`.
"""

from __future__ import annotations

from typing import Iterator, Optional

from ._shuflr_client_native import Dataset as _NativeDataset
from ._shuflr_client_native import __version__

__all__ = ["Dataset", "IterableDataset", "__version__"]


class Dataset:
    """Blocking iterator over records from a shuflr server.

    Each ``__next__`` returns one ``bytes`` record (no trailing newline).
    The stream opens lazily on the first ``iter()`` call and closes when
    the server runs out of records or ``--sample`` is exhausted.

    Parameters
    ----------
    url : str
        Server URL. ``http(s)://host[:port]/v1/streams/{dataset_id}``
        is the PR-34a form. ``shuflr(s)://`` will work in PR-36.
    seed : int, optional
        Reproducibility seed passed to the server (default 0).
    epochs : int, optional
        Number of passes over the dataset. ``0`` means infinite.
    shuffle : str, optional
        Shuffle mode. One of ``none``, ``buffer``, ``chunk-shuffled``,
        ``index-perm``, ``reservoir``. Default ``chunk-shuffled``.
    sample : int, optional
        Stop after this many records.
    rank, world_size : int, optional
        Distributed partition. Both must be set together.
    auth_token : str, optional
        Bearer token sent to auth-protected HTTP and wire servers.
    tls_ca_cert : str, optional
        PEM certificate bundle used to trust HTTPS servers with a
        private CA or self-signed certificate.
    timeout : float, optional
        Handshake timeout in seconds. Default 30.
    """

    def __init__(
        self,
        url: str,
        *,
        seed: int = 0,
        epochs: int = 1,
        shuffle: str = "chunk-shuffled",
        sample: Optional[int] = None,
        rank: Optional[int] = None,
        world_size: Optional[int] = None,
        auth_token: Optional[str] = None,
        tls_ca_cert: Optional[str] = None,
        timeout: float = 30.0,
    ) -> None:
        self._native = _NativeDataset(
            url,
            seed=seed,
            epochs=epochs,
            shuffle=shuffle,
            sample=sample,
            rank=rank,
            world_size=world_size,
            auth_token=auth_token,
            tls_ca_cert=tls_ca_cert,
            timeout=timeout,
        )

    def __iter__(self) -> Iterator[bytes]:
        return iter(self._native)

    @property
    def dataset_id(self) -> str:
        return self._native.dataset_id

    @property
    def url(self) -> str:
        return self._native.url

    @property
    def transport(self) -> str:
        return self._native.transport

    @property
    def bytes_received(self) -> int:
        """Application-layer bytes pulled off the socket so far. Useful
        for wire-size comparisons across protocols (see
        `docs/bench/002-protocols.md`)."""
        return self._native.bytes_received


def IterableDataset(*args, **kwargs):  # noqa: N802 — match torch naming
    """Return a ``torch.utils.data.IterableDataset`` wrapping
    :class:`Dataset`. Torch is an optional peer dep — if it isn't
    installed, we return the plain :class:`Dataset` (still iterable,
    just not a subclass of torch's class).

    PyTorch multi-worker: when invoked from inside a worker, this reads
    ``torch.utils.data.get_worker_info()`` and sets ``rank`` +
    ``world_size`` automatically unless the caller passed them
    explicitly. Each worker sees a disjoint slice of the shuffled
    stream (server-side rank-partitioning, 002 §2).
    """
    try:
        import torch.utils.data as _tud
    except ImportError:
        return Dataset(*args, **kwargs)

    class _TorchIterable(_tud.IterableDataset):  # type: ignore[misc]
        def __init__(self, *inner_args, **inner_kwargs):
            super().__init__()
            self._inner_args = inner_args
            self._inner_kwargs = dict(inner_kwargs)

        def __iter__(self):
            kwargs = dict(self._inner_kwargs)
            info = _tud.get_worker_info()
            if info is not None and "rank" not in kwargs:
                kwargs.setdefault("rank", int(info.id))
                kwargs.setdefault("world_size", int(info.num_workers))
            ds = Dataset(*self._inner_args, **kwargs)
            return iter(ds)

    return _TorchIterable(*args, **kwargs)
