"""Type stubs for the Rust-backed native extension."""
from __future__ import annotations
from typing import Iterator, Optional

__version__: str

class Dataset:
    def __init__(
        self,
        url: str,
        *,
        seed: int = 0,
        shuffle: str = "chunk-shuffled",
        sample: Optional[int] = None,
        rank: Optional[int] = None,
        world_size: Optional[int] = None,
        timeout: float = 30.0,
    ) -> None: ...

    def __iter__(self) -> Iterator[bytes]: ...
    def __next__(self) -> bytes: ...

    @property
    def dataset_id(self) -> str: ...
    @property
    def url(self) -> str: ...
    @property
    def transport(self) -> str: ...
    @property
    def bytes_received(self) -> int: ...
