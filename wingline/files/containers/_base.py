"""Container base class"""
from __future__ import annotations

import contextlib
import pathlib
from typing import BinaryIO, Generator

DEFAULT_CONTAINER_MIME_TYPE = "_default"


class Container:
    """Base class for a file container."""

    mime_type: str = DEFAULT_CONTAINER_MIME_TYPE

    def __init__(self, path: pathlib.Path):
        self.path = path

    @staticmethod
    @contextlib.contextmanager
    def _get_handle(path: pathlib.Path) -> Generator[BinaryIO, None, None]:
        """Return a file handle."""

        with path.open("rb") as handle:
            yield handle

    @staticmethod
    @contextlib.contextmanager
    def _get_write_handle(path: pathlib.Path) -> Generator[BinaryIO, None, None]:
        """Return a file handle for writing."""

        with path.open("wb") as handle:
            yield handle

    @contextlib.contextmanager
    def handle(self) -> Generator[BinaryIO, None, None]:
        with self.__class__._get_handle(self.path) as handle:
            yield handle

    @contextlib.contextmanager
    def write_handle(self) -> Generator[BinaryIO, None, None]:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.__class__._get_write_handle(self.path) as handle:
            yield handle
