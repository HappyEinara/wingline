"""Gzip container."""

import contextlib
import gzip
import pathlib
from typing import Any, BinaryIO, Generator, cast

from wingline.files.containers import _base


class Gzip(_base.Container):
    """Gzip container"""

    suffixes = [".gz", ".gzip"]

    @staticmethod
    @contextlib.contextmanager
    def read(path: pathlib.Path, **_: Any) -> Generator[BinaryIO, None, None]:
        """Return a file handle."""

        with gzip.open(path) as handle:
            yield cast(BinaryIO, handle)

    @staticmethod
    @contextlib.contextmanager
    def write(path: pathlib.Path, **_: Any) -> Generator[BinaryIO, None, None]:
        """Return a file handle for writing."""

        with gzip.open(path, "wb") as handle:
            yield cast(BinaryIO, handle)
