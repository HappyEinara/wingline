"""Gzip container."""

import contextlib
import gzip
import pathlib
from typing import BinaryIO, Generator, cast

from wingline.files.containers import _base


class Gzip(_base.Container):
    """Gzip container"""

    mime_type = "application/gzip"
    suffixes = {".gz", ".gzip"}

    @contextlib.contextmanager
    @staticmethod
    def _get_handle(path: pathlib.Path) -> Generator[BinaryIO, None, None]:
        """Return a file handle."""

        with gzip.open(path) as handle:
            yield cast(BinaryIO, handle)

    @staticmethod
    @contextlib.contextmanager
    def _get_write_handle(path: pathlib.Path) -> Generator[BinaryIO, None, None]:
        """Return a file handle for writing."""

        with gzip.open(path, "wb") as handle:
            yield cast(BinaryIO, handle)
