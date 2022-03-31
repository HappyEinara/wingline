"""Zip container."""

import contextlib
import zipfile
import pathlib
from typing import BinaryIO, Generator, cast

from wingline.files.containers import _base


class Zip(_base.Container):
    """Zip container"""

    mime_type = "application/zip"

    @contextlib.contextmanager
    @staticmethod
    def _get_handle(path: pathlib.Path) -> Generator[BinaryIO, None, None]:
        """Return a file handle."""

        zip = zipfile.Path(path)
        zipped_file = next(zip.iterdir())
        with zipped_file.open("rb") as handle:
            yield cast(BinaryIO, handle)

    @staticmethod
    @contextlib.contextmanager
    def _get_write_handle(path: pathlib.Path) -> Generator[BinaryIO, None, None]:
        """Return a file handle for writing."""

        with gzip.open(path, "wb") as handle:
            yield cast(BinaryIO, handle)
