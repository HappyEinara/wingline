"""Zip container."""

import contextlib
import pathlib
import zipfile
from typing import Any, BinaryIO, Generator, cast

from wingline.files.containers import _base


class Zip(_base.Container):
    """Zip container"""

    suffixes = [".zip"]

    @staticmethod
    @contextlib.contextmanager
    def read(path: pathlib.Path, **_: Any) -> Generator[BinaryIO, None, None]:
        """Return a file handle."""

        zip = zipfile.Path(path)
        zipped_file = next(iter((f for f in zip.iterdir() if f.is_file())))
        with zipped_file.open("rb") as handle:
            yield cast(BinaryIO, handle)

    @staticmethod
    @contextlib.contextmanager
    def write(path: pathlib.Path, **_: Any) -> Generator[BinaryIO, None, None]:
        """Return a file handle for writing."""

        with zipfile.ZipFile(path, "w") as zip:
            with zip.open(path.stem, "w") as handle:
                yield cast(BinaryIO, handle)
