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

        # When py3.7 is EOL:
        # zip = zipfile.Path(path)
        # zipped_file = next(iter((f for f in zip.iterdir() if f.is_file())))
        # with zipped_file.open("rb") as handle:

        zip_file = zipfile.ZipFile(path)
        try:
            zipped_file = next(iter((f for f in zip_file.infolist() if not f.is_dir())))
        except StopIteration:  # pragma: no cover
            return
        with zip_file.open(zipped_file, "r") as handle:
            yield cast(BinaryIO, handle)

    @staticmethod
    @contextlib.contextmanager
    def write(path: pathlib.Path, **_: Any) -> Generator[BinaryIO, None, None]:
        """Return a file handle for writing."""

        with zipfile.ZipFile(path, "w") as zip_file:
            with zip_file.open(path.stem, "w") as handle:
                yield cast(BinaryIO, handle)
