"""Bare container for plain files."""
import contextlib
import pathlib
from typing import Any, BinaryIO, Generator, List

from wingline.files.containers import _base


class Bare(_base.Container):
    """Bare container for plain files."""

    suffixes: List[str] = []

    @staticmethod
    @contextlib.contextmanager
    def read(path: pathlib.Path, **_: Any) -> Generator[BinaryIO, None, None]:
        """Read handle."""

        with path.open("rb") as handle:
            yield handle

    @staticmethod
    @contextlib.contextmanager
    def write(path: pathlib.Path, **_: Any) -> Generator[BinaryIO, None, None]:
        """Write handle."""

        with path.open("wb") as handle:
            yield handle
