"""A caching pipe."""
# pylint: disable=duplicate-code

import pathlib
from typing import Optional

from wingline.cache import intermediate
from wingline.files import file, writer
from wingline.plumbing import pipe
from wingline.types import PayloadIterable, PayloadIterator, WritePointer


class CacheWriter(pipe.Pipe):
    """A pipe to write the parent's output to a cache file."""

    emoji = "⏩💾"

    def __init__(
        self,
        parent: pipe.BasePipe,
        cache_path: pathlib.Path,
        name: Optional[str] = None,
    ):
        name = name if name is not None else f"cachewriter-{cache_path.name}"
        super().__init__(parent, name)
        self._file = file.File(cache_path, intermediate.filetype)
        self._writer: Optional[writer.Writer] = None
        self._write: Optional[WritePointer] = None

    def process(self, payloads: PayloadIterable) -> PayloadIterator:
        """Write the payload."""

        if not self._write:  # pragma: no cover
            raise RuntimeError("Can't write payload to cache: was the writer opened?")

        for payload in payloads:
            self._write(payload)
            yield payload

    def setup(self) -> None:
        """Open the writer."""

        self._writer = self._file.writer()
        if not self._writer:  # pragma: no cover
            raise RuntimeError("File has no writer.")
        self._write = self._writer.__enter__()

    def teardown(self, success: bool = False) -> None:
        """Close the writer."""
        if not self._writer:  # pragma: no cover
            raise RuntimeError("Can't close writer: was it opened?")
        self._writer.__exit__(None, None, None, success=success)
