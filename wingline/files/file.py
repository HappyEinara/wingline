"""Abstract reader/writer of line-delimited data."""
from __future__ import annotations

import logging
import os
import pathlib
from typing import Any, Generator, Optional

from wingline import hasher
from wingline.files import filetype

logger = logging.getLogger(__name__)


class File:
    def __init__(self, path: pathlib.Path):
        self.path = path
        self.reader = filetype.get_reader(self.path)

    @property
    def stat(self) -> Optional[os.stat_result]:
        if self.exists:
            return self.path.stat()

    @property
    def size(self) -> Optional[int]:
        if stat := self.stat:
            return stat.st_size

    @property
    def modified_at(self) -> Optional[int]:
        if stat := self.stat:
            return stat.st_mtime_ns

    @property
    def content_hash(self):
        return hasher.hash_file(self.path)

    @property
    def output_hash(self):
        return self.content_hash

    @property
    def exists(self):
        return self.path.exists()

    def _iterator(self) -> Generator[dict[str, Any], None, None]:
        """Iterate over the lines in the file."""
        with self.reader as reader:
            for line in reader:
                yield line

    def __iter__(self) -> Generator[dict[str, Any], None, None]:
        """Return self to be used as an iterator."""

        return self._iterator()

    def __str__(self):
        return str(self.path.name)

    def __repr__(self):
        hash = f"|{self.content_hash}" if self.content_hash else ""
        return f"<F {str(self)}{hash}>"
