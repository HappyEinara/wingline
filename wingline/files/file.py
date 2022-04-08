"""Abstract reader/writer of line-delimited data."""
from __future__ import annotations

import logging
import os
import pathlib
from typing import Optional

from wingline import hasher
from wingline.files import filetype as ft
from wingline.files import reader, writer
from wingline.types import PayloadIterator

logger = logging.getLogger(__name__)


class File:
    """High-level abstraction of a data file."""

    def __init__(self, path: pathlib.Path, filetype: Optional[ft.Filetype] = None):
        self.path = path
        self.filetype = (
            filetype if filetype is not None else ft.detect_filetype(self.path)
        )

    @property
    def stem(self) -> str:
        """Stem of the file."""
        return self.path.stem.partition(".")[0]

    @property
    def stat(self) -> Optional[os.stat_result]:
        """Stat of the file."""
        if self.exists:
            return self.path.stat()
        return None

    @property
    def size(self) -> Optional[int]:
        """Size of the file."""
        stat = self.stat
        if stat:
            return stat.st_size
        return None

    @property
    def modified_at(self) -> Optional[int]:
        """File modification date."""
        stat = self.stat
        if stat:
            return stat.st_mtime_ns
        return None

    @property
    def content_hash(self) -> str:
        """Hash of the file's content."""
        return hasher.hash_file(self.path)

    def reader(self) -> reader.Reader:
        """An iterating reader for the file."""
        if not self.exists:
            raise RuntimeError("Can't read from a nonexistent file.")
        return reader.Reader(self.path, self.filetype)

    def writer(self) -> writer.Writer:
        """Return a writer manager for the file."""

        if self.exists:
            raise RuntimeError("Can't write to a file that exists.")
        return writer.Writer(self.path, self.filetype)

    @property
    def exists(self) -> bool:
        """Return True if the file exists."""

        return self.path.exists()

    def __iter__(self) -> PayloadIterator:
        """Return self to be used as an iterator."""
        with self.reader() as iter_reader:
            for line in iter_reader:
                yield line

    def __str__(self) -> str:
        return str(self.stem)

    def __repr__(self) -> str:
        content_hash = f"|{self.content_hash}" if self.content_hash else ""
        return f"<F {str(self)}{content_hash}>"
