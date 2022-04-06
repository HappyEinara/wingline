"""Abstract reader/writer of line-delimited data."""
from __future__ import annotations

import logging
import os
import pathlib
from typing import Optional

from wingline import hasher
from wingline.files import containers, detect, formats, reader, writer
from wingline.types import PayloadIterator

logger = logging.getLogger(__name__)

DEFEAULT_CONTAINER = containers.Bare


class File:
    def __init__(
        self,
        path: pathlib.Path,
        format: Optional[formats.Format] = None,
        container: Optional[containers.Container] = None,
    ):
        self.path = path
        if not format:
            format_type, container_type = detect.detect_filetypes(self.path)
            self.format = format_type()
            self.container = (
                container_type() if container_type is not None else DEFEAULT_CONTAINER()
            )
        else:
            self.format = format
            self.container = (
                container if container is not None else DEFEAULT_CONTAINER()
            )

    @property
    def stem(self) -> str:
        return self.path.stem.partition(".")[0]

    @property
    def stat(self) -> Optional[os.stat_result]:
        if self.exists:
            return self.path.stat()
        return None

    @property
    def size(self) -> Optional[int]:
        if stat := self.stat:
            return stat.st_size
        return None

    @property
    def modified_at(self) -> Optional[int]:
        if stat := self.stat:
            return stat.st_mtime_ns
        return None

    @property
    def content_hash(self) -> str:
        return hasher.hash_file(self.path)

    def reader(self) -> reader.Reader:
        if not self.exists:
            raise RuntimeError("Can't read from a nonexistent file.")
        return reader.Reader(
            self.path,
            container=self.container,
            format=self.format,
        )

    def writer(self) -> writer.Writer:
        if self.exists:
            raise RuntimeError("Can't write to a file that exists.")
        return writer.Writer(self.path, self.format, self.container)

    @property
    def exists(self) -> bool:
        return self.path.exists()

    def __iter__(self) -> PayloadIterator:
        """Return self to be used as an iterator."""
        with self.reader() as reader:
            for line in reader:
                yield line

    def __str__(self) -> str:
        return str(self.stem)

    def __repr__(self) -> str:
        hash = f"|{self.content_hash}" if self.content_hash else ""
        return f"<F {str(self)}{hash}>"
