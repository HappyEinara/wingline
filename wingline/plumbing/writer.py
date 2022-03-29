"""Plumbed writer class"""

from __future__ import annotations

import logging
import pathlib
from typing import Optional

from wingline.files import containers, formats, writer
from wingline.plumbing import base, sink
from wingline.types import SENTINEL, PayloadIterator

logger = logging.getLogger(__name__)


class Writer(sink.Sink):
    def __init__(
        self,
        parent: base.BasePlumbing,
        path: pathlib.Path,
        format: type[formats.Format],
        container: Optional[type[containers.Container]] = None,
        name: Optional[str] = None,
    ):
        name = name if name is not None else self.__class__.name
        super().__init__(parent, name)
        self.path = path
        self.format = format
        self.container = container
        self.start_hooks.append(self.open_writer)
        self.end_hooks.append(self.close_writer)
        self.input_hooks.append(self.write)

    def open_writer(self, *_):
        self._writer = writer.Writer(self.path, self.format, self.container)
        self._write = self._writer.__enter__()

    def close_writer(self, *_):
        self._writer.__exit__(None, None, None)

    def write(self, _, payloads: PayloadIterator) -> PayloadIterator:
        """Write an item to the file."""
        for payload in payloads:
            if payload is not SENTINEL:
                self._write(payload)
            yield payload
