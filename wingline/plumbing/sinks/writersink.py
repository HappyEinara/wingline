"""File writer sink."""

import pathlib
from typing import Optional

from wingline.files import containers, file, formats
from wingline.plumbing import pipe, sink
from wingline.types import PayloadIterable, PayloadIterator


class WriterSink(sink.Sink):

    emoji = "⇥💾"

    def __init__(
        self,
        parent: pipe.Pipe,
        path: pathlib.Path,
        name: str,
        format: Optional[formats.Format] = None,
        container: Optional[containers.Container] = None,
    ):
        super().__init__(parent, name)
        self._file = file.File(path, format, container)

    def _open_writer(self) -> None:
        self._writer = self._file.writer()
        self._write = self._writer.__enter__()

    def _close_writer(self) -> None:
        self._writer.__exit__(None, None, None)

    def process(self, payloads: PayloadIterable) -> PayloadIterator:
        """Write the payload."""
        for payload in payloads:
            self._write(payload)
            yield payload

    def setup(self) -> None:
        self._open_writer()

    def teardown(self) -> None:
        self._close_writer()
