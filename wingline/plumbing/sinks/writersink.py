"""File writer sink."""

import pathlib
from typing import Optional

from wingline.files import containers, file, formats
from wingline.plumbing import pipe, sink
from wingline.types import Payload


class WriterSink(sink.Sink):
    def __init__(
        self,
        parent: pipe.Pipe,
        path: pathlib.Path,
        name: str,
        format_type: Optional[type[formats.Format]] = None,
        container_type: Optional[type[containers.Container]] = None,
    ):
        super().__init__(parent, name)
        self._file = file.File(path, format_type, container_type)

    def _open_writer(self):
        self._writer = self._file.writer
        self._write = self._writer.__enter__()

    def _close_writer(self, *_):
        self._writer.__exit__(None, None, None)

    def process(self, payload: Payload):
        """Write the payload."""
        self._write(payload)
        yield payload

    def setup(self):
        self._open_writer()

    def teardown(self):
        self._close_writer()
