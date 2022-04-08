"""File writer sink."""


import pathlib
from typing import Union

from wingline.files import file as fl
from wingline.files import writer
from wingline.plumbing import pipe, sink
from wingline.types import PayloadIterable, PayloadIterator, WritePointer


class WriterSink(sink.Sink):
    """A sink that writes to a file."""

    emoji = "⇥💾"

    def __init__(
        self,
        parent: pipe.BasePipe,
        file: Union[pathlib.Path, fl.File],
        name: str,
    ):
        super().__init__(parent, name)
        self._file = file if isinstance(file, fl.File) else fl.File(file)
        self._writer: writer.Writer
        self._write: WritePointer

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
