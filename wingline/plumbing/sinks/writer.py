"""File writer sink."""


import logging
import pathlib
from typing import Union

from wingline.files import file as fl
from wingline.files import writer
from wingline.plumbing import base
from wingline.types import PayloadIterable, PayloadIterator, WritePointer

logger = logging.getLogger(__name__)


class WriterSink(base.Sink):
    """A sink that writes to a file."""

    emoji = "💾"

    def __init__(
        self,
        parent: base.Plumbing,
        file: Union[pathlib.Path, fl.File],
        name: str,
    ):
        super().__init__(parent, name)
        self._file = file if isinstance(file, fl.File) else fl.File(file)
        self._writer: writer.Writer
        self._write: WritePointer
        logger.debug("Initialized writersink.")
        self.description = f"WriteFile[{self._file.path.name}]"

    def setup(self) -> None:
        self._writer = self._file.writer()
        self._write = self._writer.__enter__()
        logger.debug("Got writer.")

    def teardown(self, success: bool = False) -> None:
        self._writer.success = success
        self._writer.__exit__(None, None, None)

    def process(self, payloads: PayloadIterable) -> PayloadIterator:
        """Write the payload."""
        for payload in payloads:
            logger.debug("Writing payload.")
            self._write(payload)
            yield payload
