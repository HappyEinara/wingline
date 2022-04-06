"""Reader class."""

import pathlib
from types import TracebackType
from typing import Optional

from wingline.files import containers, formats
from wingline.types import PayloadIterator


class Reader:
    """An abstract reader that yields dicts from any source."""

    def __init__(
        self,
        path: pathlib.Path,
        format: formats.Format,
        container: containers.Container,
    ):
        self.path = path
        self.container = container
        self.format = format

    def __enter__(self) -> PayloadIterator:
        """Context manager entrypoint."""

        self._container_manager = self.container.read_manager(self.path)
        self._container_handle = self._container_manager.__enter__()
        self._format_manager = self.format.read_manager(self._container_handle)
        reader_func = self._format_manager.__enter__()
        return reader_func()

    def __exit__(
        self,
        exception_type: Optional[type[BaseException]],
        exception: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        """Context manager exitpoint."""
        self._format_manager.__exit__(exception_type, exception, traceback)
        self._container_manager.__exit__(exception_type, exception, traceback)
