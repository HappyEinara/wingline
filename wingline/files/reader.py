"""Reader class."""
# pylint: disable=duplicate-code

import pathlib
from types import TracebackType
from typing import BinaryIO, Optional, Type

from wingline.files import filetype as ft
from wingline.types import ContainerReadManager, FormatReadManager, PayloadIterator


class Reader:
    """An abstract reader that yields dicts from any source."""

    def __init__(self, path: pathlib.Path, filetype: ft.Filetype):
        self.path = path
        self.filetype = filetype
        self._container_manager: ContainerReadManager
        self._container_handle: BinaryIO
        self._format_manager: FormatReadManager

    def __enter__(self) -> PayloadIterator:
        """Context manager entrypoint."""

        self._container_manager = self.filetype.container.read_manager(self.path)
        self._container_handle = self._container_manager.__enter__()
        self._format_manager = self.filetype.format.read_manager(self._container_handle)
        reader_func = self._format_manager.__enter__()
        return reader_func()

    def __exit__(
        self,
        exception_type: Optional[Type[BaseException]],
        exception: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        """Context manager exitpoint."""
        self._format_manager.__exit__(exception_type, exception, traceback)
        self._container_manager.__exit__(exception_type, exception, traceback)
