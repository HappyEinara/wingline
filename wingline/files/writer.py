"""Writer class."""
# pylint: disable=duplicate-code


import pathlib
from types import TracebackType
from typing import BinaryIO, Optional, Type

from wingline.files import filetype as ft
from wingline.types import ContainerWriteManager, FormatWriteManager, WritePointer


class Writer:
    """An abstract writer that can write data to any format in any container."""

    def __init__(self, path: pathlib.Path, filetype: ft.Filetype):
        self.path = path
        self.filetype = filetype
        self._container_manager: ContainerWriteManager
        self._container_handle: BinaryIO
        self._format_manager: FormatWriteManager

    def __enter__(self) -> WritePointer:
        """Context manager entrypoint."""

        self._container_manager = self.filetype.container.write_manager(self.path)
        self._container_handle = self._container_manager.__enter__()
        self._format_manager = self.filetype.format.write_manager(
            self._container_handle
        )
        return self._format_manager.__enter__()

    def __exit__(
        self,
        exception_type: Optional[Type[BaseException]],
        exception: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        """Context manager exitpoint."""
        self._format_manager.__exit__(exception_type, exception, traceback)
        self._container_manager.__exit__(exception_type, exception, traceback)
