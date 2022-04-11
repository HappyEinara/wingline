"""Writer class."""
# pylint: disable=duplicate-code


import logging
import pathlib
import tempfile
from types import TracebackType
from typing import BinaryIO, Optional, Type

from wingline.files import filetype as ft
from wingline.types import ContainerWriteManager, FormatWriteManager, WritePointer

logger = logging.getLogger(__name__)


class Writer:  # pylint: disable=R0902
    """An abstract writer that can write data to any format in any container."""

    def __init__(self, path: pathlib.Path, filetype: ft.Filetype):
        self.path = path
        self.filetype = filetype
        self._container_manager: ContainerWriteManager
        self._container_handle: BinaryIO
        self._format_manager: FormatWriteManager
        self._temp_dir: tempfile.TemporaryDirectory[str]
        self._temp_file: pathlib.Path
        self.success = False

    def __enter__(self) -> WritePointer:
        """Context manager entrypoint."""
        self._temp_dir = tempfile.TemporaryDirectory()
        self._temp_file = pathlib.Path(self._temp_dir.__enter__()) / self.path.name
        self._container_manager = self.filetype.container.write_manager(self._temp_file)
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
        if self.success:
            logger.debug(
                "%s: Pipeline was successful so far, so writing to %s", self, self.path
            )
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self._temp_file.rename(self.path)
        else:
            logger.debug(
                "%s: Pipeline failed, so not persisting output to %s", self, self.path
            )
        self._temp_dir.__exit__(exception_type, exception, traceback)
