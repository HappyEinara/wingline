"""High-level writer class."""

import pathlib
from types import TracebackType
from typing import Callable, Optional

from wingline.files import containers, formats
from wingline.types import Payload


class Writer:
    """An abstract writer that can write data to any format in any container."""

    def __init__(
        self,
        path: pathlib.Path,
        format: formats.Format,
        container: containers.Container,
    ):
        self.path = path
        self.container = container
        self.format = format

    def __enter__(self) -> Callable[[Payload], None]:
        """Context manager entrypoint."""

        self._container_manager = self.container.write_manager(self.path)
        self._container_handle = self._container_manager.__enter__()
        self._format_manager = self.format.write_manager(self._container_handle)
        return self._format_manager.__enter__()

    def __exit__(
        self,
        exception_type: Optional[type[BaseException]],
        exception: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        """Context manager exitpoint."""
        self._format_manager.__exit__(exception_type, exception, traceback)
        self._container_manager.__exit__(exception_type, exception, traceback)
