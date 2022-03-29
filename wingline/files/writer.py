"""High-level writer class."""

import contextlib
import pathlib
from typing import Callable, Generator, Iterator, Optional

from wingline.files import containers, formats
from wingline.types import Payload


class Writer:
    """An abstract writer that can write data to any format in any container."""

    def __init__(
        self,
        path: pathlib.Path,
        format: type[formats.Format],
        container: Optional[type[containers.Container]] = None,
    ):
        self.path = path
        self.container = (
            container(self.path)
            if container is not None
            else containers.Container(path)
        )
        self.format = format

    @contextlib.contextmanager
    def _get_write_handle(self):
        with self.container.write_handle() as _handle:
            yield _handle

    @contextlib.contextmanager
    def _get_writer(self) -> Generator[Callable[[Payload], None], None, None]:
        with self._get_write_handle() as _handle:
            writer = self.format(_handle).writer
            yield writer

    def __enter__(self):
        """Context manager entrypoint."""
        self.writer = self._get_writer()
        self.write = self.writer.__enter__()
        return self.write

    def __exit__(self, exception_type, exception, traceback):
        """Context manager exitpoint."""
        self.writer.__exit__(exception_type, exception, traceback)
