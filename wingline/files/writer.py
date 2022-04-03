"""High-level writer class."""

import contextlib
import pathlib
from typing import Callable, Generator, Iterator, Optional

from wingline.files import containers, filetype, formats
from wingline.types import Payload


class Writer:
    """An abstract writer that can write data to any format in any container."""

    def __init__(
        self,
        path: pathlib.Path,
        format: Optional[type[formats.Format]] = None,
        container: Optional[type[containers.Container]] = None,
    ):

        self.path = path
        if container and format:
            container_type = container
            format_type = format
        else:
            inferred_container, inferred_format = filetype.get_filetypes_by_path(path)

            format_type = format if format is not None else inferred_format
            container_type = container if container is not None else inferred_container
        self.container = container_type(path)
        self.format_type = format_type

    @contextlib.contextmanager
    def _get_write_handle(self):
        with self.container.write_handle() as _handle:
            yield _handle

    @contextlib.contextmanager
    def _get_writer(self) -> Generator[Callable[[Payload], None], None, None]:
        with self._get_write_handle() as _handle:
            writer = self.format_type(_handle).writer
            yield writer

    def __enter__(self):
        """Context manager entrypoint."""
        self.writer = self._get_writer()
        self.write = self.writer.__enter__()
        return self.write

    def __exit__(self, exception_type, exception, traceback):
        """Context manager exitpoint."""
        self.writer.__exit__(exception_type, exception, traceback)
