"""Reader class."""

import contextlib
import pathlib
from typing import Any, Generator, Iterator, Optional

from wingline.files import containers, filetype, formats


class Reader:
    """An abstract reader that yields dicts from any source."""

    def __init__(
        self,
        path: pathlib.Path,
        format: Optional[type[formats.Format]] = None,
        container: Optional[type[containers.Container]] = None,
    ):
        self.path = path
        self.container = (
            container(self.path)
            if container is not None
            else filetype.get_container(self.path)
        )
        self.format_type = (
            format if format is not None else filetype.detect_format(self.container)
        )

    @contextlib.contextmanager
    def _get_handle(self):
        with self.container.handle() as _handle:
            yield _handle

    @contextlib.contextmanager
    def _get_iterator(self) -> Generator[Iterator[dict[str, Any]], None, None]:
        with self._get_handle() as _handle:
            iterator = self.format_type(_handle).reader
            yield iterator

    def __enter__(self):
        """Context manager entrypoint."""
        self._iterator = self._get_iterator()
        return self._iterator.__enter__()

    def __exit__(self, exception_type, exception, traceback):
        """Context manager exitpoint."""
        self._iterator.__exit__(exception_type, exception, traceback)
