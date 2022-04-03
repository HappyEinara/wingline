"""A caching pipe."""
import pathlib
from typing import Optional

from wingline.cache import intermediate
from wingline.files import file
from wingline.plumbing import pipe
from wingline.types import Payload


class CacheWriter(pipe.Pipe):
    emoji = "⏩💾"

    def __init__(
        self, parent: pipe.Pipe, cache_path: pathlib.Path, name: Optional[str] = None
    ):
        name = name if name is not None else f"cache-{cache_path.name}"
        super().__init__(parent, name)
        self._file = file.File(cache_path, intermediate.FORMAT, intermediate.CONTAINER)

    def _open_writer(self):
        """Open the writer."""
        self._writer = self._file.writer
        self._write = self._writer.__enter__()

    def _close_writer(self, *_):
        """Close the writer."""
        self._writer.__exit__(None, None, None)

    def process(self, payload: Payload):
        """Write the payload."""
        self._write(payload)
        yield payload

    def setup(self):
        """Set up the pipe."""
        self._open_writer()

    def teardown(self):
        """Finalize the pipe."""
        self._close_writer()
