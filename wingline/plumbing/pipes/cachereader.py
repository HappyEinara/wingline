"""A cache-reading pipe."""
import pathlib
from typing import Optional

from wingline.cache import intermediate
from wingline.files import file
from wingline.plumbing import pipe, tap
from wingline.plumbing.taps import filetap


class CacheReader(filetap.FileTap):
    emoji = "💾⏩"

    def __init__(
        self, parent: pipe.Pipe, cache_path: pathlib.Path, name: Optional[str] = None
    ):
        name = name if name is not None else f"cachereader-{cache_path.name}"
        cache_file = file.File(cache_path, intermediate.FORMAT, intermediate.CONTAINER)
        super(tap.Tap, self).__init__(parent, name)
        super().__init__(cache_file, name)
        self.hash = parent.hash

    def start(self) -> None:
        super().start()
