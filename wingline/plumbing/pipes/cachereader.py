"""A cache-reading pipe."""
# pylint: disable=duplicate-code

import pathlib
from typing import Optional

from wingline.cache import intermediate
from wingline.files import file
from wingline.plumbing import pipe
from wingline.plumbing.taps import filetap


class CacheReader(filetap.FileTap):
    """A tap that reads from the cache of a previous run."""

    emoji = "💾⏩"

    def __init__(
        self,
        parent: pipe.BasePipe,
        cache_path: pathlib.Path,
        name: Optional[str] = None,
    ):
        name = name if name is not None else f"cachereader-{cache_path.name}"
        cache_file = file.File(cache_path, intermediate.filetype)
        super().__init__(cache_file, name, parent=parent)
        self.hash = parent.hash
