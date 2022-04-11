"""FileTap class."""
from __future__ import annotations

import pathlib
from typing import Optional

from wingline.files import file
from wingline.plumbing import pipe, tap


class FileTap(tap.Tap):
    """A tap that uses a wingline File as a source."""

    emoji = "📄↦"

    def __init__(
        self,
        source_file: file.File,
        name: str,
        cache_dir: Optional[pathlib.Path] = None,
        parent: Optional[pipe.BasePipe] = None,
    ) -> None:
        super().__init__(source_file, name, cache_dir=cache_dir, parent=parent)
        self.hash = source_file.content_hash
        self.description = f"File[{source_file.path.name}]"
