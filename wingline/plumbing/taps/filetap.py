"""FileTap class."""
from __future__ import annotations

from wingline.files import file
from wingline.plumbing import tap


class FileTap(tap.Tap):
    """A tap that uses a wingline File as a source."""

    def __init__(self, source_file: file.File, name: str) -> None:
        super().__init__(source_file, name)
        self.hash = source_file.content_hash
