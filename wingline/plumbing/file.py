"""File tap class."""

from __future__ import annotations

import pathlib

from wingline.files import file
from wingline.plumbing import tap


class File(tap.Tap):

    emoji = "📄"

    def __init__(self, path: pathlib.Path):
        self._name = path.name
        if not path.exists():
            raise ValueError("%s doesn't exist", path)
        self.file = file.File(path)
        super().__init__(self.file, (str(self.file)))
        self._hash = self.file.content_hash


class IntermediateCacheFile(File):
    pass
