"""High-level file writer class"""

import pathlib
from typing import Optional

from wingline.files import containers, filetype, formats


class Writer:
    def __init__(
        self,
        path: pathlib.Path,
        format: formats.Format,
        container: Optional[containers.Container] = None,
    ):
        self.path = path
        self.writer = filetype.get_writer(self.path, format, container)
