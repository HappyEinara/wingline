"""Detect filetype."""


import pathlib
from typing import Optional

from wingline.files import containers, formats


def detect_filetypes(
    path: pathlib.Path,
) -> tuple[type[formats.Format], Optional[type[containers.Container]]]:
    """Detect filetypes based on path."""

    container = None
    suffixes = path.suffixes
    suffix = suffixes.pop()
    for c in containers.CONTAINERS:
        if path.suffix in c.suffixes:
            container = c
            suffix = suffixes.pop()
            break
    for f in formats.FORMATS:
        if suffix in f.suffixes:
            return f, container
    raise ValueError(f"Couldn't determine filetypes of {path}")
