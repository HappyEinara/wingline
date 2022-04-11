"""Filetype encapsulation."""

# pylint: disable=redefined-builtin

import dataclasses
import pathlib
from typing import Optional, Type

from wingline.files import containers, formats

DefaultContainer = containers.Bare


@dataclasses.dataclass
class Filetype:
    """Container for file format/container spec."""

    def __init__(
        self, format: formats.Format, container: Optional[containers.Container] = None
    ):
        self.format = format
        self.container = container if container is not None else DefaultContainer()


def detect_filetype(
    path: pathlib.Path,
) -> Filetype:
    """Detect filetypes based on path."""

    container: Type[containers.Container] = DefaultContainer
    suffixes = path.suffixes
    suffix: Optional[str]
    suffix = suffixes.pop()
    for next_container in containers.CONTAINERS:
        if suffix in next_container.suffixes:
            container = next_container
            suffix = suffixes.pop()
            break
    for next_format in formats.FORMATS:
        if suffix in next_format.suffixes:
            return Filetype(next_format(), container())
    raise ValueError(f"Couldn't determine filetypes of {path}")
