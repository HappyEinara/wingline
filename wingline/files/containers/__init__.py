"""Container formats."""

from typing import Set, Type

from wingline.files.containers._base import Container
from wingline.files.containers.bare import Bare
from wingline.files.containers.gzip import Gzip
from wingline.files.containers.zip import Zip

CONTAINERS: Set[Type[Container]] = {Bare, Gzip, Zip}

__all__ = [
    "Bare",
    "CONTAINERS",
    "Container",
    "Gzip",
    "Zip",
]
