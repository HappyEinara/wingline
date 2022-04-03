"""Core plumbing threads."""
import pathlib
from typing import Callable

from wingline.files import containers, formats
from wingline.plumbing import base
from wingline.plumbing.file import IntermediateCacheFile
from wingline.plumbing.pipe import Pipe
from wingline.plumbing.pipeline import Pipeline
from wingline.plumbing.writer import Writer
from wingline.types import PayloadIterator

PlumbingHook = Callable[[base.BasePlumbing], None]
PayloadIteratorHook = Callable[[base.BasePlumbing, PayloadIterator], PayloadIterator]


__all__ = [
    "Pipeline",
]
