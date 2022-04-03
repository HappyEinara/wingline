"""Typing."""

from __future__ import annotations

import pathlib
from contextlib import _GeneratorContextManager
from typing import TYPE_CHECKING, Any, Callable, Iterable, Iterator, Union

SENTINEL = {"SENTINEL": "TO TERMINATE STREAM"}

if TYPE_CHECKING:
    from wingline import pipeline
    from wingline.files import file

Payload = dict[str, Any]
PayloadIterable = Iterable[Payload]
PayloadIterator = Iterator[Payload]
PayloadIterable = Iterable[Payload]
PayloadIterator = Iterator[Payload]
PipeProcess = Callable[[Payload], PayloadIterator]
PlumbingContext = Callable[..., _GeneratorContextManager[Any]]
OpenPlumbingContext = _GeneratorContextManager[Any]
