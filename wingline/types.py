"""Typing."""

from __future__ import annotations

import pathlib
from contextlib import _GeneratorContextManager
from typing import Any, Callable, Iterable, Iterator, Union

SENTINEL = {"SENTINEL": "TO TERMINATE STREAM"}

Payload = dict[str, Any]
PayloadIterable = Iterable[Payload]
PayloadIterator = Iterator[Payload]
PayloadIterable = Iterable[Payload]
PayloadIterator = Iterator[Payload]
PipeProcess = Callable[[Payload], PayloadIterator]
PlumbingContext = Callable[..., _GeneratorContextManager[Any]]
OpenPlumbingContext = _GeneratorContextManager[Any]
Source = Union[pathlib.Path, PayloadIterable]
