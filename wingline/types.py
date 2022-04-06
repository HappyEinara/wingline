"""Typing."""

from __future__ import annotations

from contextlib import _GeneratorContextManager
from typing import Any, Callable, Iterable, Iterator

SENTINEL = {"SENTINEL": "TO TERMINATE STREAM"}

Payload = dict[str, Any]
PayloadIterable = Iterable[Payload]
PayloadIterator = Iterator[Payload]
PipeProcess = Callable[[PayloadIterator], PayloadIterator]
PlumbingContext = Callable[..., _GeneratorContextManager[Any]]
OpenPlumbingContext = _GeneratorContextManager[Any]
