"""Typing."""

from __future__ import annotations

from typing import Any, Callable, Iterable, Iterator

SENTINEL = {"SENTINEL": "TO TERMINATE STREAM"}

Payload = dict[str, Any]
PayloadIterable = Iterable[Payload]
PayloadIterator = Iterator[Payload]
PipeProcess = Callable[[PayloadIterator], PayloadIterator]
