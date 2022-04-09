"""Typing."""

from __future__ import annotations

from typing import (
    Any,
    BinaryIO,
    Callable,
    ContextManager,
    Dict,
    Iterable,
    Iterator,
    Optional,
)

SENTINEL = {"SENTINEL": "TO TERMINATE STREAM"}

Payload = Dict[str, Any]
PayloadIterable = Iterable[Payload]
PayloadIterator = Iterator[Payload]
PipeProcess = Callable[[PayloadIterator], PayloadIterator]
EachProcess = Callable[[Payload], Optional[Payload]]
WritePointer = Callable[[Payload], None]
ReadPointer = Callable[..., PayloadIterator]
ContainerReadManager = ContextManager[BinaryIO]
ContainerWriteManager = ContextManager[BinaryIO]
FormatReadManager = ContextManager[ReadPointer]
FormatWriteManager = ContextManager[WritePointer]
