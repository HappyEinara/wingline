"""Tap class."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Optional

import msgpack

from wingline import hasher
from wingline.plumbing import base, hooks, pipe
from wingline.types import SENTINEL, PayloadIterable, PayloadIterator

if TYPE_CHECKING:
    from wingline.plumbing import PayloadIteratorHook

logger = logging.getLogger(__name__)


class Tap(base.BasePlumbing):
    """Base class for sources with no parent."""

    emoji = "↦"

    def __init__(self, source: PayloadIterable, name: str):
        self._input_iterator = iter(source)
        super().__init__()
        self.name = name
        self._hash = None
        self.output_hooks: list[PayloadIteratorHook] = []

    def run(self) -> None:
        self.output_hooks.append(hooks.log_payloads("output"))
        self._debug("Starting subscribers %s", self.subscribers)
        [subscriber.start() for subscriber in self.subscribers]
        self._debug("Subscribers started")
        for payload in self._iter_input():
            # Output hooks can again read the iterable
            # of processed output but must not modify it.
            iter_payload = iter((payload,))
            for hook in self.output_hooks:
                iter_payload = hook(self, iter_payload)
            for payload in iter_payload:
                self.propagate(payload)

    def _iter_input(self) -> PayloadIterator:
        # Progressively hash the content
        # if it has not been provided by
        # a subclass (e.g. the file hash
        # in the case of a File tap.
        content_hash = hasher.hasher() if self._hash is None else None

        for item in self._input_iterator:
            if content_hash is not None:
                content_hash.update(msgpack.packb(item))
            yield item
        if content_hash is not None:
            self._hash = content_hash.hexdigest()
        logger.debug("Content iterator exhausted: passing SENTINEL.")
        yield SENTINEL

    def __iter__(self) -> PayloadIterator:
        for payload in self._iter_input():
            if payload is not SENTINEL:
                yield payload

    @property
    def hash(self):
        return self._hash

    def pipe(self, operation, name: Optional[str] = None) -> pipe.Pipe:
        return pipe.Pipe(self, operation, name=name)

    def propagate(self, item) -> None:
        for subscriber in self.subscribers:
            self._debug("Propagating %.20s to %s", item, subscriber)
            subscriber.input_queue.put(item)
