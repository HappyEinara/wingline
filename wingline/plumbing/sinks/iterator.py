"""Pipe iterator class."""
from __future__ import annotations

import logging

from wingline.plumbing import base, queue
from wingline.types import SENTINEL, PayloadIterator

logger = logging.getLogger(__name__)


class IteratorSink(base.Sink):
    """A pipe that acts as an iterator over its input queue."""

    def __init__(self, parent: base.Plumbing, name: str) -> None:
        super().__init__(parent, name)
        self._iter_queue = queue.Queue()
        self.queues.output.add(self._iter_queue)

    def __iter__(self) -> PayloadIterator:
        """Iterate over the pipe output."""
        self.start()
        while True:
            if self.abort_event.is_set():  # pragma: no cover
                logger.debug("%s: Received abort event.", self)
                break
            payload = self._iter_queue.get()
            if payload is not SENTINEL:
                yield payload
            self._iter_queue.task_done()
            if payload is SENTINEL:
                break
        self.join()
        if self.queues.input.qsize() or self._iter_queue.qsize():  # pragma: no cover
            raise RuntimeError("Iterator queues not empty after join!")
