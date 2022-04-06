"""Pipe iterator class."""
from __future__ import annotations

import logging

from wingline.plumbing import queue, sink
from wingline.types import SENTINEL, PayloadIterator

logger = logging.getLogger(__name__)


class IteratorSink(sink.Sink):
    """A pipe that acts as an iterator over its input queue."""

    def __iter__(self) -> PayloadIterator:
        """Iterate over the pipe output."""
        self._iter_queue = queue.Queue()
        self._output_queues.add(self._iter_queue)
        self.start()
        while True:
            payload = self._iter_queue.get()
            if payload is not SENTINEL:
                yield payload
            self._iter_queue.task_done()
            if payload is SENTINEL:
                break
        self.join()
