"""Tap class."""

from __future__ import annotations

import collections.abc

from wingline import hasher
from wingline.plumbing import queue
from wingline.plumbing.pipe import Pipe, PipeThread
from wingline.types import SENTINEL, PayloadIterable


class Tap(Pipe):
    """A Pipe subclass that has no parent and generates output from some other source."""

    emoji = "﹛﹜↦"

    def __init__(self, source: PayloadIterable, name: str) -> None:
        self.name = name
        self.source = source
        self.children: set[Pipe] = set()
        self.input_queue: queue.Queue = queue.Queue()
        self._output_queues: set[queue.Queue] = set()
        self._started = False
        self.hash = (
            hasher.hash_sequence(source)
            if isinstance(source, collections.abc.Sequence)
            else None
        )

    def start(self) -> None:
        """Start the process."""
        if self._started:
            return
        self.thread = PipeThread(
            input_queue=self.input_queue,
            output_queues=self._output_queues,
            setup=self.setup,
            process=self.process,
            teardown=self.teardown,
            parent_name=self.name,
        )
        self.thread.start()
        self._started = True
        for payload in self.source:
            self.input_queue.put(payload)
        self.input_queue.put(SENTINEL)

    def join(self):
        """Wait for the process to complete."""
        if not self._started:
            raise RuntimeError("Can't join a pipe that hasn't started.")
        self.thread.join()
