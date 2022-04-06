"""Base pipe class."""
from __future__ import annotations

import logging
import pathlib
import threading
from typing import Callable, Optional

from wingline.plumbing import queue
from wingline.types import SENTINEL, PayloadIterator, PipeProcess

logger = logging.getLogger(__name__)


class PipeThread(threading.Thread):
    def __init__(
        self,
        input_queue: queue.Queue,
        output_queues: set[queue.Queue],
        process: PipeProcess,
        setup: Callable[[], None],
        teardown: Callable[[], None],
        parent_name: str,
    ):
        self.input_queue = input_queue
        self.output_queues = output_queues
        self.setup = setup
        self.process = process
        self.teardown = teardown
        self.parent_name = parent_name
        super().__init__()
        self.name = f"<T {self.parent_name}>"

    def _iter_input(self) -> PayloadIterator:
        while True:
            payload = self.input_queue.get()
            if payload is not SENTINEL:
                yield payload
                self.input_queue.task_done()
            else:
                self.input_queue.task_done()
                break

    def run(self) -> None:
        """Run the thread."""

        self.setup()
        for output_payload in self.process(self._iter_input()):
            for output_queue in self.output_queues:
                output_queue.put(output_payload)
        for output_queue in self.output_queues:
            output_queue.put(SENTINEL)

        self.teardown()


class Pipe:
    """Base pipe."""

    emoji = "🠞"

    def __init__(self, parent: Pipe, name: str) -> None:
        self.name = name
        self.parent = parent
        self.input_queue = queue.Queue()
        self.children: set[Pipe] = set()
        self._output_queues: set[queue.Queue] = set()
        self.thread = PipeThread(
            input_queue=self.input_queue,
            output_queues=self._output_queues,
            setup=self.setup,
            process=self.process,
            teardown=self.teardown,
            parent_name=self.name,
        )
        self._started = False
        self.parent.add_child(self)
        self.hash: Optional[str] = self.parent.hash
        self.cache_dir: Optional[pathlib.Path] = parent.cache_dir

    def add_child(self, other: Pipe) -> None:
        """Register a child so it's input queue receives this pipe's output."""

        self.children.add(other)
        self._output_queues.add(other.input_queue)

    def start(self) -> None:
        """Start the process."""
        self.parent.start()
        if self._started:
            raise RuntimeError("Attempted to start a pipe for the second time.")
        self._started = True
        self.thread.start()

    def join(self) -> None:
        """Wait for the process to complete."""
        if not self._started:
            raise RuntimeError("Can't join a pipe that hasn't started.")
        self.parent.join()
        self.thread.join()

    @property
    def started(self) -> bool:
        """Read-only property. True if the pipe has started."""

        return self._started

    def process(self, payloads: PayloadIterator) -> PayloadIterator:
        """Business logic between input and output."""

        for payload in payloads:
            yield payload

    def setup(self) -> None:
        pass

    def teardown(self) -> None:
        pass

    def __str__(self) -> str:
        return f"{self.emoji} {self.name}"

    def __repr__(self) -> str:
        return f"<{str(self)}-{id(self)}>"
