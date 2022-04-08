"""Base pipe class."""
from __future__ import annotations

import logging
import pathlib
import threading
from dataclasses import dataclass
from typing import Callable, Optional, Set

from wingline.plumbing import queue
from wingline.types import SENTINEL, PayloadIterator, PipeProcess

logger = logging.getLogger(__name__)


@dataclass
class PipeQueues:
    """Queues for a pipe."""

    input: queue.Queue
    output: Set[queue.Queue]


@dataclass
class PipeCallbacks:
    """Callbacks for a pipe."""

    process: PipeProcess
    setup: Callable[[], None]
    teardown: Callable[[], None]


class PipeThread(threading.Thread):
    """The thread underlying a pipe process."""

    def __init__(
        self,
        queues: PipeQueues,
        callbacks: PipeCallbacks,
        parent_name: str,
    ):
        self.queues = queues
        self.callbacks = callbacks
        self.parent_name = parent_name
        super().__init__()
        self.name = f"<T {self.parent_name}>"

    def _iter_input(self) -> PayloadIterator:
        while True:
            payload = self.queues.input.get()
            if payload is not SENTINEL:
                yield payload
                self.queues.input.task_done()
            else:
                self.queues.input.task_done()
                break

    def run(self) -> None:
        """Run the thread."""

        # https://github.com/python/mypy/issues/6910

        self.callbacks.setup()  # type: ignore

        for output_payload in self.callbacks.process(  # type: ignore
            self._iter_input()
        ):
            for output_queue in self.queues.output:
                output_queue.put(output_payload)
        for output_queue in self.queues.output:
            output_queue.put(SENTINEL)

        self.callbacks.teardown()  # type: ignore


class PipeRelationships:
    """Encapsulation of a pipe's parent and children."""

    def __init__(
        self,
        pipe: BasePipe,
        parent: Optional[BasePipe] = None,
        children: Optional[Set[BasePipe]] = None,
    ):
        self.pipe = pipe
        self.parent = parent
        self.children = children if children is not None else set()

    def register_with_parent(self) -> None:
        """Register the pipe with its parent."""
        if self.parent is None:
            return
        self.parent.add_child(self.pipe)

    def add_child(self, other: BasePipe) -> None:
        """Add a child pipe."""
        self.children.add(other)


class BasePipe:
    """Base pipe."""

    emoji = "🠞"
    hash: Optional[str]

    def __init__(
        self,
        name: str,
        relationships: PipeRelationships,
        cache_dir: Optional[pathlib.Path] = None,
    ) -> None:
        self.name = name
        self.queues = PipeQueues(
            input=queue.Queue(),
            output=set(),
        )
        self.relationships = relationships
        self.relationships.register_with_parent()
        self._thread: Optional[PipeThread] = None
        self._started = False
        self.cache_dir: Optional[pathlib.Path] = (
            cache_dir
            if cache_dir is not None
            else (
                self.relationships.parent.cache_dir
                if self.relationships.parent
                else None
            )
        )

    def add_child(self, other: BasePipe) -> None:
        """Register a child so it's input queue receives this pipe's output."""

        self.relationships.add_child(other)
        self.queues.output.add(other.queues.input)

    @property
    def thread(self) -> PipeThread:
        """The thread in which this pipe will run."""
        if self._thread:
            return self._thread

        self._thread = PipeThread(
            queues=self.queues,
            callbacks=PipeCallbacks(
                process=self.process,
                setup=self.setup,
                teardown=self.teardown,
            ),
            parent_name=self.name,
        )
        return self._thread

    def start(self) -> None:
        """Start the process."""
        if self.relationships.parent:
            self.relationships.parent.start()
        self._started = True
        self.thread.start()

    def join(self) -> None:
        """Wait for the process to complete."""
        if not self._started:
            raise RuntimeError("Can't join a pipe that hasn't started.")
        if self.relationships.parent:
            self.relationships.parent.join()
        self.thread.join()

    @property
    def started(self) -> bool:
        """Read-only property. True if the pipe has started."""

        return self._started

    def process(self, payloads: PayloadIterator) -> PayloadIterator:
        """Business logic between input and output."""

        if not self._started:  # pragma: no cover
            raise RuntimeError("Cant run process in pipe that hasn't started.")
        for payload in payloads:
            yield payload

    def setup(self) -> None:
        """Set up before processing."""

    def teardown(self) -> None:
        """Tidy up after processing."""

    def __str__(self) -> str:
        return f"{self.emoji} {self.name}"

    def __repr__(self) -> str:
        return f"<{str(self)}-{id(self)}>"


class Pipe(BasePipe):
    """Pipe."""

    def __init__(self, parent: BasePipe, name: str) -> None:

        relationships = PipeRelationships(
            pipe=self,
            parent=parent,
            children=set(),
        )
        super().__init__(name, relationships)
        self.hash: Optional[str] = (
            self.relationships.parent.hash if self.relationships.parent else None
        )
