"""Base plumbing classes."""
from __future__ import annotations

import collections.abc
import logging
import pathlib
import threading
from dataclasses import dataclass
from queue import Empty
from typing import Callable, Optional, Set

from wingline import hasher
from wingline.plumbing import queue
from wingline.types import SENTINEL, AllProcess, PayloadIterable, PayloadIterator

logger = logging.getLogger(__name__)


@dataclass
class PipeQueues:
    """Queues for a pipe."""

    input: queue.Queue
    output: Set[queue.Queue]


@dataclass
class PipeCallbacks:
    """Callbacks for a pipe."""

    process: AllProcess
    setup: Callable[[], None]
    teardown: Callable[[bool], None]


class PipeThread(threading.Thread):
    """The thread underlying a pipe process."""

    def __init__(
        self,
        queues: PipeQueues,
        callbacks: PipeCallbacks,
        parent_name: str,
        abort_event: threading.Event,
    ):
        self.queues = queues
        self.callbacks = callbacks
        self.parent_name = parent_name
        super().__init__()
        self.name = f"<T {self.parent_name}>"
        self.abort_event = abort_event

    def _iter_input(self) -> PayloadIterator:
        while True:
            if self.abort_event.is_set():
                logger.debug("%s: Received abort event.", self)
                break
            logger.debug("%s: waiting for payload", self)
            try:
                payload = self.queues.input.get(timeout=1)
            except Empty:
                continue
            logger.debug("%s: got payload", self)
            if payload is not SENTINEL:
                yield payload
                self.queues.input.task_done()
            else:
                self.queues.input.task_done()
                break

    def run(self) -> None:
        """Run the thread."""

        logger.debug("%s: starting", self)

        try:
            # https://github.com/python/mypy/issues/6910
            self.callbacks.setup()  # type: ignore

            for output_payload in self.callbacks.process(  # type: ignore
                self._iter_input()
            ):
                if self.abort_event.is_set():  # pragma: no cover
                    logger.debug("%s thread run got abort event at output.", self)
                    break
                for output_queue in self.queues.output:
                    logger.debug("%s: putting payload to %s", self, output_queue)
                    output_queue.put(output_payload)
            for output_queue in self.queues.output:
                output_queue.put(SENTINEL, timeout=1)

            self.callbacks.teardown(  # type: ignore
                success=(not self.abort_event.is_set())
            )

        except Exception as exc:  # pylint:disable=W0703
            logger.error("%s: Error in thread:", self)
            logger.exception(exc)
            self.abort_event.set()
            return


class PipeRelationships:
    """Encapsulation of a pipe's parent and children."""

    def __init__(
        self,
        pipe: Plumbing,
        parent: Optional[Plumbing] = None,
        children: Optional[Set[Plumbing]] = None,
    ):
        self.pipe = pipe
        self.parent = parent
        self.children = children if children is not None else set()

    def register_with_parent(self) -> None:
        """Register the pipe with its parent."""
        if self.parent is None:
            return
        self.parent.add_child(self.pipe)

    def add_child(self, other: Plumbing) -> None:
        """Add a child pipe."""
        self.children.add(other)


class Plumbing:  # pylint: disable=too-many-instance-attributes
    """Base pipe."""

    emoji = "🠞"
    hash: Optional[str]
    description: Optional[str]

    def __init__(
        self,
        parent: Optional[Plumbing],
        name: str,
        cache_dir: Optional[pathlib.Path] = None,
    ) -> None:
        self.name = name
        self.queues = PipeQueues(
            input=queue.Queue(name=f"input-{self.name}"),
            output=set(),
        )
        self.relationships = PipeRelationships(self, parent=parent, children=set())

        self.hash: Optional[str] = (
            self.relationships.parent.hash if self.relationships.parent else None
        )
        self.description: Optional[str] = (
            self.relationships.parent.description if self.relationships.parent else None
        )

        self.relationships.register_with_parent()
        self.abort_event: threading.Event = (
            self.relationships.parent.abort_event
            if self.relationships.parent
            else threading.Event()
        )
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
        self.is_active: bool = True

    def add_child(self, other: Plumbing) -> None:
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
            abort_event=self.abort_event,
        )
        return self._thread

    def start(self) -> None:
        """Start the process."""
        if not self.is_active:  # pragma no cover
            raise RuntimeError(f"{self}: Inactive pipe was started.")
        logger.debug("%s: Starting...", self)
        if self._started:
            raise RuntimeError(f"{self}: Pipe was started for a second time.")
        self._started = True
        self.thread.start()
        logger.debug("%s: Started thread", self)

        # Sinks may have already been started by
        # the Graph; this is not an error condition
        # so we should not attempt to start it again.
        if self.relationships.parent and not (
            isinstance(self.relationships.parent, Sink)
            and self.relationships.parent.started
        ):
            logger.debug("%s: Starting parent %s...", self, self.relationships.parent)
            self.relationships.parent.start()
            logger.debug("%s: Started parent; pipe start complete.", self)

    def join(self) -> None:
        """Wait for the process to complete."""
        logger.debug("%s: Received join request.", self)
        if not self._started:
            raise RuntimeError("Can't join a pipe that hasn't started.")
        if self.relationships.parent:
            self.relationships.parent.join()
            logger.debug("%s: Joined parent: %s", self, self.relationships.parent)
        logger.debug("%s: Attempting to join thread.", self)
        self.thread.join()
        logger.debug("%s: Joined thread.", self)

    @property
    def started(self) -> bool:
        """Read-only property. True if the pipe has started."""

        return self._started

    def process(self, payloads: PayloadIterator) -> PayloadIterator:
        """Business logic between input and output."""

        if not self._started:  # pragma: no cover
            raise RuntimeError("Cant run process in pipe that hasn't started.")
        for payload in payloads:
            if self.abort_event.is_set():  # pragma: no cover
                break
            yield payload

    def setup(self) -> None:
        """Set up before processing."""

    def teardown(self, success: bool = False) -> None:  # pylint: disable=R0201
        """Tidy up after processing."""

    def __str__(self) -> str:
        return f"{self.emoji} {self.description}-{self.hash}"

    def __repr__(self) -> str:
        return f"<{str(self)}-{id(self)}>"


class Tap(Plumbing):
    """A Pipe subclass with no parent, generating output from some other source."""

    emoji = "﹛﹜↦"

    def __init__(
        self,
        source: PayloadIterable,
        name: str,
        cache_dir: Optional[pathlib.Path] = None,
    ) -> None:
        super().__init__(None, name=name, cache_dir=cache_dir)
        self.source = source
        if isinstance(source, collections.abc.Sequence):
            self.hash = hasher.hash_sequence(source)
        else:
            self.hash = None

    def start(self) -> None:
        """Start the process."""

        if self._started:  # pragma: no cover
            raise RuntimeError("Attempted to start a pipe for the second time.")
        self._started = True
        self.thread.start()
        logger.debug("(Tap) %s: started thread.", self)
        for payload in self.source:
            if self.abort_event.is_set():  # pragma: no cover
                logger.debug("(Tap) %s: received abort event.", self)
                break
            self.queues.input.put(payload)
        self.queues.input.put(SENTINEL)


class Sink(Plumbing):
    """A pipe that has an output and must always run.

    Note that a sink may still have downstream children.
    Subclassing this means that it will run even if it's the
    ancestor of a cached ProcessPipe
    """

    emoji = "⇥"
