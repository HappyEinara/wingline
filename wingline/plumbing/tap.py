"""Tap class."""
from __future__ import annotations

import collections.abc
import logging
import pathlib
from typing import Optional

from wingline import hasher
from wingline.plumbing import pipe
from wingline.types import SENTINEL, PayloadIterable

logger = logging.getLogger(__name__)


class Tap(pipe.BasePipe):
    """A Pipe subclass with no parent, generating output from some other source."""

    emoji = "﹛﹜↦"

    def __init__(
        self,
        source: PayloadIterable,
        name: str,
        cache_dir: Optional[pathlib.Path] = None,
        parent: Optional[pipe.BasePipe] = None,
    ) -> None:
        relationships = pipe.PipeRelationships(
            pipe=self,
            parent=parent,
            children=set(),
        )
        super().__init__(name, relationships, cache_dir=cache_dir)
        self.source = source
        self.hash = (
            hasher.hash_sequence(source)
            if isinstance(source, collections.abc.Sequence)
            else None
        )
        self.is_real_tap = True

    def start(self) -> None:
        """Start the process."""

        if self._started:
            raise RuntimeError("Attempted to start a pipe for the second time.")
        self._started = True
        self.thread.start()
        logger.debug("(Tap) %s: started thread.", self)
        for payload in self.source:
            if self.abort_event.is_set():
                logger.debug("(Tap) %s: received abort event.", self)
                break
            self.queues.input.put(payload)
        self.queues.input.put(SENTINEL)

    def join(self) -> None:
        """Wait for the process to complete."""
        if not self._started:
            raise RuntimeError("Can't join a pipe that hasn't started.")
        self.thread.join()
