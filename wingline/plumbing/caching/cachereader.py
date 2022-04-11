"""A cache-reading pipe."""
# pylint: disable=duplicate-code

import logging
import pathlib
from typing import Optional

from wingline.cache import intermediate
from wingline.files import file
from wingline.plumbing import base
from wingline.types import SENTINEL

logger = logging.getLogger(__name__)


class CacheReader(base.Plumbing):
    """A tap that reads from the cache of a previous run."""

    emoji = "☁️⏩"

    def __init__(
        self,
        parent: base.Plumbing,
        cache_path: pathlib.Path,
        name: Optional[str] = None,
    ):
        name = name if name is not None else f"cachereader-{cache_path.name}"
        cache_file = file.File(cache_path, intermediate.filetype)
        super().__init__(parent, name)
        parent.queues.output.remove(self.queues.input)
        self.source = cache_file
        self.hash = parent.hash

    def start(self) -> None:
        """Start the process."""

        if self._started:
            raise RuntimeError(
                "Attempted to start a pipe for the second time."
            )  # pragma: no cover
        self._started = True
        self.thread.start()
        logger.debug("(CacheReader) %s: started thread.", self)
        for payload in self.source:
            if self.abort_event.is_set():  # pragma: no cover
                logger.debug("(Tap) %s: received abort event.", self)
                break
            self.queues.input.put(payload)
        self.queues.input.put(SENTINEL)

    def join(self) -> None:
        """Wait for the process to complete."""
        logger.debug("%s: Received join request.", self)
        if not self._started:
            raise RuntimeError(
                "Can't join a pipe that hasn't started."
            )  # pragma: no cover
        logger.debug("%s: Attempting to join thread.", self)
        self.thread.join()
        logger.debug("%s: Joined thread.", self)

    def __str__(self) -> str:
        return f"{self.emoji} CacheReader[{self.hash}]"
