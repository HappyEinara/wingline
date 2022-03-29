"""Base plumbing ABC."""
from __future__ import annotations

import abc
import logging
import threading
from typing import TYPE_CHECKING, Optional, Union

logger = logging.getLogger(__name__)
if TYPE_CHECKING:
    from wingline.plumbing import pipe, sink


class BasePlumbing(abc.ABC, threading.Thread):
    """Abstract base class for plumbing elements."""

    _name: str
    hash: Optional[str] = None
    parent: Optional[BasePlumbing] = None
    is_disabled: bool = False
    is_cached: bool = False
    will_cache: bool = False
    emoji: str = "⇢"

    def __init__(self) -> None:

        # Initialize thread and basic ID.
        super().__init__()
        self.name = self._name

        # Initialize subscribers (downstream plumbing)
        self.subscribers: list[Union[pipe.Pipe, sink.Sink]] = []

    def join(self):
        self._debug("Joining. Subscribers: %s", self.subscribers)
        for subscriber in self.subscribers:
            self._debug("   Waiting to join: %s", subscriber)
            subscriber.join()
        self._debug("      All subscribers joined.")

    def subscribe(self, other: Union[pipe.Pipe, sink.Sink]) -> None:
        self.subscribers.append(other)

    def _debug(self, message: str, *args) -> None:
        logger.debug("%s|" + message, self, *args)

    def __str__(self) -> str:
        if self.is_disabled:
            emoji = "🗙"
        elif self.is_cached:
            emoji = "💾"
        else:
            emoji = self.emoji

        return f"<{emoji} {repr(self)}>"

    def __repr__(self) -> str:
        return f"{self.__class__.__name__} {self._name}"
