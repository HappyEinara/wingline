"""A wrapper around Queue to give it a name and better repr."""

from __future__ import annotations

import queue
from typing import Optional


# Queue[Payload] when py3.8 is EOL.
class Queue(queue.Queue):  # type: ignore
    """A queue with an optional name."""

    def __init__(self, name: Optional[str] = None, maxsize: int = 0):
        self.name = name
        super().__init__(maxsize=maxsize)

    def __str__(self) -> str:
        return f"<Q {self.name}>"
