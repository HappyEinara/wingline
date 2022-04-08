"""A wrapper around Queue to give it a name and better repr."""

from __future__ import annotations

import queue
from typing import Any, Optional


# Queue[Payload] when py3.8 is EOL.
class Queue(queue.Queue):  # type: ignore
    """A queue with an optional name."""

    def __init__(self, *args: Any, name: Optional[str] = None, **kwargs: Any):
        self.name = name
        super().__init__(*args, **kwargs)
