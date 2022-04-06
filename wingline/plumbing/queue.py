"""A wrapper around Queue to give it a name and better repr."""

import queue
from typing import Any, Optional

from wingline.types import Payload


class Queue(queue.Queue[Payload]):
    def __init__(self, *args: Any, name: Optional[str] = None, **kwargs: Any):
        self.name = name
        super().__init__(*args, **kwargs)
