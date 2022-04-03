"""A wrapper around Queue to give it a name and better repr."""

import queue
from typing import Optional

from wingline.types import Payload


class Queue(queue.Queue[Payload]):
    def __init__(self, *args, name: Optional[str] = None, **kwargs):
        self.name = name
        super().__init__(*args, **kwargs)

    def __str__(self) -> str:
        if self.name:
            return f"<Q {self.name}>"
        else:
            return super().__str__()

    def __repr__(self) -> str:
        if self.name:
            return str(self)
        else:
            return super().__repr__()
