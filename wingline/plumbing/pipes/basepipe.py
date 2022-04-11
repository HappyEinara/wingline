"""A process pipe."""

from typing import Optional

from wingline import hasher
from wingline.plumbing import base
from wingline.types import AllProcess, PayloadIterator


class BasePipe(base.Plumbing):
    """A pipe that carries out a process on its input"""

    emoji = "⚙️"

    def __init__(
        self, parent: base.Plumbing, process: AllProcess, name: Optional[str] = None
    ):
        self._process = process
        name = name if name is not None else self._process.__name__
        super().__init__(parent, name)
        process_hash = hasher.hash_callable(self._process)
        if parent.hash is None:
            raise RuntimeError("Couldn't get hash of parent.")
        self.hash = hasher.hash_string(parent.hash + process_hash)
        self.description = f"Process[{self.name}]"

    def process(self, payloads: PayloadIterator) -> PayloadIterator:
        """Do the processing."""

        for result in self._process(payloads):
            yield result
