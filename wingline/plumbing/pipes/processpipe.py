"""A process pipe."""

from typing import Optional

from wingline import hasher
from wingline.plumbing import pipe
from wingline.types import Payload, PayloadIterator, PipeProcess


class ProcessPipe(pipe.Pipe):
    """A pipe that carries out a process on its input"""

    def __init__(
        self, parent: pipe.Pipe, process: PipeProcess, name: Optional[str] = None
    ):
        self._process = process
        name = name if name is not None else self._process.__name__
        super().__init__(parent, name)
        process_hash = hasher.hash_callable(self._process)
        self.hash = hasher.hash_string(parent.hash + process_hash)

    def process(self, payload: Payload) -> PayloadIterator:
        """Do the processing."""

        for result in self._process(payload):
            yield result
