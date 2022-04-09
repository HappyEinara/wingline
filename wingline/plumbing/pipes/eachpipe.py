"""A process pipe that operatis on individual records"""

from typing import Optional

from wingline.plumbing import pipe
from wingline.plumbing.pipes import processpipe
from wingline.types import EachProcess, PayloadIterable, PayloadIterator, PipeProcess


class EachPipe(processpipe.ProcessPipe):
    """A pipe that carries out a process on its input"""

    emoji = "🠞⚙️"

    def __init__(
        self,
        parent: pipe.BasePipe,
        each_process: EachProcess,
        name: Optional[str] = None,
    ):
        self._each_process = each_process
        process = EachPipe.each_process(each_process)
        super().__init__(parent, process, name)

    @staticmethod
    def each_process(each_process: EachProcess) -> PipeProcess:
        """Do the processing."""

        def _each_process(payloads: PayloadIterable) -> PayloadIterator:
            for payload in payloads:
                result = each_process(payload)
                if result is not None:
                    yield result

        return _each_process
