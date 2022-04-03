"""High-level pipeline interface."""
import logging
import pathlib
from time import sleep
from typing import Optional

from wingline.files import containers, formats
from wingline.plumbing import (
    base,
    execution,
    intermediate_cache,
    pipe,
    queue,
    sink,
    tap,
    utils,
    writer,
)
from wingline.types import PayloadIterable, PayloadIterator, PipeOperation

logger = logging.getLogger(__name__)


class Pipeline:
    """
    High-level interface.

    This is a container for pipes, accepting a Pipe,
    a Tap, or another Pipeline.
    It also accepts an arbitrary iterable
    as a source, instantiating a Tap automatically.

    It also accepts multiple operations, and instantiates
    one ancestor pipe for each.
    """

    name: str
    input_queue: queue.Queue
    source: base.BasePlumbing
    operations: list[PipeOperation]

    def __init__(
        self,
        source: PayloadIterable,
        *operations: PipeOperation,
        name: Optional[str] = None,
        cache_dir: Optional[pathlib.Path] = None,
    ):
        # Basic init and id
        super().__init__()
        self.name = name if name is not None else self.__class__.__name__
        self.input_queue = queue.Queue()
        self.operations = list(operations)

        # If the source is not a wingline-native plumbing element
        # then wrap it in a tap (a one-ended pipe without an upstream
        # parent). This lets us use any old iterable of dicts as a source if
        # the native File classes, for example, are unsuitable.
        if isinstance(source, base.BasePlumbing):
            self.source = source
        else:
            self.source = tap.Tap(source, f"{name}|Tap")

        # Set up the intermediate (file) cache.
        self.cache: Optional[intermediate_cache.IntermediateCache] = (
            intermediate_cache.IntermediateCache(cache_dir)
            if cache_dir is not None
            else None
        )

        # Initialize the graph with the operations specified in the
        # init.
        self._init_operation_pipes()

    def _init_operation_pipes(self) -> None:
        """Populate the initial operations specified in the init."""

        operations = self.operations
        operations_count = len(operations)

        # Use sink as a cursor starting at the beginning of
        # the stream and adding the operations between itself
        # and the source.
        # The sink therefore remains the last element in the pipeline.
        sink: base.BasePlumbing = self.source
        for i, operation in enumerate(operations):
            operation_name = (
                f"{self.name}" f"-{i+1}/{operations_count}" f"-{operation.__name__}"
            )
            sink = pipe.Pipe(parent=sink, operation=operation, name=operation_name)

        self.sink = sink

    @property
    def hash(self) -> str:
        """Return the output hash of the entire process."""
        if self.sink.hash is None:
            raise RuntimeError("Pipeline sink has no hash.")
        return self.sink.hash

    def pipe(self, operation, name: Optional[str] = None):
        self.sink = pipe.Pipe(parent=self.sink, operation=operation, name=name)
        return self

    def write(
        self,
        path: pathlib.Path,
        format: type[formats.Format],
        container: Optional[type[containers.Container]],
    ):
        self.sink = writer.Writer(self.sink, path, format, container)
        return self

    @property
    def execution_plan(self):
        """Build an return an execution plan for the pipeline.

        This is implemented as a property so it's dynamically regenerated on demand.
        The `.pipe()` and `.write()` methods add new plumbing after init to provide
        the fluent interface, so the graph is mutable and the precise execution should
        be calculated right before the pipeline is started.
        """

        return execution.ExecutionPlan(self.sink, self.cache)

    def start(self):
        """Start the pipeline running."""
        plan = self.execution_plan
        logger.debug("Starting pipeline with the following execution plan:")
        logger.debug(plan.pretty())
        self.source = plan.source
        # utils.ThreadMonitor(self.source).start()
        self.source.start()
        sleep(1)
        self.source.join()

    def join(self) -> None:
        self.source.join()

    def __iter__(self) -> PayloadIterator:

        # TODO: Rethink the Sink.
        # There's a semantic conflict between sink-as-concept
        # (defined as the last element in the pipeline)
        # vs the concrete Sink class (provides an iterator as
        # a pleasant interface for implementers; has no downstream
        # children).

        # Each makes reasonable sense in isolation. But
        # here, `self.sink` might not actually be a `Sink`
        # so we have to wrap the sink in a `Sink` to be
        # sure `self.sink` is a `Sink` and not just think
        # the sink is a `Sink`.

        # This is why I drink.
        if not isinstance(self.sink, sink.Sink):
            iter_sink = sink.Sink(self.sink)
        else:
            iter_sink = self.sink
        self.start()
        for payload in iter_sink:
            yield payload
        self.join()
