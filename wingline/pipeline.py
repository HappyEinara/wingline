"""High-level interface."""

from __future__ import annotations

import pathlib
from typing import Optional

from wingline import graph
from wingline.files import containers, file, formats
from wingline.plumbing import tap
from wingline.plumbing.pipes import processpipe
from wingline.plumbing.sinks import iteratorsink, writersink
from wingline.plumbing.taps import filetap
from wingline.types import PayloadIterator, PipeProcess, Source


class Pipeline:
    def __init__(
        self,
        source: Source,
        *processes: PipeProcess,
        name: Optional[str] = None,
    ):

        self.name = name if name is not None else f"wingline-{id(self)}"

        if isinstance(source, pathlib.Path):
            source_file = file.File(source)
            name = f"source-{source_file.stem}"
            self._pipe = filetap.FileTap(source_file, name=name)
        else:
            name = f"source-{source.__class__.__name__}"
            self._pipe = tap.Tap(source, name=name)

        for process in processes:
            self.process(process)

    def process(self, process: PipeProcess) -> Pipeline:
        """Fluent interface to add a process."""

        if self._pipe.started:
            raise RuntimeError("Can't add processes after pipeline has started.")

        self._pipe = processpipe.ProcessPipe(self._pipe, process)
        return self

    def write(
        self,
        output_file: pathlib.Path,
        format: Optional[type[formats.Format]] = None,
        container: Optional[type[containers.Container]] = None,
        name: Optional[str] = None,
    ) -> Pipeline:
        """Fluent interface to add a file writer."""

        if self._pipe.started:
            raise RuntimeError("Can't add writers after pipeline has started.")
        name = name if name is not None else f"writer-{self._pipe.name}"
        self._pipe = writersink.WriterSink(
            self._pipe,
            output_file,
            name=name,
            format_type=format,
            container_type=container,
        )
        return self

    @property
    def started(self):
        """Read-only property. True if the pipe has started."""

        return self._pipe.started

    @property
    def graph(self):
        """Get the graph for the pipeline."""

        return graph.PipelineGraph(self)

    def run(self):
        """Run the pipeline."""

        if self.started:
            raise RuntimeError("Pipeline has already started!")
        self._pipe.start()
        self._pipe.join()

    def __iter__(self) -> PayloadIterator:
        """Iterate over the result of the pipeline."""

        if self.started:
            raise RuntimeError("Pipeline has already started.")

        name = f"iter-{self.name}"
        self._pipe = iteratorsink.IteratorSink(self._pipe, name=name)
        return iter(self._pipe)
