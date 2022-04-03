"""High-level interface."""

from __future__ import annotations

import pathlib
from typing import Optional, Union

from wingline import graph
from wingline.cache import intermediate
from wingline.files import containers, file, formats
from wingline.plumbing import pipe, tap
from wingline.plumbing.pipes import cachereader, cachewriter, processpipe
from wingline.plumbing.sinks import iteratorsink, writersink
from wingline.plumbing.taps import filetap
from wingline.types import PayloadIterable, PayloadIterator, PipeProcess


class Pipeline:
    """Fluent interface to a pipeline at a specific node."""
    def __init__(
        self,
        source: Source,
        *processes: PipeProcess,
        name: Optional[str] = None,
        cache_dir:Optional[pathlib.Path] = None,
        at_node: Optional[pipe.Pipe] = None
    ):
        if isinstance(source, Pipeline):
            if at_node is None:
                raise ValueError("Cloned pipelines must provide `at_node`")
            self.name = source.name
            self.cache_dir = source.cache_dir
            self.graph = source.graph
            self.at: pipe.Pipe = at_node
        else:
            self.name = name if name is not None else f"wingline-{id(self)}"
            self.cache_dir = cache_dir
            self.graph = graph.PipelineGraph(self.name)
            source_node = self._get_source_node(source)
            self.graph.add_node(source_node)
            self.at = source_node
            for process in processes:
                self.at = self.process(process).at

    def _get_source_node(self, source: Source) -> tap.Tap:
        """Interpret the source to create the initial node of a new pipeline."""

        if isinstance(source, pathlib.Path):
            source_file = file.File(source)
        elif isinstance(source, file.File):
            source_file = source
        else:
            name = f"source-{source.__class__.__name__}"
            source_node = tap.Tap(source, name=name)
            return source_node

        name = f"source-{source_file.stem}"
        source_node = filetap.FileTap(source_file, name=name)
        return source_node

    def process(self, process: PipeProcess) -> Pipeline:
        """Fluent interface to add a process."""

        if self.graph.started:
            raise RuntimeError("Can't add processes after pipeline has started.")

        new_pipe = processpipe.ProcessPipe(self.at, process)
        self.graph.add_node(new_pipe)
        if self.cache_dir:
            cache_path = intermediate.get_cache_path(new_pipe.hash, self.cache_dir)
            if cache_path.exists():
                new_pipe = cachereader.CacheReader(new_pipe, cache_path)
            else:
                new_pipe = cachewriter.CacheWriter(new_pipe, cache_path)
            self.graph.add_node(new_pipe)
        return Pipeline(self, at_node=new_pipe)

    def write(
        self,
        output_file: pathlib.Path,
        format: Optional[type[formats.Format]] = None,
        container: Optional[type[containers.Container]] = None,
        name: Optional[str] = None,
    ) -> Pipeline:
        """Fluent interface to add a file writer."""

        if self.graph.started:
            raise RuntimeError("Can't add writers after pipeline has started.")

        name = name if name is not None else f"writer-{self.at.name}"

        new_pipe = writersink.WriterSink(
            self.at,
            output_file,
            name=name,
            format_type=format,
            container_type=container,
        )
        self.graph.add_node(new_pipe)
        return Pipeline(self, at_node=new_pipe)

    def run(self):
        """Run the graph."""

        self.graph.run()

    def __iter__(self) -> PayloadIterator:
        """Iterate over the node."""

        if self.graph.started:
            raise RuntimeError("Pipeline has already started.")

        name = f"iter-{self.at.name}"
        new_pipe = iteratorsink.IteratorSink(self.at, name=name)
        self.graph.add_node(new_pipe)
        return iter(new_pipe)

Source = Union[pathlib.Path, PayloadIterable, PayloadIterator, Pipeline, file.File]
