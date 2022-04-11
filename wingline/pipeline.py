"""High-level interface."""

from __future__ import annotations

import pathlib
from typing import Optional, Union

from wingline import graph
from wingline.cache import intermediate
from wingline.files import file
from wingline.plumbing import base
from wingline.plumbing.caching import cachereader, cachewriter
from wingline.plumbing.pipes import allpipe, eachpipe
from wingline.plumbing.sinks import iterator, writer
from wingline.plumbing.taps import filetap
from wingline.types import AllProcess, EachProcess, PayloadIterable, PayloadIterator


class Pipeline:
    """Fluent interface to a pipeline at a specific node."""

    def __init__(
        self,
        source: Source,
        *processes: AllProcess,
        name: Optional[str] = None,
        cache_dir: Optional[Union[pathlib.Path, str]] = None,
        at_node: Optional[base.Plumbing] = None,
    ):
        if isinstance(source, Pipeline):
            if at_node is None:
                raise ValueError("Cloned pipelines must provide `at_node`")
            self.name: str = source.name
            self.cache_dir: Optional[pathlib.Path] = source.cache_dir
            self.graph: graph.PipelineGraph = source.graph
            self.at_node: base.Plumbing = at_node
        else:
            self.name = name if name is not None else f"wingline-{id(self)}"
            self.cache_dir = pathlib.Path(cache_dir) if cache_dir is not None else None
            self.graph = graph.PipelineGraph(self.name)
            source_node = self._get_source_node(source)
            self.graph.add_node(source_node)
            self.at_node = source_node
            for process in processes:
                self.at_node = self.all(process).at_node

    @staticmethod
    def _get_source_node(source: Source) -> base.Tap:
        """Interpret the source to create the initial node of a new pipeline."""

        if isinstance(source, pathlib.Path):
            source_file = file.File(source)
        elif isinstance(source, file.File):
            source_file = source
        else:
            name = f"source-{source.__class__.__name__}"
            source_node = base.Tap(source, name=name)
            return source_node

        name = f"source-{source_file.stem}"
        source_node = filetap.FileTap(source_file, name=name)
        return source_node

    def all(self, process: AllProcess, name: Optional[str] = None) -> Pipeline:
        """Fluent interface to add a process."""

        if self.graph.started:
            raise RuntimeError("Can't add processes after pipeline has started.")

        new_pipe: base.Plumbing = allpipe.AllPipe(self.at_node, process, name=name)
        self.graph.add_node(new_pipe)
        if self.cache_dir and new_pipe.hash:
            cache_path = intermediate.get_cache_path(new_pipe.hash, self.cache_dir)
            if cache_path.exists():
                new_pipe = cachereader.CacheReader(new_pipe, cache_path)
            else:
                new_pipe = cachewriter.CacheWriter(new_pipe, cache_path)
            self.graph.add_node(new_pipe)
        return Pipeline(self, at_node=new_pipe)

    def each(self, process: EachProcess) -> Pipeline:
        """Fluent interface to add an each-process."""

        if self.graph.started:
            raise RuntimeError("Can't add processes after pipeline has started.")

        new_pipe: base.Plumbing = eachpipe.EachPipe(self.at_node, process)
        self.graph.add_node(new_pipe)
        if self.cache_dir and new_pipe.hash:
            cache_path = intermediate.get_cache_path(new_pipe.hash, self.cache_dir)
            if cache_path.exists():
                new_pipe = cachereader.CacheReader(new_pipe, cache_path)
            else:
                new_pipe = cachewriter.CacheWriter(new_pipe, cache_path)
            self.graph.add_node(new_pipe)
        return Pipeline(self, at_node=new_pipe)

    def write(
        self,
        output_file: Union[str, pathlib.Path, file.File],
        name: Optional[str] = None,
    ) -> Pipeline:
        """Fluent interface to add a file writer."""

        if isinstance(output_file, str):
            output_file = pathlib.Path(output_file)
        if isinstance(output_file, pathlib.Path):
            output_file = file.File(output_file)

        if self.graph.started:
            raise RuntimeError("Can't add writers after pipeline has started.")

        name = name if name is not None else f"writer-{self.at_node.name}"

        new_pipe = writer.WriterSink(
            self.at_node,
            output_file,
            name=name,
        )
        self.graph.add_node(new_pipe)
        return Pipeline(self, at_node=new_pipe)

    def show_graph(self) -> None:
        """Output a visualisation of the graph."""

        self.graph.print()

    def run(self) -> None:
        """Run the graph."""

        self.graph.run()

    def __iter__(self) -> PayloadIterator:
        """Iterate over the node."""

        if self.graph.started:
            raise RuntimeError("Pipeline has already started.")

        name = f"iter-{self.at_node.name}"
        new_pipe = iterator.IteratorSink(self.at_node, name=name)
        self.graph.add_node(new_pipe)
        return iter(new_pipe)


Source = Union[pathlib.Path, PayloadIterable, PayloadIterator, Pipeline, file.File]
