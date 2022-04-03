"""Class to represent pipelines as a graph."""

from __future__ import annotations

from typing import Any

from wingline.plumbing import pipe, sink, tap

GraphDict = dict[pipe.Pipe, list[pipe.Pipe]] 


class PipelineGraph:

    def __init__(self, name: str) -> None:
        self.name = name
        self._graph: GraphDict = {}


    def add_node(self, node: pipe.Pipe) -> None:
        """Add a node to the graph."""

        def _add_children(graph: GraphDict, node: pipe.Pipe):
            """Recursively add child nodes."""

            graph.setdefault(node, [])
            if getattr(node, "parent", None):
                graph.setdefault(node.parent, []).append(node)
            for child in node.children:
                _add_children(graph, child)

        _add_children(self._graph, node)

    @property
    def started(self):
        """Read-only property. True if the pipe has started."""

        return any(node.started for node in self._graph)

    def run(self):
        """Run the pipeline."""

        if self.started:
            raise RuntimeError("Pipeline has already started!")

        for sink in self.sinks:
            sink.start()
        for sink in self.sinks:
            sink.join()

    @property
    def taps(self) -> set[pipe.Pipe]:
        """Return all the taps in the graph."""

        taps: set[pipe.Pipe] = {
            node for node in self._graph.keys() if isinstance(node, tap.Tap)
        }
        return taps

    @property
    def sinks(self) -> set[pipe.Pipe]:
        """Return all the sinks in the graph."""

        sinks: set[pipe.Pipe] = {
            node for node in self._graph.keys() if isinstance(node, sink.Sink)
        }
        return sinks

    @property
    def dict(self) -> dict[pipe.Pipe, Any]:
        """Return the graph as a nested dict."""

        def add_node(graph, node):
            subgraph = graph.setdefault(node, {})
            for child in node.children:
                add_node(subgraph, child)

        graph_dict = {}
        for tap in self.taps:
            add_node(graph_dict, tap)

        return graph_dict
