"""Class to represent pipelines as a graph."""
from __future__ import annotations

from typing import TYPE_CHECKING, Any

from wingline.plumbing import pipe, sink, tap

if TYPE_CHECKING:
    from wingline import pipeline


class PipelineGraph:
    def __init__(self, pipeline: pipeline.Pipeline):
        self.pipeline = pipeline

    @property
    def _graph(self) -> dict[pipe.Pipe, list[pipe.Pipe]]:
        """Return the graph as a dict."""

        at: pipe.Pipe = self.pipeline._pipe
        while hasattr(at, "parent"):
            at = at.parent
        apex_node = at

        def _populate_children(node, graph):
            for child in node.children:
                graph.setdefault(node, []).append(child)
                _populate_children(child, graph)

        graph = {}
        _populate_children(apex_node, graph)

        return graph

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
