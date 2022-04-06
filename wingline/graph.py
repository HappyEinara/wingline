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

        def _add_children(graph: GraphDict, node: pipe.Pipe) -> None:
            """Recursively add child nodes."""

            graph.setdefault(node, [])
            if getattr(node, "parent", None):
                graph.setdefault(node.parent, []).append(node)

            # Currently new nodes never have children.
            # Commenting this to satisfy Coverage
            # Add this back if trees are coupled.
            #
            # for child in node.children:
            #     _add_children(graph, child)

        _add_children(self._graph, node)

    @property
    def started(self) -> bool:
        """Read-only property. True if the pipe has started."""

        return any(node.started for node in self._graph)

    def run(self) -> None:
        """Run the pipeline."""

        if self.started:
            raise RuntimeError("Pipeline has already started!")

        for next_sink in self.sinks:
            next_sink.start()
        for next_sink in self.sinks:
            next_sink.join()

    @property
    def nodes(self) -> set[pipe.Pipe]:
        """Return all the nodes in the graph."""
        nodes: set[pipe.Pipe] = {node for node in self._graph.keys()}
        return nodes

    @property
    def taps(self) -> set[pipe.Pipe]:
        """Return all the taps in the graph."""

        taps: set[pipe.Pipe] = {
            node for node in self.nodes if isinstance(node, tap.Tap)
        }
        return taps

    @property
    def sinks(self) -> set[pipe.Pipe]:
        """Return all the sinks in the graph."""

        sinks: set[pipe.Pipe] = {
            node for node in self.nodes if isinstance(node, sink.Sink)
        }
        return sinks

    @property
    def dict(self) -> dict[pipe.Pipe, Any]:
        """Return the graph as a nested dict."""

        def add_node(graph: dict[pipe.Pipe, Any], node: pipe.Pipe) -> None:
            subgraph = graph.setdefault(node, {})
            for child in node.children:
                add_node(subgraph, child)

        graph_dict: dict[pipe.Pipe, Any] = {}
        for next_tap in self.taps:
            add_node(graph_dict, next_tap)

        return graph_dict
