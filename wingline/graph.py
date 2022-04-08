"""Class to represent pipelines as a graph."""

from __future__ import annotations

from typing import Any, Dict, List, Set

from wingline.plumbing import pipe, sink, tap

GraphDict = Dict[pipe.BasePipe, List[pipe.BasePipe]]


class PipelineGraph:
    """Represent a pipeline as a directed acyclic graph."""

    def __init__(self, name: str) -> None:
        self.name = name
        self._graph: GraphDict = {}

    def add_node(self, node: pipe.BasePipe) -> None:
        """Add a node to the graph."""

        def _add_children(graph: GraphDict, node: pipe.BasePipe) -> None:
            """Recursively add child nodes."""

            graph.setdefault(node, [])
            if node.relationships.parent is not None:
                graph.setdefault(node.relationships.parent, []).append(node)

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
    def nodes(self) -> Set[pipe.BasePipe]:
        """Return all the nodes in the graph."""
        nodes: Set[pipe.BasePipe] = set(node for node in self._graph)
        return nodes

    @property
    def taps(self) -> Set[pipe.BasePipe]:
        """Return all the taps in the graph."""

        taps: Set[pipe.BasePipe] = {
            node for node in self.nodes if isinstance(node, tap.Tap)
        }
        return taps

    @property
    def sinks(self) -> Set[pipe.Pipe]:
        """Return all the sinks in the graph."""

        sinks: Set[pipe.Pipe] = {
            node for node in self.nodes if isinstance(node, sink.Sink)
        }
        return sinks

    @property
    def dict(self) -> Dict[pipe.BasePipe, Any]:
        """Return the graph as a nested dict."""

        def add_node(graph: Dict[pipe.BasePipe, Any], node: pipe.BasePipe) -> None:
            subgraph = graph.setdefault(node, {})
            for child in node.relationships.children:
                add_node(subgraph, child)

        graph_dict: Dict[pipe.BasePipe, Any] = {}
        for next_tap in self.taps:
            add_node(graph_dict, next_tap)

        return graph_dict
