"""Class to represent pipelines as a graph."""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Set

from wingline import ui
from wingline.plumbing import pipe, sink, tap
from wingline.plumbing.pipes import cachereader

GraphDict = Dict[pipe.BasePipe, List[pipe.BasePipe]]

logger = logging.getLogger(__name__)


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
        self.refresh_active_nodes()

    def refresh_active_nodes(self) -> None:
        """Activate/deactivate nodes based on caching and sinks."""

        def _activate_ancestors(this_pipe: pipe.BasePipe, active: bool) -> None:
            """Set ancestors to active unless a cache reader has appeared."""

            if isinstance(this_pipe, sink.Sink):
                active = True
            this_pipe.is_active = active
            if this_pipe.relationships.parent:
                if isinstance(this_pipe, cachereader.CacheReader):
                    _activate_ancestors(this_pipe.relationships.parent, False)
                else:
                    _activate_ancestors(this_pipe.relationships.parent, active)

        for next_sink in self.sinks:
            _activate_ancestors(next_sink, True)

    @property
    def started(self) -> bool:
        """Read-only property. True if the pipe has started."""

        return any(node.started for node in self._graph)

    def run(self) -> None:
        """Run the pipeline."""

        if self.started:
            raise RuntimeError("Pipeline has already started!")

        for next_sink in self.sinks:
            logger.debug("%s: starting sink %s", self, next_sink)
            next_sink.start()
        logger.debug("%s: all sinks started", self)
        for next_sink in self.sinks:
            logger.debug("%s: waiting to join %s", self, next_sink)
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
            node
            for node in self.nodes
            if isinstance(node, tap.Tap) and node.is_real_tap
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

    def print(self):
        """Print the graph."""
        ui.print(ui.graph_tree(self))
