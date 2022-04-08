"""UI elements."""

from __future__ import annotations

import functools
import sys
import warnings
from typing import Any, Callable, Dict

from wingline import graph
from wingline.plumbing import pipe

try:
    import rich.text
    import rich.tree
except ImportError:  # pragma: no cover
    pass


def warn_if_unavailable(func: Callable[..., Any]) -> Callable[..., Any]:
    """Wrap UI functions to gracefully fail if the ui is not installed."""

    @functools.wraps(func)
    def _inner(*args: Any, **kwargs: Any) -> Any:

        if not sys.modules.get("rich"):
            warnings.warn(
                "Attempted to call a UI function when the UI extras are not installed."
            )
            return None
        return func(*args, **kwargs)

    return _inner


@warn_if_unavailable
def render_pipe(top_pipe: pipe.BasePipe) -> rich.text.Text:
    """Pretty print a view of the graph descending from a pipe."""
    return rich.text.Text(str(top_pipe))


@warn_if_unavailable
def graph_tree(input_graph: graph.PipelineGraph) -> rich.tree.Tree:
    """Pretty print a tree of the graph."""

    tree = rich.tree.Tree(input_graph.name)

    def add_graph(tree: rich.tree.Tree, graph_dict: Dict[pipe.BasePipe, Any]) -> None:
        for node, children in graph_dict.items():
            subtree = tree.add(render_pipe(node))
            add_graph(subtree, children)

    add_graph(tree, input_graph.dict)

    return tree
