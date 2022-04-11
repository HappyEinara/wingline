"""UI elements."""

from __future__ import annotations

import functools
import sys
import warnings
from typing import TYPE_CHECKING, Any, Callable, Dict

from wingline.plumbing import base

try:
    import rich.console
    import rich.text
    import rich.tree
except ImportError:  # pragma: no cover
    pass

if TYPE_CHECKING:  # pragma: no cover
    from wingline import graph


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
def render_pipe(top_pipe: base.Plumbing) -> rich.text.Text:
    """Pretty print a view of the graph descending from a pipe."""

    if top_pipe.is_active:
        return rich.text.Text(f"{top_pipe}")
    return rich.text.Text(f"{top_pipe}", style="bright_black")


@warn_if_unavailable
def graph_tree(input_graph: graph.PipelineGraph) -> rich.tree.Tree:
    """Pretty print a tree of the graph."""

    tree = rich.tree.Tree(input_graph.name)

    def add_graph(tree: rich.tree.Tree, graph_dict: Dict[base.Plumbing, Any]) -> None:
        for node, children in graph_dict.items():
            subtree = tree.add(render_pipe(node))
            add_graph(subtree, children)

    add_graph(tree, input_graph.dict)

    return tree


@warn_if_unavailable
def print(content: Any) -> None:  # pylint: disable=redefined-builtin
    """Print a Rich renderable"""

    console = rich.console.Console()
    console.print(content)
