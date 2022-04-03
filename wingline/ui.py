"""UI elements."""

import functools
import warnings

from wingline import graph
from wingline.plumbing import pipe

try:
    import rich.text
    import rich.tree

    UI_AVAILABLE = True
except ImportError:
    UI_AVAILABLE = False


def warn_if_unavailable(func):
    @functools.wraps(func)
    def _inner(*args, **kwargs):
        if not UI_AVAILABLE:
            warnings.warn(
                "Attempted to call a UI function when the UI extras are not installed."
            )
            return lambda: None
        return func(*args, **kwargs)

    return _inner

@warn_if_unavailable
def render_pipe(pipe: pipe.Pipe):
    return rich.text.Text(str(pipe))

@warn_if_unavailable
def graph_tree(graph: graph.PipelineGraph):
    """Pretty print a tree of the graph."""

    tree = rich.tree.Tree(graph.name)

    def add_graph(tree, graph_dict):
        for node, children in graph_dict.items():
            subtree = tree.add(render_pipe(node))
            add_graph(subtree, children)

    add_graph(tree, graph.dict)

    return tree
