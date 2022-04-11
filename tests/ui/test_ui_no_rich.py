"""Handle missing UI gracefully.

The UI components are intended to be optional.
Wherever they are used they should fail gracefully if the optional
`ui` extra (the `rich` library) is not installed.
"""

import sys
from unittest import mock

import pytest

import wingline
from wingline import ui
from wingline.types import AllProcess, PayloadIterable


def test_graph_with_ui(
    func_add_one: AllProcess, add_one_input: PayloadIterable
) -> None:
    """Graph works with Rich installed."""

    test_pipeline = wingline.Pipeline(add_one_input).all(func_add_one).all(func_add_one)
    graph = test_pipeline.graph

    graph_tree = ui.graph_tree(graph)
    assert graph_tree


def test_graph_without_ui(
    func_add_one: AllProcess, add_one_input: PayloadIterable
) -> None:
    """Graph degrades gracefully if Rich is not present."""

    test_pipeline = wingline.Pipeline(add_one_input).all(func_add_one).all(func_add_one)
    graph = test_pipeline.graph

    with mock.patch.dict(sys.modules, {"rich": None}):
        with pytest.warns(UserWarning):
            graph_tree = ui.graph_tree(graph)
        assert graph_tree is None
