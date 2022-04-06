"""Test the graph."""

import pytest
import rich.console

import wingline
from wingline import ui

CASES = (
    (
        [
            {"a": 1, "b": 1, "c": 1},
            {"d": 1, "e": 1, "f": 1},
        ],
        "\n".join(
            [
                '{"a": 3, "b": 3, "c": 3}',
                '{"d": 3, "e": 3, "f": 3}',
            ]
        )
        + "\n",
    ),
)


@pytest.mark.parametrize("input,expected", CASES)
def test_ui_graph(input, expected, func_add_one, tmp_path):
    """The fluent interface works."""

    output_file = tmp_path / "test-write-fluent-output.jl"
    test_pipeline = (
        wingline.Pipeline(input)
        .process(func_add_one)
        .process(func_add_one)
        .write(output_file)
    )

    graph = test_pipeline.graph

    graph_tree = ui.graph_tree(graph)

    console = rich.console.Console(stderr=True)
    console.print(graph_tree)
