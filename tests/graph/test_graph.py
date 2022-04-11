"""Test the graph."""
import pathlib
import threading

import pytest

import wingline
from wingline.types import AllProcess, PayloadIterable

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


@pytest.mark.parametrize("input,_", CASES)
def test_graph(input, _, func_add_one, tmp_path):
    """The fluent interface works."""

    output_file = tmp_path / "test-write-fluent-output.jl"
    test_pipeline = (
        wingline.Pipeline(input).all(func_add_one).all(func_add_one).write(output_file)
    )

    graph = test_pipeline.graph.dict
    # TODO validate this better when the graph is
    # stable.
    assert len(graph) == 1

    # There should only be the main thread left
    # Check the threading module's reports are
    # consistent with that.
    assert threading.active_count() == 1
    assert (
        threading.enumerate()[0]
        == threading.main_thread()
        == threading.current_thread()
    )


@pytest.mark.parametrize("input,_", CASES)
def test_start_twice(input, _, func_add_one, tmp_path):
    """Starting twice raises."""

    output_file = tmp_path / "test-write-fluent-output.jl"
    test_pipeline = (
        wingline.Pipeline(input).all(func_add_one).all(func_add_one).write(output_file)
    )

    list(test_pipeline.graph.taps)[0].start()
    with pytest.raises(RuntimeError):
        test_pipeline.graph.run()


def test_print_graph(
    add_one_input: PayloadIterable, func_add_one: AllProcess, tmp_path: pathlib.Path
) -> None:
    """The print method doesn't raise."""

    output_file = tmp_path / "test-write-fluent-output.jl"
    test_pipeline = (
        wingline.Pipeline(add_one_input)
        .all(func_add_one)
        .all(func_add_one)
        .write(output_file)
    )

    graph = test_pipeline.graph
    graph.print()
