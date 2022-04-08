"""Misc pipe tests."""
import pytest

import wingline


@pytest.fixture
def simple_input():
    return [
        {"a": 1, "b": 1, "c": 1},
        {"d": 1, "e": 1, "f": 1},
    ]


def test_missing_hash(simple_input, func_add_one):
    """Missing hash raises a runtime error."""

    test_pipeline = wingline.Pipeline(simple_input)
    first_process = test_pipeline.process(func_add_one)
    first_process.at_node.hash = None
    with pytest.raises(RuntimeError):
        first_process.process(func_add_one)


def test_start_twice(simple_input, func_add_one):
    """Starting a pipe twice raises a runtime error."""

    test_pipeline = wingline.Pipeline(simple_input)
    first_process = test_pipeline.process(func_add_one)
    second_process = first_process.process(func_add_one)
    first_process.at_node.start()
    assert first_process.at_node.started
    with pytest.raises(RuntimeError):
        second_process.at_node.start()


def test_join_before_start(simple_input, func_add_one):
    """Joining a pipe that hasn't started raises a runtime error."""

    test_pipeline = wingline.Pipeline(simple_input)
    first_process = test_pipeline.process(func_add_one)
    with pytest.raises(RuntimeError):
        first_process.at_node.join()


def test_str_repr(simple_input, func_add_one):
    """Test str and repr for pipe raise no exceptions."""
    test_pipeline = wingline.Pipeline(simple_input, func_add_one, func_add_one)
    for node in test_pipeline.graph.nodes:
        assert str(node)
        assert repr(node)
