"""Misc pipe tests."""

# pylint: disable=redefined-outer-name

from typing import Any, Dict, List

import pytest

import wingline
from wingline.types import AllProcess


def test_missing_hash(
    add_one_input: List[Dict[str, Any]], func_add_one: AllProcess
) -> None:
    """Missing hash raises a runtime error."""

    test_pipeline = wingline.Pipeline(add_one_input)
    first_process = test_pipeline.all(func_add_one)
    first_process.at_node.hash = None
    with pytest.raises(RuntimeError):
        first_process.all(func_add_one)


def test_start_twice(
    add_one_input: List[Dict[str, Any]], func_add_one: AllProcess
) -> None:
    """Starting a pipe twice raises a runtime error."""

    test_pipeline = wingline.Pipeline(add_one_input)
    first_process = test_pipeline.all(func_add_one)
    second_process = first_process.all(func_add_one)
    first_process.at_node.start()
    assert first_process.at_node.started
    with pytest.raises(RuntimeError):
        second_process.at_node.start()


def test_join_before_start(
    add_one_input: List[Dict[str, Any]], func_add_one: AllProcess
) -> None:
    """Joining a pipe that hasn't started raises a runtime error."""

    test_pipeline = wingline.Pipeline(add_one_input)
    first_process = test_pipeline.all(func_add_one)
    with pytest.raises(RuntimeError):
        first_process.at_node.join()


def test_str_repr(
    add_one_input: List[Dict[str, Any]], func_add_one: AllProcess
) -> None:
    """Test str and repr for pipe raise no exceptions."""
    test_pipeline = wingline.Pipeline(add_one_input, func_add_one, func_add_one)
    for node in test_pipeline.graph.nodes:
        assert str(node)
        assert repr(node)
