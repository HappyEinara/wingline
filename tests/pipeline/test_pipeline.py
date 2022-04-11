"""Test the pipeline."""

# pylint: disable=redefined-outer-name,redefined-builtin

import pathlib

import pytest
from pytest_cases import fixture

import wingline


@fixture
def input():
    """Sample input."""
    return [
        {"a": 1, "b": 1, "c": 1},
        {"d": 1, "e": 1, "f": 1},
    ]


@fixture
def expected():
    """Expected output."""
    return [
        {"a": 3, "b": 3, "c": 3},
        {"d": 3, "e": 3, "f": 3},
    ]


def test_pipeline(input, expected, func_add_one):
    """The pipeline works with operations as args."""

    test_pipeline = wingline.Pipeline(input, func_add_one, func_add_one)
    result = list(test_pipeline)
    assert result == expected


def test_pipeline_fluent(input, func_add_one, expected):
    """the fluent interface works."""

    test_pipeline = wingline.Pipeline(input).all(func_add_one).all(func_add_one)

    result = list(test_pipeline)
    assert result == expected


def test_pipeline_print(input, func_add_one, expected):
    """the fluent interface works."""

    test_pipeline = wingline.Pipeline(input).all(func_add_one).all(func_add_one)
    test_pipeline.show_graph()

    result = list(test_pipeline)
    test_pipeline.show_graph()
    assert result == expected


def test_pipeline_concat_without_at(input, func_add_one):
    """Concatenating pipelines without the `at_node` param raises."""

    test_pipeline_1 = wingline.Pipeline(input).all(func_add_one).all(func_add_one)
    with pytest.raises(ValueError):
        wingline.Pipeline(test_pipeline_1)


def test_pipeline_add_process_after_start(input, func_add_one):
    """Adding a process after starting the pipeline raises."""

    test_pipeline = wingline.Pipeline(input).all(func_add_one)
    test_pipeline.at_node.start()
    with pytest.raises(RuntimeError):
        test_pipeline.all(func_add_one)


def test_pipeline_add_each_after_start(input, func_each_add_one):
    """Adding a process after starting the pipeline raises."""

    test_pipeline = wingline.Pipeline(input).each(func_each_add_one)
    test_pipeline.at_node.start()
    with pytest.raises(RuntimeError):
        test_pipeline.each(func_each_add_one)


def test_pipeline_add_writer_after_start(input, func_add_one):
    """Adding a writer after starting the pipeline raises."""

    test_pipeline = wingline.Pipeline(input).all(func_add_one)
    test_pipeline.at_node.start()
    with pytest.raises(RuntimeError):
        test_pipeline.write(pathlib.Path("blah.jsonl"))


def test_pipeline_iter_after_start(input, func_add_one):
    """Adding a writer after starting the pipeline raises."""

    test_pipeline = wingline.Pipeline(input).all(func_add_one)
    next(iter(test_pipeline))
    with pytest.raises(RuntimeError):
        iter(test_pipeline)
