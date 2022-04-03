"""Test the pipeline."""

from pytest_cases import fixture

import wingline
from wingline.types import Payload, PayloadIterator


@fixture
def input():
    return [
        {"a": 1, "b": 1, "c": 1},
        {"d": 1, "e": 1, "f": 1},
    ]


@fixture
def expected():
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
    """The fluent interface works."""

    test_pipeline = wingline.Pipeline(input).process(func_add_one).process(func_add_one)

    result = list(test_pipeline)
    assert result == expected
