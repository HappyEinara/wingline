"""Test the pipeline."""

from pytest_cases import fixture

import wingline
from wingline.types import PayloadIterator


def add_one(parent: PayloadIterator) -> PayloadIterator:

    """Add one to the value of each key."""

    for payload in parent:
        yield {k: v + 1 for (k, v) in payload.items()}


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


def test_pipeline(input, expected):
    """The pipeline works with operations as args."""

    test_pipeline = wingline.Pipeline(input, add_one, add_one)
    result = list(test_pipeline)
    assert result == expected


def test_pipeline_fluent(input, expected):
    """The fluent interface works."""

    test_pipeline = wingline.Pipeline(input).pipe(add_one).pipe(add_one)

    result = list(test_pipeline)
    assert result == expected
