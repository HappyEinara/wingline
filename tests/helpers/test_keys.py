"""Test helpers adapting specific keys."""

import wingline
from wingline import helpers


def test_filter_keys(add_one_input, func_add_one):
    """The filter keys helper works."""

    filter_pipeline = (
        wingline.Pipeline(add_one_input)
        .process(func_add_one)
        .process(helpers.filter_keys("a"))
    )
    filter_result = list(filter_pipeline)
    assert filter_result == [
        {"a": 2},
        {"a": 3},
    ]


def test_remove_keys(add_one_input, func_add_one):
    """The filter keys helper works."""

    filter_pipeline = (
        wingline.Pipeline(add_one_input)
        .process(func_add_one)
        .process(helpers.remove_keys("a"))
    )
    filter_result = list(filter_pipeline)
    assert filter_result == [
        {"b": 2, "c": 2},
        {"b": 3, "c": 3},
    ]
