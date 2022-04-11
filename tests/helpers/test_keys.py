"""Test helpers adapting specific keys."""

from typing import Any, Dict, List

import pytest

import wingline
from wingline import helpers
from wingline.types import AllProcess

KEYS = [
    # Input, snake, python, Pascal, camel
    (
        "NymphicicusHollandicus",
        "nymphicicus_hollandicus",
        "nymphicicus_hollandicus",
        "NymphicicusHollandicus",
        "nymphicicusHollandicus",
    ),
    (
        "CacatuaÖphthalmica",
        "cacatuaöphthalmica",
        "cacatua_ophthalmica",
        "CacatuaÖphthalmica",
        "cacatuaÖphthalmica",
    ),
    (
        "paddy_o'brien",
        "paddy_o'brien",
        "paddy_o_brien",
        "PaddyO'brien",
        "paddyO'brien",
    ),
]


def key_cases():
    """Return a set of key cases."""
    return (
        (
            [{k[0]: 1 for k in KEYS}],
            [{k[1]: 1 for k in KEYS}],
            [{k[2]: 1 for k in KEYS}],
            [{k[3]: 1 for k in KEYS}],
            [{k[4]: 1 for k in KEYS}],
        ),
    )


def test_filter_keys(
    add_one_input: List[Dict[str, Any]], func_add_one: AllProcess
) -> None:
    """The filter keys helper works."""

    filter_pipeline = (
        wingline.Pipeline(add_one_input).all(func_add_one).all(helpers.keys.filter("a"))
    )
    filter_result = list(filter_pipeline)
    assert filter_result == [
        {"a": 2},
        {"a": 3},
    ]


def test_remove_keys(
    add_one_input: List[Dict[str, Any]], func_add_one: AllProcess
) -> None:
    """The filter keys helper works."""

    filter_pipeline = (
        wingline.Pipeline(add_one_input).all(func_add_one).all(helpers.keys.remove("a"))
    )
    filter_result = list(filter_pipeline)
    assert filter_result == [
        {"b": 2, "c": 2},
        {"b": 3, "c": 3},
    ]


@pytest.mark.parametrize("input,snake,python,pascal,camel", key_cases())
def test_key_transliteration(
    input: List[Dict[str, Any]],  # pylint: disable=redefined-builtin
    snake: List[Dict[str, Any]],
    python: List[Dict[str, Any]],
    pascal: List[Dict[str, Any]],
    camel: List[Dict[str, Any]],
) -> None:
    """Test that the key transliteration helpers work."""

    snake_result = list(wingline.Pipeline(input).all(helpers.keys.snake))
    assert snake_result == snake
    python_result = list(wingline.Pipeline(input).all(helpers.keys.python))
    assert python_result == python
    pascal_result = list(wingline.Pipeline(input).all(helpers.keys.pascal))
    assert pascal_result == pascal
    camel_result = list(wingline.Pipeline(input).all(helpers.keys.camel))
    assert camel_result == camel
