"""Fixtures."""
import pathlib

import cachelib
from pytest_cases import fixture


@fixture
def testing_cache():
    return cachelib.SimpleCache()


@fixture
def data_dir():
    return pathlib.Path(__file__).parents[1] / "examples" / "data"


@fixture
def simple_data():
    return [
        {"name": "Doctor Who", "first_aired": "1963"},
        {"name": "24", "first_aired": "2001"},
        {"name": "The Sopranos", "first_aired": "1999"},
    ]
