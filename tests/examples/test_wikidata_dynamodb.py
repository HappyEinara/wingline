"""Test the Wikidata/Dynamodb example."""

import pathlib

import pytest

import wingline
from wingline import helpers


@pytest.fixture
def input_path() -> pathlib.Path:
    """Return the path of the input file."""
    return pathlib.Path(__file__).parents[2] / "examples/data/dynamodb-tv-casts.jl.gz"


def test_wikidata_dynamodb_example(input_path: pathlib.Path, tmp_path) -> None:
    """Test the Wikidata/Dynamodb example end-to-end."""

    assert input_path.exists

    pl = (
        wingline.Pipeline(input_path)
        .all(helpers.dynamodb_deserialize)
        .write(tmp_path / "tv.jsonl")
    )
    result = list(pl)
    assert result
