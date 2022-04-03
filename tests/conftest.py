"""Test fixtures."""

import pathlib
from typing import Any, Iterable

from pytest_cases import fixture, parametrize

from wingline.files import containers, formats
from wingline.types import Payload, PayloadIterator

TEST_FILES = [
    (
        # b2sum --length 64 --binary examples/data/dynamodb-tv-casts.jl.gz
        # c8e2e027a73751df *examples/data/dynamodb-tv-casts.jl.gz
        "dynamodb-tv-casts.jl.gz",
        "c8e2e027a73751df",
        ("Gzip", "JsonLines"),
        85,
    ),
    (
        # b2sum --length 64 --binary examples/data/2016Census_G01_NSW_LGA.csv
        # cbd95cd394278dc5 *examples/data/2016Census_G01_NSW_LGA.csv
        "2016Census_G01_NSW_LGA.csv",
        "cbd95cd394278dc5",
        (None, "Csv"),
        132,
    ),
    # (
    #     # b2sum --length 64 --binary examples/data/2016_GCP_LGA_for_NSW_short-header.zip
    #     # d89d6200b8975e2e *examples/data/2016_GCP_LGA_for_NSW_short-header.zip
    #     "2016_GCP_LGA_for_NSW_short-header.zip",
    #     "d89d6200b8975e2e",
    #     ("Zip", "Csv"),
    #     85,
    # ),
]


def data_dir():
    return pathlib.Path(__file__).parents[1] / "examples" / "data"


def files() -> Iterable[tuple[Any, ...]]:
    """All test file details."""

    dir = data_dir()
    test_files = []
    for basename, content_hash, filetype, line_count in TEST_FILES:
        path = dir / basename
        container_name, format_name = filetype
        container_type = containers.CONTAINERS_BY_NAME.get(
            container_name, containers.DEFAULT_CONTAINER
        )
        format_type = formats.FORMATS_BY_NAME[format_name]
        test_files.append((path, content_hash, container_type, format_type, line_count))
    return test_files


@fixture()
@parametrize("file_details", files())
def file(file_details):
    return file_details


@fixture
def simple_data():
    return [
        {"name": "Doctor Who", "first_aired": "1963"},
        {"name": "24", "first_aired": "2001"},
        {"name": "The Sopranos", "first_aired": "1999"},
    ]


@fixture
def func_add_one():
    def add_one(payload: Payload) -> PayloadIterator:

        """Add one to the value of each key."""

        yield {k: v + 1 for (k, v) in payload.items()}

    return add_one
