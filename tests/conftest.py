"""Test fixtures."""

import pathlib
from typing import Any, Iterable

from pytest_cases import fixture, parametrize

from wingline.files.containers import Gzip, Zip
from wingline.files.formats import Csv, JsonLines
from wingline.types import PayloadIterable

TEST_FILES = [
    (
        # b2sum --length 64 --binary examples/data/dynamodb-tv-casts.jl.gz
        # c8e2e027a73751df *examples/data/dynamodb-tv-casts.jl.gz
        "dynamodb-tv-casts.jl.gz",
        "c8e2e027a73751df",
        (Gzip, JsonLines),
        85,
        True,  # Automatic detection
        813248,  # Size
    ),
    (
        # b2sum --length 64 --binary 2016Census_G20A_NSW_LGA.zip
        # 884018a59916efc7 *2016Census_G20A_NSW_LGA.zip
        "2016Census_G20A_NSW_LGA.csv.zip",
        "884018a59916efc7",
        (Zip, Csv),
        132,
        False,  # Automatic detection
        45974,  # Size
    ),
]


def data_dir():
    return pathlib.Path(__file__).parents[1] / "examples" / "data"


@fixture
def unrecognizable_file():
    """A deliberately unrecognizable mystery file."""

    return data_dir() / "deliberately-unrecognizable-file.wtf"


def files() -> Iterable[tuple[Any, ...]]:
    """All test file details."""

    dir = data_dir()
    test_files = []
    for (
        basename,
        content_hash,
        filetype,
        line_count,
        expect_automatic_detection,
        size,
    ) in TEST_FILES:
        path = dir / basename
        container_type, format_type = filetype
        test_files.append(
            (
                path,
                content_hash,
                container_type,
                format_type,
                line_count,
                expect_automatic_detection,
                size,
            )
        )
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
def add_one_input():
    return [
        {"a": 1, "b": 1, "c": 1},
        {"d": 1, "e": 1, "f": 1},
    ]


@fixture
def func_add_one():
    def add_one(payloads: PayloadIterable) -> PayloadIterable:
        """Add one to the value of each key."""

        for payload in payloads:
            yield {k: v + 1 for (k, v) in payload.items()}

    return add_one
