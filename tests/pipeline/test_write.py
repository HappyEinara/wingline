"""Test the Pipeline.write interface."""

import random
import threading

import pytest

import wingline
from wingline import json
from wingline.files import containers, file
from wingline.files import filetype as ft
from wingline.files import formats

CASES = (
    (
        [
            {"a": 1, "b": 1, "c": 1},
            {"a": 2, "b": 2, "c": 2},
        ],
        "\n".join(
            [
                '{"a":3,"b":3,"c":3}',
                '{"a":4,"b":4,"c":4}',
            ]
        )
        + "\n",
    ),
)


@pytest.mark.parametrize("input,expected", CASES)
@pytest.mark.parametrize("output_path_as_str", [True, False])
def test_write_fluent(
    input, expected, func_add_one, tmp_path, output_path_as_str: bool
):
    """The fluent interface works."""

    output_file = tmp_path / "test-write-fluent-output.jl"
    if output_path_as_str:
        output_file = str(output_file)
    test_pipeline = (
        wingline.Pipeline(input).all(func_add_one).all(func_add_one).write(output_file)
    )

    test_pipeline.run()

    # There should only be the main thread left
    # Check the threading module's reports are
    # consistent with that.
    assert threading.active_count() == 1
    assert (
        threading.enumerate()[0]
        == threading.main_thread()
        == threading.current_thread()
    )

    output_files = list(tmp_path.glob("*"))
    assert len(output_files) == 1

    result = output_files[0].read_text()
    assert result == expected


@pytest.mark.parametrize("input_data,expected", CASES)
@pytest.mark.parametrize(
    "container", [None, containers.Bare, containers.Gzip, containers.Zip]
)
@pytest.mark.parametrize("format", [formats.JsonLines, formats.Csv, formats.Msgpack])
def test_write_formats(input_data, expected, func_add_one, tmp_path, container, format):
    """All formats and containers work."""

    container_suffix = (
        random.choice(list(container.suffixes))
        if container and container.suffixes
        else ""
    )
    format_suffix = random.choice(list(format.suffixes))
    output_file = tmp_path / f"test_formats{format_suffix}{container_suffix}"

    test_pipeline = (
        wingline.Pipeline(input_data)
        .all(func_add_one)
        .all(func_add_one)
        .write(
            file.File(
                output_file,
                filetype=ft.Filetype(
                    container=(container() if container else None), format=format()
                ),
            )
        )
    )

    test_pipeline.run()

    # There should only be the main thread left
    # Check the threading module's reports are
    # consistent with that.
    assert threading.active_count() == 1
    assert (
        threading.enumerate()[0]
        == threading.main_thread()
        == threading.current_thread()
    )

    assert output_file.exists()
    reread = [
        {k: int(v) for k, v in payload.items()}
        for payload in wingline.Pipeline(output_file)
    ]
    expected_obj = [json.loads(line) for line in expected.strip().split("\n")]
    assert reread == expected_obj
