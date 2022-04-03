"""Test the Pipeline.write interface."""

import threading

import pytest

import wingline

CASES = (
    (
        [
            {"a": 1, "b": 1, "c": 1},
            {"d": 1, "e": 1, "f": 1},
        ],
        "\n".join(
            [
                '{"a": 3, "b": 3, "c": 3}',
                '{"d": 3, "e": 3, "f": 3}',
            ]
        )
        + "\n",
    ),
)


@pytest.mark.parametrize("input,expected", CASES)
def test_write_fluent(input, expected, func_add_one, tmp_path):
    """The fluent interface works."""

    output_file = tmp_path / "test-write-fluent-output.jl"
    test_pipeline = (
        wingline.Pipeline(input)
        .process(func_add_one)
        .process(func_add_one)
        .write(output_file)
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