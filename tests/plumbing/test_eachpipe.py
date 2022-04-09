"""Test the each-processes."""

from typing import Dict, List

import wingline
from wingline.types import EachProcess, PipeProcess


def test_each_process(
    add_one_input: List[Dict[str, int]],
    add_one_output_twice: List[Dict[str, int]],
    func_add_one: PipeProcess,
    func_each_add_one: EachProcess,
) -> None:
    """Joining a pipe that hasn't started raises a runtime error."""

    each_pipeline = (
        wingline.Pipeline(add_one_input).each(func_each_add_one).each(func_each_add_one)
    )
    each_result = list(each_pipeline)

    assert each_result == add_one_output_twice

    # Cross-check with .process()
    process_pipeline = (
        wingline.Pipeline(add_one_input).process(func_add_one).process(func_add_one)
    )
    process_result = list(process_pipeline)

    assert process_result == each_result
