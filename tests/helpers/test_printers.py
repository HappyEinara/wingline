"""Test the printing helpers."""


import pprint
from typing import Any, Dict, List

from wingline import Pipeline, helpers


def test_pretty_printer(simple_data: List[Dict[str, Any]], capfd: Any) -> None:
    """Test the pretty printer helper."""

    test_pipe = Pipeline(simple_data, helpers.pretty)
    result = list(test_pipe)
    assert len(result) == len(simple_data)

    captured = capfd.readouterr()
    stdout = captured.out

    expected = (
        "\n".join((pprint.pformat(dict(sorted(item.items()))) for item in simple_data))
        + "\n"
    )

    assert stdout == expected
