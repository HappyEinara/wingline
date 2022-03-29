"""Test the printing helpers."""


import pprint

from wingline import Pipeline, helpers


def test_pretty_printer(simple_data, capsys):

    test_pipe = Pipeline(simple_data, helpers.pretty)
    result = list(test_pipe)
    assert len(result) == len(simple_data)

    captured = capsys.readouterr()
    stdout = captured.out

    assert stdout == "\n".join((pprint.pformat(item) for item in simple_data)) + "\n"
