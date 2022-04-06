from wingline import Pipeline, helpers


def test_head(simple_data):

    test_pipe = Pipeline(simple_data, helpers.head(2))
    result = list(test_pipe)
    assert len(result) == 2
    assert result == simple_data[:2]


def test_tail(simple_data):

    test_pipe = Pipeline(simple_data, helpers.tail(2))
    result = list(test_pipe)
    assert len(result) == 2
    assert result == simple_data[-2:]
