from wingline.plumbing import pipeline, tap


def append_key(key):
    def _inner(items):
        for item in items:
            item[key] = key
            yield item

    return _inner


def duplicate_items(items):
    for item in items:
        yield item
        yield item


def test_new_pipe():

    input = [{"id": i} for i in range(10000)]
    pipeline = (
        tap.Tap(input, name="Tap")
        .pipe(append_key("a"), name="append_a")
        .pipe(append_key("b"), name="append_b")
        .pipe(append_key("c"), name="append_c")
        .pipe(duplicate_items, name="duplicate")
    )
    pipeline.start()


def test_pipeline():

    input = [
        {"id": 1},
        {"id": 2},
        {"id": 3},
        {"id": 4},
        {"id": 5},
    ]
    expected = [
        {"id": 1, "a": "a", "b": "b", "c": "c"},
        {"id": 1, "a": "a", "b": "b", "c": "c"},
        {"id": 2, "a": "a", "b": "b", "c": "c"},
        {"id": 2, "a": "a", "b": "b", "c": "c"},
        {"id": 3, "a": "a", "b": "b", "c": "c"},
        {"id": 3, "a": "a", "b": "b", "c": "c"},
        {"id": 4, "a": "a", "b": "b", "c": "c"},
        {"id": 4, "a": "a", "b": "b", "c": "c"},
        {"id": 5, "a": "a", "b": "b", "c": "c"},
        {"id": 5, "a": "a", "b": "b", "c": "c"},
    ]
    pipe = pipeline.Pipeline(
        input, append_key("a"), append_key("b"), append_key("c"), duplicate_items
    )
    result = list(pipe)
    assert result == expected


def test_pipeline_fluent():

    input = [
        {"id": 1},
        {"id": 2},
        {"id": 3},
        {"id": 4},
        {"id": 5},
    ]
    expected = [
        {"id": 1, "a": "a", "b": "b", "c": "c"},
        {"id": 1, "a": "a", "b": "b", "c": "c"},
        {"id": 2, "a": "a", "b": "b", "c": "c"},
        {"id": 2, "a": "a", "b": "b", "c": "c"},
        {"id": 3, "a": "a", "b": "b", "c": "c"},
        {"id": 3, "a": "a", "b": "b", "c": "c"},
        {"id": 4, "a": "a", "b": "b", "c": "c"},
        {"id": 4, "a": "a", "b": "b", "c": "c"},
        {"id": 5, "a": "a", "b": "b", "c": "c"},
        {"id": 5, "a": "a", "b": "b", "c": "c"},
    ]
    pipe = (
        pipeline.Pipeline(input)
        .pipe(append_key("a"), name="append_a")
        .pipe(append_key("b"), name="append_b")
        .pipe(append_key("c"), name="append_c")
        .pipe(duplicate_items, name="duplicate")
    )
    result = list(pipe)
    assert result == expected


def test_pipeline_joined():

    input = [
        {"id": 1},
        {"id": 2},
        {"id": 3},
        {"id": 4},
        {"id": 5},
    ]
    expected = [
        {"id": 1, "a": "a", "b": "b", "c": "c"},
        {"id": 1, "a": "a", "b": "b", "c": "c"},
        {"id": 1, "a": "a", "b": "b", "c": "c"},
        {"id": 1, "a": "a", "b": "b", "c": "c"},
        {"id": 2, "a": "a", "b": "b", "c": "c"},
        {"id": 2, "a": "a", "b": "b", "c": "c"},
        {"id": 2, "a": "a", "b": "b", "c": "c"},
        {"id": 2, "a": "a", "b": "b", "c": "c"},
        {"id": 3, "a": "a", "b": "b", "c": "c"},
        {"id": 3, "a": "a", "b": "b", "c": "c"},
        {"id": 3, "a": "a", "b": "b", "c": "c"},
        {"id": 3, "a": "a", "b": "b", "c": "c"},
        {"id": 4, "a": "a", "b": "b", "c": "c"},
        {"id": 4, "a": "a", "b": "b", "c": "c"},
        {"id": 4, "a": "a", "b": "b", "c": "c"},
        {"id": 4, "a": "a", "b": "b", "c": "c"},
        {"id": 5, "a": "a", "b": "b", "c": "c"},
        {"id": 5, "a": "a", "b": "b", "c": "c"},
        {"id": 5, "a": "a", "b": "b", "c": "c"},
        {"id": 5, "a": "a", "b": "b", "c": "c"},
    ]
    pipeline_1 = (
        pipeline.Pipeline(input)
        .pipe(append_key("a"), name="append_a")
        .pipe(append_key("b"), name="append_b")
        .pipe(append_key("c"), name="append_c")
        .pipe(duplicate_items, name="duplicate")
    )
    pipeline_2 = (
        pipeline.Pipeline(pipeline_1)
        .pipe(append_key("a"), name="append_a")
        .pipe(append_key("b"), name="append_b")
        .pipe(append_key("c"), name="append_c")
        .pipe(duplicate_items, name="duplicate")
    )
    result = list(pipeline_2)
    assert result == expected
