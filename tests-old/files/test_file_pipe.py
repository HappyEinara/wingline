"""Test pipe with file as input."""

from pytest_cases import parametrize_with_cases

from wingline.plumbing import Pipeline, file
from wingline.types import PayloadIterable


def add_a(parent: PayloadIterable) -> PayloadIterable:
    for payload in parent:
        payload["_a"] = "a"
        yield payload


def add_b(parent: PayloadIterable) -> PayloadIterable:
    for payload in parent:
        payload["_b"] = "b"
        yield payload


@parametrize_with_cases(
    "path,content_hash,container,format,item_count", cases="tests.cases.files"
)
def test_cached_file_line_count(
    path, content_hash, container, format, item_count, testing_cache
):
    """Test that file hashing is correct."""

    test_pipe = Pipeline(file.File(path), add_a, add_b)
    result = list(test_pipe)
    assert len(result) == item_count

    # Check that we've added the keys on to every row.
    assert [i["_a"] for i in result] == ["a"] * item_count
    assert [i["_b"] for i in result] == ["b"] * item_count
