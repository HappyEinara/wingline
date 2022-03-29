"""Test intermediate caching."""
import logging

from pytest_cases import parametrize_with_cases

from wingline.plumbing import Pipeline, file
from wingline.types import Payload, PayloadIterable, PayloadIterator

logger = logging.getLogger(__name__)


def add_a(parent: PayloadIterable) -> PayloadIterator:
    for payload in parent:
        payload["_a"] = "a"
        yield payload


@parametrize_with_cases(
    "path,content_hash,container,format,item_count", cases="tests.cases.files"
)
def test_intermediate_hash(
    path, content_hash, container, format, item_count, testing_cache
):
    """A no-op pipeline has the same hash as its source file."""

    test_pipe = Pipeline(file.File(path))
    assert test_pipe.hash == content_hash


@parametrize_with_cases(
    "path,content_hash,container,format,item_count", cases="tests.cases.files"
)
def test_intermediate_caching(
    path, content_hash, container, format, item_count, testing_cache, tmp_path
):
    """Test that file hashing is correct."""

    # First run a single pipeline in a clean directory with a single op.
    test_pipe = Pipeline(file.File(path), add_a, cache_dir=tmp_path)
    pipe_result: list[Payload] = list(test_pipe)
    assert len(pipe_result) == item_count

    # Check exactly one file exists
    cache_files = list(tmp_path.glob("**/*.wingline"))
    assert len(cache_files) == 1
    cache_file = cache_files[0]

    # Read in the cache file
    cache_pipe = file.File(cache_file)
    cache_result = list(cache_pipe)
    assert len(cache_result) == item_count

    # Check that we've added the keys on to every row.
    assert [i["_a"] for i in cache_result] == ["a"] * item_count
    assert pipe_result == cache_result


@parametrize_with_cases(
    "path,content_hash,container,format,item_count", cases="tests.cases.files"
)
def test_cache_file_used(
    path, content_hash, container, format, item_count, testing_cache, tmp_path
):
    """Test that file hashing is correct."""

    # First run a single pipeline in a clean directory with a single op.

    test_pipe = Pipeline(file.File(path), add_a, cache_dir=tmp_path)
    pipe_result = list(test_pipe)
    assert len(pipe_result) == item_count

    # Check exactly one file exists
    cache_files = list(tmp_path.glob("**/*.wingline"))
    assert len(cache_files) == 1
    cache_file = cache_files[0]

    import threading

    logger.debug("=========================")
    logger.debug(threading.enumerate())
    logger.debug("=========================")
    # Run the same file again.
    test_pipe = Pipeline(file.File(path), add_a, cache_dir=tmp_path)
    pipe_result = list(test_pipe)
    assert len(pipe_result) == item_count
