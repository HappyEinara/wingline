"""Test intermediate cacheing."""
from unittest import mock

import pytest

import wingline
from wingline.files import file


@pytest.fixture
def input():
    return [
        {"a": 1, "b": 1, "c": 1},
        {"d": 1, "e": 1, "f": 1},
    ]

def test_cachepipe_writer(input, func_add_one, tmp_path):
    """The cache pipes wors."""

    cache_dir = tmp_path

    test_pipeline = wingline.Pipeline(input, cache_dir=cache_dir)
    first_process = test_pipeline.process(func_add_one)
    second_process = first_process.process(func_add_one)

    result = list(second_process)
    assert len(result) == len(input)


    first_expected = [{'a': 2, 'b': 2, 'c': 2}, {'d': 2, 'e': 2, 'f': 2}]
    first_cache_hash = first_process.at.hash
    assert first_cache_hash is not None
    first_cache_path = cache_dir / first_cache_hash[:2] / f"{first_cache_hash}.wingline"
    assert first_cache_path.exists()
    first_cache_file = file.File(first_cache_path)
    first_result = list(first_cache_file)
    assert first_result == list(first_expected)

    second_expected = [{'a': 3, 'b': 3, 'c': 3}, {'d': 3, 'e': 3, 'f': 3}]
    second_cache_hash = second_process.at.hash
    assert second_cache_hash is not None
    second_cache_path = cache_dir / second_cache_hash[:2] / f"{second_cache_hash}.wingline"
    assert second_cache_path.exists()
    second_cache_file = file.File(second_cache_path)
    second_result = list(second_cache_file)
    assert second_result == list(second_expected)

def test_cachepipe_reader(input, func_add_one, tmp_path):
    """The cache pipes wors."""

    cache_dir = tmp_path

    test_pipeline = wingline.Pipeline(input, cache_dir=cache_dir)
    first_process = test_pipeline.process(func_add_one)
    second_process = first_process.process(func_add_one)
    first_run_first_process_hash = first_process.at.hash
    first_run_second_process_hash = second_process.at.hash
    first_process.at.parent.thread.process = mock.MagicMock(side_effect=first_process.at.parent.thread.process)
    second_process.at.parent.thread.process = mock.MagicMock(side_effect=second_process.at.parent.thread.process)
    result = list(second_process)
    assert len(result) == len(input)
    first_process.at.parent.thread.process.assert_called()
    second_process.at.parent.thread.process.assert_called()
    assert first_process.at.parent.started
    assert second_process.at.parent.started

    test_pipeline = wingline.Pipeline(input, cache_dir=cache_dir)
    first_process = test_pipeline.process(func_add_one)
    second_process = first_process.process(func_add_one)
    second_run_first_process_hash = first_process.at.hash
    second_run_second_process_hash = second_process.at.hash
    first_process.at.parent.thread.process = mock.MagicMock(side_effect=first_process.at.parent.thread.process)
    second_process.at.parent.thread.process = mock.MagicMock(side_effect=second_process.at.parent.thread.process)
    result = list(second_process)
    assert len(result) == len(input)
    first_process.at.parent.thread.process.assert_not_called()
    second_process.at.parent.thread.process.assert_not_called()
    assert first_run_first_process_hash == second_run_first_process_hash
    assert first_run_second_process_hash == second_run_second_process_hash
    assert not first_process.at.parent.started
    assert not second_process.at.parent.started
