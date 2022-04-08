"""Test intermediate cacheing."""
# pylint: disable=redefined-outer-name

from unittest import mock

import pytest

import wingline
from wingline.cache import intermediate
from wingline.files import file


@pytest.fixture
def simple_input():
    return [
        {"a": 1, "b": 1, "c": 1},
        {"d": 1, "e": 1, "f": 1},
    ]


def test_cachepipe_writer(simple_input, func_add_one, tmp_path):
    """The cache pipes works."""

    cache_dir = tmp_path

    test_pipeline = wingline.Pipeline(simple_input, cache_dir=cache_dir)
    first_process = test_pipeline.process(func_add_one)
    second_process = first_process.process(func_add_one)

    result = list(second_process)
    assert len(result) == len(simple_input)

    first_expected = [{"a": 2, "b": 2, "c": 2}, {"d": 2, "e": 2, "f": 2}]
    first_cache_hash = first_process.at_node.hash
    assert first_cache_hash is not None
    first_cache_path = cache_dir / first_cache_hash[:2] / f"{first_cache_hash}.wingline"
    assert first_cache_path.exists()
    first_cache_file = file.File(first_cache_path, intermediate.filetype)
    first_result = list(first_cache_file)
    assert first_result == list(first_expected)

    second_expected = [{"a": 3, "b": 3, "c": 3}, {"d": 3, "e": 3, "f": 3}]
    second_cache_hash = second_process.at_node.hash
    assert second_cache_hash is not None
    second_cache_path = (
        cache_dir / second_cache_hash[:2] / f"{second_cache_hash}.wingline"
    )
    assert second_cache_path.exists()
    second_cache_file = file.File(second_cache_path, intermediate.filetype)
    second_result = list(second_cache_file)
    assert second_result == list(second_expected)


def test_cachepipe_reader(simple_input, func_add_one, tmp_path):
    """The cache pipes wors."""

    cache_dir = tmp_path

    test_pipeline = wingline.Pipeline(simple_input, cache_dir=cache_dir)
    first_process = test_pipeline.process(func_add_one)
    second_process = first_process.process(func_add_one)
    first_run_first_process_hash = first_process.at_node.hash
    first_run_second_process_hash = second_process.at_node.hash
    assert first_process.at_node.relationships.parent
    first_process_parent_callbacks = (
        first_process.at_node.relationships.parent.thread.callbacks
    )
    first_process_parent_callbacks.process = mock.MagicMock(
        side_effect=first_process_parent_callbacks.process
    )
    assert second_process.at_node.relationships.parent
    second_process_parent_callbacks = (
        second_process.at_node.relationships.parent.thread.callbacks
    )
    second_process_parent_callbacks.process = mock.MagicMock(
        side_effect=second_process_parent_callbacks.process
    )
    result = list(second_process)
    assert len(result) == len(simple_input)
    first_process_parent_callbacks.process.assert_called()
    second_process_parent_callbacks.process.assert_called()
    assert first_process.at_node.relationships.parent.started
    assert second_process.at_node.relationships.parent.started

    test_pipeline = wingline.Pipeline(simple_input, cache_dir=cache_dir)
    first_process = test_pipeline.process(func_add_one)
    second_process = first_process.process(func_add_one)
    second_run_first_process_hash = first_process.at_node.hash
    second_run_second_process_hash = second_process.at_node.hash
    assert first_process.at_node.relationships.parent
    assert second_process.at_node.relationships.parent
    first_process_parent_callbacks = (
        first_process.at_node.relationships.parent.thread.callbacks
    )
    first_process_parent_callbacks.process = mock.MagicMock(
        side_effect=first_process_parent_callbacks.process
    )
    second_process_parent_callbacks = (
        second_process.at_node.relationships.parent.thread.callbacks
    )
    second_process_parent_callbacks.process = mock.MagicMock(
        side_effect=second_process_parent_callbacks.process
    )
    result = list(second_process)
    assert len(result) == len(simple_input)
    first_process_parent_callbacks.process.assert_not_called()
    second_process_parent_callbacks.process.assert_not_called()
    assert first_run_first_process_hash == second_run_first_process_hash
    assert first_run_second_process_hash == second_run_second_process_hash
    assert not first_process.at_node.relationships.parent.started
    assert not second_process.at_node.relationships.parent.started
