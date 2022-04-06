"""Test the Filetap source."""

import pathlib
import threading

import pytest
import pytest_cases

from wingline import pipeline
from wingline.files import file
from wingline.plumbing.sinks import iteratorsink
from wingline.plumbing.taps import filetap


@pytest_cases.parametrize_with_cases(
    "path,container_type,format_type,line_count,expect_automatic_detection"
)
def test_filetap(
    path, container_type, format_type, line_count, expect_automatic_detection
):

    if expect_automatic_detection:
        test_file = file.File(path)
    else:
        test_file = file.File(path, format_type(), container_type())
    tap = filetap.FileTap(test_file, "test_file_tap")
    pipe_iterator = iteratorsink.IteratorSink(tap, "test_iterator_sink")

    result = list(pipe_iterator)

    assert len(result) == line_count

    # There should only be the main thread left
    # Check the threading module's reports are
    # consistent with that.
    assert threading.active_count() == 1
    assert (
        threading.enumerate()[0]
        == threading.main_thread()
        == threading.current_thread()
    )


@pytest_cases.parametrize_with_cases(
    "path,container_type,format_type,line_count,expect_automatic_detection"
)
def test_pipeline_with_path(
    path,
    container_type,
    format_type,
    line_count,
    expect_automatic_detection,
    func_add_one,
):
    """Pipelines work both with path and File as sources."""

    test_file_pipeline = pipeline.Pipeline(file.File(path))
    file_result = list(test_file_pipeline)
    test_path_pipeline = pipeline.Pipeline(path)
    path_result = list(test_path_pipeline)
    assert file_result == path_result
    assert len(file_result) == line_count


def test_bad_file(unrecognizable_file):
    """An unrecognizable file raises."""

    with pytest.raises(ValueError):
        test_file = file.File(unrecognizable_file)
        list(test_file)


def test_nonexistent_file():
    """Output of properties of nonexistent file are None."""

    missing = file.File(pathlib.Path("deliberately-nonexistent-path.jsonl.gzip"))
    assert not missing.exists
    assert missing.size is None
    assert missing.stat is None
    assert missing.modified_at is None
    with pytest.raises(RuntimeError):
        missing.reader()


@pytest_cases.parametrize_with_cases(
    "path,container_type,format_type,line_count,expect_automatic_detection,size"
)
def test_filesize(
    path, container_type, format_type, line_count, expect_automatic_detection, size
):

    modified = path.stat().st_mtime_ns
    test_file = file.File(path)
    assert test_file.size == size
    assert test_file.modified_at == modified


def test_write_exists(tmp_path):
    """Attempting to write to an existent file will fail."""

    existent = tmp_path / "existant.jsonl.gzip"
    existent.touch()

    test_file = file.File(existent)
    with pytest.raises(RuntimeError):
        test_file.writer()
