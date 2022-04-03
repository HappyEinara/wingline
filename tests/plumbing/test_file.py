"""Test the Filetap source."""

import threading

import pytest_cases

from wingline.files import file
from wingline.plumbing.sinks import iteratorsink
from wingline.plumbing.taps import filetap


@pytest_cases.parametrize_with_cases("path,line_count")
def test_filetap(path, line_count):

    test_file = file.File(path)
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
