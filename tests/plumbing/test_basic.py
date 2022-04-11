"""Test the basic tap-pipe-iter elements."""

import threading

from wingline.plumbing import base
from wingline.plumbing.sinks import iterator


def test_basic() -> None:
    """Test a very basic pair of pipes."""

    pipe_input = [{"id": i} for i in range(10)]

    pipeline = base.Tap(pipe_input, "test_tap")
    itersink = iterator.IteratorSink(pipeline, "test_iterator")

    result = list(itersink)

    assert result == pipe_input

    # There should only be the main thread left
    # Check the threading module's reports are
    # consistent with that.
    assert threading.active_count() == 1
    assert (
        threading.enumerate()[0]
        == threading.main_thread()
        == threading.current_thread()
    )
