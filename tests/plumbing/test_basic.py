"""Test the basic tap-pipe-iter elements."""

import threading

from wingline.plumbing import tap
from wingline.plumbing.sinks import iteratorsink


def test_basic():

    input = [{"id": i} for i in range(10)]

    pipeline = tap.Tap(input, "test_tap")
    iter = iteratorsink.IteratorSink(pipeline, "test_iterator")

    result = list(iter)

    assert result == input
    breakpoint()

    # There should only be the main thread left
    # Check the threading module's reports are
    # consistent with that.
    assert threading.active_count() == 1
    assert (
        threading.enumerate()[0]
        == threading.main_thread()
        == threading.current_thread()
    )
