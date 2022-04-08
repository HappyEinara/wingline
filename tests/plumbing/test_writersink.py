"""Test the writer sink."""
from __future__ import annotations

import threading

from wingline.plumbing import tap
from wingline.plumbing.sinks import writersink


def test_writersink(simple_data, tmp_path):

    expected = (
        "\n".join(
            (
                '{"first_aired": "1963", "name": "Doctor Who"}',
                '{"first_aired": "2001", "name": "24"}',
                '{"first_aired": "1999", "name": "The Sopranos"}',
            )
        )
        + "\n"
    )

    output_file = tmp_path / "test-writer-output.jl"
    pipeline = tap.Tap(simple_data, "test_tap")
    writer = writersink.WriterSink(pipeline, output_file, "test_iterator")

    writer.start()
    assert writer.started
    writer.join()

    # There should only be the main thread left
    # Check the threading module's reports are
    # consistent with that.
    assert threading.active_count() == 1
    assert (
        threading.enumerate()[0]
        == threading.main_thread()
        == threading.current_thread()
    )

    output_files = list(tmp_path.glob("*"))
    assert len(output_files) == 1

    result = output_files[0].read_text()
    assert result == expected
