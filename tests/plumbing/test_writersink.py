"""Test the writer sink."""
from __future__ import annotations

import json
import pathlib
import threading
from typing import Any, Dict, List

from wingline.plumbing import base
from wingline.plumbing.pipes import allpipe
from wingline.plumbing.sinks import writer
from wingline.types import AllProcess, PayloadIterable


def test_writersink(simple_data: List[Dict[str, Any]], tmp_path: pathlib.Path) -> None:
    """The writer sink works."""

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
    pipeline = base.Tap(simple_data, "test_tap")
    writer_sink = writer.WriterSink(pipeline, output_file, "test_iterator")

    writer_sink.start()
    assert writer_sink.started
    writer_sink.join()

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
    assert [json.loads(r) for r in result.strip().split("\n")] == [
        json.loads(r) for r in expected.strip().split("\n")
    ]


def test_failure_doesnt_write(
    simple_data: PayloadIterable, tmp_path: pathlib.Path, bad_all_process: AllProcess
) -> None:
    """If processing fails, the output file should not be written."""

    output_file = tmp_path / "test-writer-output.jl"
    tap = base.Tap(simple_data, "test_tap")
    bad_process = allpipe.AllPipe(tap, bad_all_process)
    writer_sink = writer.WriterSink(bad_process, output_file, "test_iterator")

    writer_sink.start()
    assert writer_sink.started
    writer_sink.join()

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
    assert len(output_files) == 0
    assert not output_file.exists()
