"""The CSV adapter."""

import contextlib
import csv
import io
from typing import Any, BinaryIO, Callable, Generator

from wingline.files.formats import _base
from wingline.types import Payload, PayloadIterator


class Csv(_base.Format):
    """CSV format."""

    suffixes = [".csv"]

    @contextlib.contextmanager
    def read(
        self,
        fp: BinaryIO,
        **kwargs: Any,
    ) -> Generator[Callable[..., PayloadIterator], None, None]:
        """Dict iterator"""

        kwargs = {
            **{
                "fieldnames": None,
                "restkey": "(rest)",
                "restval": None,
                "dialect": "excel",
            },
            **kwargs,
        }

        text_handle = io.TextIOWrapper(fp, encoding="utf-8")
        reader = csv.DictReader(text_handle, **kwargs)

        def _read() -> PayloadIterator:
            """Innermost reader."""

            for line in reader:
                yield line

        yield _read

    @contextlib.contextmanager
    def write(
        self, fp: BinaryIO, **kwargs: Any
    ) -> Generator[Callable[..., None], None, None]:
        """File writer."""

        kwargs = {
            **{
                "fieldnames": None,
                "restval": None,
                "extrasaction": "ignore",
                "dialect": "excel",
            },
            **kwargs,
        }

        text_handle = io.TextIOWrapper(fp, encoding="utf-8")
        fieldnames = kwargs.pop("fieldnames")
        state = {
            "writer_initialized": False,
            "fieldnames": fieldnames,
            "kwargs": kwargs,
            "writer": None,
        }

        def _write(payload: Payload) -> None:
            """Innermost reader."""
            writer = state["writer"]
            if writer is None:
                fieldnames = state.get("fieldnames") or payload.keys()
                writer = csv.DictWriter(text_handle, fieldnames, **state["kwargs"])
                writer.writeheader()
                state["writer"] = writer

            writer.writerow(payload)
            text_handle.flush()

        yield _write
