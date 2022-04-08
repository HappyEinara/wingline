"""The MessagePack adapter."""
# pylint: disable=duplicate-code

import contextlib
from typing import Any, BinaryIO, Callable, Generator

import msgpack

from wingline.files.formats import _base
from wingline.types import Payload, PayloadIterator


class Msgpack(_base.Format):
    """Msgpack format."""

    suffixes = [".msgpack", ".wingline"]

    @contextlib.contextmanager
    def read(
        self, handle: BinaryIO, **kwargs: Any
    ) -> Generator[Callable[..., PayloadIterator], None, None]:
        """Dict iterator."""

        kwargs.setdefault("strict_map_key", False)

        unpacker = msgpack.Unpacker(handle)

        def _read() -> PayloadIterator:
            """Innermost reader."""

            for item in unpacker:
                yield item

        yield _read

    @contextlib.contextmanager
    def write(
        self, handle: BinaryIO, **kwargs: Any
    ) -> Generator[Callable[..., None], None, None]:
        """Writer."""

        def _write(payload: Payload) -> None:
            msg = msgpack.packb(payload, **kwargs)
            handle.write(msg)

        yield _write
