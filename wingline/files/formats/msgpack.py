"""The MessagePack adapter."""

import contextlib
from typing import Any, BinaryIO, Callable, Generator

import msgpack

from wingline.files.formats import _base
from wingline.types import Payload, PayloadIterator


class Msgpack(_base.Format):
    """Msgpack format."""

    mime_type = "application/x-msgpack"
    suffixes = [".msgpack", ".wingline"]

    @contextlib.contextmanager
    def read(
        self, fp: BinaryIO, **kwargs: Any
    ) -> Generator[Callable[..., PayloadIterator], None, None]:
        """Dict iterator."""

        kwargs.setdefault("strict_map_key", False)

        unpacker = msgpack.Unpacker(fp)

        def _read() -> PayloadIterator:
            """Innermost reader."""

            for item in unpacker:
                yield item

        yield _read

    @contextlib.contextmanager
    def write(
        self, fp: BinaryIO, **kwargs: Any
    ) -> Generator[Callable[..., None], None, None]:
        """Writer."""

        def _write(payload: Payload) -> None:
            msg = msgpack.packb(payload)
            fp.write(msg)

        yield _write
