"""The MessagePack adapter."""
# pylint: disable=duplicate-code

import contextlib
import datetime
from decimal import Decimal
from typing import Any, BinaryIO, Callable, Generator

import msgpack

from wingline.files.formats import _base
from wingline.types import Payload, PayloadIterator


def recover(obj: Any) -> Any:
    """Try to recover from a TypeError."""

    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, datetime.datetime):
        return obj.isoformat()
    if isinstance(obj, dict):
        return {k: recover(v) for k, v in obj.items()}
    if isinstance(obj, (list, set)):
        return [recover(v) for v in obj]
    return obj


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
            try:
                msg = msgpack.packb(payload, **kwargs)
            except TypeError:
                payload = recover(payload)
                msg = msgpack.packb(payload, **kwargs)
            handle.write(msg)

        yield _write
