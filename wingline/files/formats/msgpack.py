"""The MessagePack adapter."""

from typing import Any, BinaryIO, Iterable

import msgpack

from wingline.files.formats import _base
from wingline.types import Payload


class Msgpack(_base.Format):
    """Msgpack format."""

    mime_type = "application/x-msgpack"
    suffixes = {".wingline", ".msgpack"}

    def read(self, handle: BinaryIO) -> Iterable[dict[str, Any]]:
        """Dict iterator."""

        unpacker = msgpack.Unpacker(handle)
        for item in unpacker:
            yield item

    def write(self, handle: BinaryIO, payload: Payload) -> None:
        """Writer."""

        handle.write(msgpack.packb(payload))
