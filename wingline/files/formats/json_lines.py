"""The JSONline adapter."""

from typing import Any, BinaryIO, Iterable

from wingline.files.formats import _base
from wingline.json import json
from wingline.types import Payload


class JsonLines(_base.Format):
    """JSONlines format."""

    mime_type = "application/json"
    suffixes = {".json", ".jl", ".jsonl"}

    def read(self, handle: BinaryIO) -> Iterable[dict[str, Any]]:
        """Dict iterator."""

        for line in handle:
            yield json.loads(line)

    def write(self, handle: BinaryIO, payload: Payload) -> None:
        """Writer."""

        handle.write(json.dumps(payload, default=str, sort_keys=True).encode("utf-8"))
