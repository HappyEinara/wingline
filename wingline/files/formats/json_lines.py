"""The JSONline adapter."""

import contextlib
from typing import Any, BinaryIO, Callable, Generator

from wingline.files.formats import _base
from wingline.json import json
from wingline.types import Payload, PayloadIterator


class JsonLines(_base.Format):
    """JSONlines format."""

    mime_type = "application/json"
    suffixes = [".jsonl", ".jl", ".json"]

    @contextlib.contextmanager
    def read(
        self, fp: BinaryIO, **default_kwargs: Any
    ) -> Generator[Callable[..., PayloadIterator], None, None]:
        """Dict iterator."""

        def _read(**kwargs: Any) -> PayloadIterator:
            """Innermost reader."""

            # When py3.8 reaches EOL:
            # payload_kwargs = kwargs | payload_kwargs
            kwargs = {**default_kwargs, **kwargs}
            for line in fp:
                yield json.loads(line, **kwargs)

        yield _read

    @contextlib.contextmanager
    def write(
        self, fp: BinaryIO, **default_kwargs: Any
    ) -> Generator[Callable[..., None], None, None]:
        """Writer."""

        default_kwargs.setdefault("sort_keys", True)

        def _write(payload: Payload, **kwargs: Any) -> None:
            """Innermost writer."""

            # When py3.8 reaches EOL:
            # kwargs = default_kwargs | kwargs
            kwargs = {**default_kwargs, **kwargs}
            fp.write((json.dumps(payload, **kwargs) + "\n").encode("utf-8"))

        yield _write
