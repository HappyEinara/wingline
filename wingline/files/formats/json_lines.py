"""The JSONline adapter."""

import contextlib
import logging
from typing import Any, BinaryIO, Callable, Generator

from wingline.files.formats import _base
from wingline.json import json
from wingline.types import Payload, PayloadIterator

logger = logging.getLogger(__name__)


class JsonLines(_base.Format):
    """JSONlines format."""

    suffixes = [".jsonl", ".jl", ".json"]

    @contextlib.contextmanager
    def read(
        self, handle: BinaryIO, **default_kwargs: Any
    ) -> Generator[Callable[..., PayloadIterator], None, None]:
        """Dict iterator."""

        def _read(**kwargs: Any) -> PayloadIterator:
            """Innermost reader."""

            # When py3.8 reaches EOL:
            # payload_kwargs = kwargs | payload_kwargs
            kwargs = {**default_kwargs, **kwargs}
            for line in handle:
                yield json.loads(line, **kwargs)

        yield _read

    @contextlib.contextmanager
    def write(
        self, handle: BinaryIO, **default_kwargs: Any
    ) -> Generator[Callable[..., None], None, None]:
        """Writer."""

        def _write(payload: Payload, **kwargs: Any) -> None:
            """Innermost writer."""

            # When py3.8 reaches EOL:
            # kwargs = default_kwargs | kwargs
            kwargs = {**default_kwargs, **kwargs}
            try:
                output = json.dumps(payload, **kwargs)
            except TypeError as exc:
                logger.error("Bad payload: %s", str(payload))
                logger.exception(exc)
                raise TypeError(f"Couldn't encode payload with {json}") from exc
            handle.write((output + "\n").encode("utf-8"))

        yield _write
