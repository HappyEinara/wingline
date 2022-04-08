"""Format base class"""
# pylint: disable=duplicate-code

from __future__ import annotations

import abc
import contextlib
from typing import Any, BinaryIO, Callable, Dict, Generator, List, Optional

from wingline.types import PayloadIterator, ReadPointer, WritePointer


class Format(abc.ABC):
    """Base class for a file format."""

    suffixes: List[str]

    def __init__(
        self,
        read_kwargs: Optional[Dict[str, Any]] = None,
        write_kwargs: Optional[Dict[str, Any]] = None,
    ):
        self._read_kwargs = read_kwargs if read_kwargs is not None else {}
        self._write_kwargs = write_kwargs if write_kwargs is not None else {}

    @contextlib.contextmanager
    def read_manager(
        self, handle: BinaryIO, **kwargs: Any
    ) -> Generator[ReadPointer, None, None]:
        """Return a Payload iterator for reading.

        This is the public interface which merges kwargs first.
        """

        # When py3.8 reaches EOL:
        # kwargs = self._read_kwargs | kwargs
        kwargs = {**self._read_kwargs, **kwargs}
        with self.read(handle, **kwargs) as reader:
            yield reader

    @contextlib.contextmanager
    def write_manager(
        self, handle: BinaryIO, **kwargs: Any
    ) -> Generator[WritePointer, None, None]:
        """Write a payload

        This is the public interface which merges kwargs first.
        """

        # When py3.8 reaches EOL:
        # kwargs = self._write_kwargs | kwargs
        kwargs = {**self._write_kwargs, **kwargs}
        with self.write(handle, **kwargs) as writer:
            yield writer

    @abc.abstractmethod
    @contextlib.contextmanager
    def read(
        self, handle: BinaryIO, **kwargs: Any
    ) -> Generator[Callable[..., PayloadIterator], None, None]:
        """Yields dicts from a file handle."""

    @abc.abstractmethod
    @contextlib.contextmanager
    def write(
        self, handle: BinaryIO, **kwargs: Any
    ) -> Generator[Callable[..., None], None, None]:
        """Writes payload dicts to a file handle."""
