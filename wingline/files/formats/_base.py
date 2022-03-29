"""Format base class"""

import abc
from typing import Any, BinaryIO, Iterable, Iterator

from wingline.types import Payload


class Format(metaclass=abc.ABCMeta):
    """Base class for a file format."""

    mime_type: str
    suffixes: Iterable[str] = set()

    def __init__(self, handle: BinaryIO):
        self._handle = handle

    @property
    def reader(self) -> Iterator[dict[str, Any]]:
        """Reader property"""

        return self.read(self._handle)

    def writer(self, payload: Payload) -> None:
        """Writer property"""

        self.write(self._handle, payload)

    @abc.abstractmethod
    def read(self, handle: BinaryIO) -> Iterator[dict[str, Any]]:
        """Yields dicts from a file handle."""

        raise NotImplementedError

    @abc.abstractmethod
    def write(self, handle: BinaryIO, payload: Payload) -> None:
        """Writes a payload dict to a file handle."""

        raise NotImplementedError
