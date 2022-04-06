"""Format base class"""

import abc
import contextlib
from typing import Any, BinaryIO, Callable, Generator, Optional

from wingline.types import PayloadIterator


class Format(abc.ABC):
    """Base class for a file format."""

    suffixes: list[str]

    def __init__(
        self,
        read_kwargs: Optional[dict[str, Any]] = None,
        write_kwargs: Optional[dict[str, Any]] = None,
    ):
        self._read_kwargs = read_kwargs if read_kwargs is not None else {}
        self._write_kwargs = write_kwargs if write_kwargs is not None else {}

    @contextlib.contextmanager
    def read_manager(
        self, fp: BinaryIO, **kwargs: Any
    ) -> Generator[Callable[..., PayloadIterator], None, None]:
        """Return a Payload iterator for reading.

        This is the public interface which merges kwargs first.
        """

        # When py3.8 reaches EOL:
        # kwargs = self._read_kwargs | kwargs
        kwargs = {**self._read_kwargs, **kwargs}
        with self.read(fp, **kwargs) as reader:
            yield reader

    @contextlib.contextmanager
    def write_manager(
        self, fp: BinaryIO, **kwargs: Any
    ) -> Generator[Callable[..., None], None, None]:
        """Write a payload

        This is the public interface which merges kwargs first.
        """

        # When py3.8 reaches EOL:
        # kwargs = self._write_kwargs | kwargs
        kwargs = {**self._write_kwargs, **kwargs}
        with self.write(fp, **kwargs) as writer:
            yield writer

    @abc.abstractmethod
    @contextlib.contextmanager
    def read(
        self, fp: BinaryIO, **kwargs: Any
    ) -> Generator[Callable[..., PayloadIterator], None, None]:
        """Yields dicts from a file handle."""

    @abc.abstractmethod
    @contextlib.contextmanager
    def write(
        self, fp: BinaryIO, **kwargs: Any
    ) -> Generator[Callable[..., None], None, None]:
        """Writes payload dicts to a file handle."""
