"""Container base class"""
from __future__ import annotations

import abc
import contextlib
import pathlib
from typing import Any, BinaryIO, Generator, Optional


class Container(abc.ABC):
    """Base class for a file container."""

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
        self, path: pathlib.Path, **kwargs: Any
    ) -> Generator[BinaryIO, None, None]:
        """Return a file handle for reading.

        This is the public interface.
        """

        # When py3.8 reaches EOL:
        # kwargs = self._read_kwargs | kwargs
        kwargs = {**self._read_kwargs, **kwargs}
        with self.read(path, **kwargs) as fp:
            yield fp

    @contextlib.contextmanager
    def write_manager(
        self, path: pathlib.Path, **kwargs: Any
    ) -> Generator[BinaryIO, None, None]:
        """Return a file handle for writing.

        This is the public interface, which creates any
        parent directories necessary.
        """

        # When py3.8 reaches EOL:
        # kwargs = self._write_kwargs | kwargs
        kwargs = {**self._write_kwargs, **kwargs}

        path.parent.mkdir(parents=True, exist_ok=True)
        with self.write(path, **kwargs) as fp:
            yield fp

    @staticmethod
    @contextlib.contextmanager
    @abc.abstractmethod
    def read(path: pathlib.Path, **kwargs: Any) -> Generator[BinaryIO, None, None]:
        """Return a file handle for reading.

        This is the abstract method that should be overridden by
        concrete container implementations.
        """

    @staticmethod
    @contextlib.contextmanager
    @abc.abstractmethod
    def write(path: pathlib.Path, **kwargs: Any) -> Generator[BinaryIO, None, None]:
        """Return a file handle for writing.

        This is the abstract method that should be overridden by
        concrete container implementations.
        """
