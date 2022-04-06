"""File hasher."""

import collections.abc
import hashlib
import pathlib
from typing import Any, Callable

import dill as pickle  # type: ignore # nosec B403

DIGEST_SIZE = 8
HASH_BLOCK_SIZE = 4096


def hasher(data: bytes = b"", digest_size: int = 8, **kwargs: Any) -> Any:
    return hashlib.blake2b(data, digest_size=digest_size, **kwargs)


def hash_file(path: pathlib.Path) -> str:
    """Hash a file."""

    file_hash = hasher()
    with path.open("rb") as handle:
        while chunk := handle.read(HASH_BLOCK_SIZE):
            file_hash.update(chunk)
    return file_hash.hexdigest()


def _hash_object(obj: object) -> str:
    """Hash an arbitrary python object.

    Internal function to DRY the more specific public functions.
    """

    obj_pickle = pickle.dumps(obj)
    obj_hash = hasher(obj_pickle).hexdigest()
    return obj_hash


def hash_sequence(sequence: collections.abc.Sequence[Any]) -> str:
    """Hash a sequence."""

    return _hash_object(sequence)


def hash_callable(callable: Callable[..., Any]) -> str:
    """Hash a callable."""

    return _hash_object(callable)


def hash_string(input: str) -> str:
    """Hash a string."""

    return hasher(input.encode("utf-8")).hexdigest()
