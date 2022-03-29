"""File hasher."""

import hashlib
import pathlib
from typing import Any, Callable

import dill as pickle  # nosec B403

DIGEST_SIZE = 8
HASH_BLOCK_SIZE = 4096


def hasher(data: bytes = b"", digest_size=8, **kwargs):
    return hashlib.blake2b(data, digest_size=digest_size, **kwargs)


def hash_file(path: pathlib.Path) -> str:
    """Hash a file."""

    file_hash = hasher()
    with path.open("rb") as handle:
        while chunk := handle.read(HASH_BLOCK_SIZE):
            file_hash.update(chunk)
    return file_hash.hexdigest()


def hash_callable(callable: Callable[..., Any]) -> str:
    """Hash a callable."""

    callable_pickle = pickle.dumps(callable)
    callable_hash = hasher(callable_pickle).hexdigest()
    return callable_hash
