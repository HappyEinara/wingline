"""Container formats."""
from typing import Optional

from wingline.files.containers._base import DEFAULT_CONTAINER_MIME_TYPE, Container
from wingline.files.containers.gzip import Gzip
from wingline.files.containers.zip import Zip

_CONTAINER_TYPES: set[type[Container]] = {Container, Gzip, Zip}


CONTAINERS_BY_MIME_TYPE: dict[str, type[Container]] = {
    container.mime_type: container for container in _CONTAINER_TYPES
}

CONTAINERS_BY_NAME: dict[str, type[Container]] = {
    container.__name__: container for container in _CONTAINER_TYPES
}

CONTAINERS_BY_SUFFIX: dict[str, type[Container]] = {
    suffix: container for container in _CONTAINER_TYPES for suffix in container.suffixes
}

DEFAULT_CONTAINER = CONTAINERS_BY_MIME_TYPE[DEFAULT_CONTAINER_MIME_TYPE]


def get_container_by_mime_type(mime_type: Optional[str]) -> type[Container]:
    """Return a container for matching the mime type or the default."""

    if not mime_type:
        return DEFAULT_CONTAINER
    return CONTAINERS_BY_MIME_TYPE.get(mime_type, DEFAULT_CONTAINER)


def get_container_by_suffix(suffix: str) -> Optional[type[Container]]:
    """Return a container for a path suffix"""

    # Don't return the default here; calling code
    # may need None to know if the suffix has been consumed
    # or not.
    return CONTAINERS_BY_SUFFIX.get(suffix)


__all__ = [
    "Container",
    "Gzip",
    "get_container_by_mime_type",
]
