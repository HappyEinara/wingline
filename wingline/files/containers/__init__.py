"""Container formats."""
from typing import Optional

from wingline.files.containers._base import DEFAULT_CONTAINER_MIME_TYPE, Container
from wingline.files.containers.gzip import Gzip
from wingline.files.containers.zip import Zip

_CONTAINER_TYPES: set[type[Container]] = {Container, Gzip, Zip}


CONTAINERS: dict[str, type[Container]] = {
    container.mime_type: container for container in _CONTAINER_TYPES
}

DEFAULT_CONTAINER = CONTAINERS[DEFAULT_CONTAINER_MIME_TYPE]


def get_container_by_mime_type(mime_type: Optional[str]) -> type[Container]:
    """Return a container for matching the mime type or the default."""

    if not mime_type:
        return DEFAULT_CONTAINER
    return CONTAINERS.get(mime_type, DEFAULT_CONTAINER)


__all__ = [
    "Container",
    "Gzip",
    "get_container_by_mime_type",
]
