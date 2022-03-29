"""Detect filetype."""

import pathlib
from typing import BinaryIO

import filetype

from wingline.files import containers, formats, reader

# https://github.com/h2non/filetype.py/blame/master/README.rst#L19
HEADER_SIZE = 261


def detect_container(path: pathlib.Path) -> type[containers.Container]:
    """Detect the container of a file"""

    container_type = filetype.archive_match(path)
    container_mime_type = container_type.mime if container_type else None
    return containers.get_container_by_mime_type(container_mime_type)


def get_container(path: pathlib.Path) -> containers.Container:
    """Detect the container of a file"""

    return detect_container(path)(path)


def detect_format(container: containers.Container) -> type[formats.Format]:
    """Detect the format of an open file object."""

    with container.handle() as handle:
        format_type = filetype.guess(handle)
    if format_type:
        format_mime_type = format_type.mime if format_type else None
    else:
        format_mime_type = formats.get_mime_type_by_path(container.path)
    format = formats.get_format_by_mime_type(format_mime_type)
    if not format:
        raise ValueError("Couldn't determine format.")
    return format


def get_reader(path: pathlib.Path) -> reader.Reader:
    """Get a reader for the file."""

    return reader.Reader(path)
