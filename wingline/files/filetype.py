"""Detect filetype."""

import pathlib
from typing import BinaryIO, Container, Optional

import filetype

from wingline.files import containers, formats, reader, writer

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
        format = formats.get_format_by_mime_type(format_mime_type)
    else:
        _, format = get_filetypes_by_path(container.path)
    if not format:
        raise ValueError("Couldn't determine format.")
    return format


def get_filetypes_by_path(
    path: pathlib.Path,
) -> tuple[type[containers.Container], type[formats.Format]]:
    """Infer container and format types based on the path suffixes."""

    container = containers.get_container_by_suffix(path.suffix)
    if container:
        suffix = path.suffixes[-2]
        format = formats.get_format_by_suffix(suffix)
        if not format:
            raise ValueError(f"Couldn't determine file types for path {path} (got container {container}, but nothing for format suffix {suffix})")
    else:
        container = containers.DEFAULT_CONTAINER
        format = formats.get_format_by_suffix(path.suffix)
        if not format:
            raise ValueError(f"Couldn't determine file types for path {path}")

    return container, format


def get_reader(path: pathlib.Path) -> reader.Reader:
    """Get a reader for the file."""

    return reader.Reader(path)


def get_writer(
    path: pathlib.Path,
    format: Optional[type[formats.Format]] = None,
    container: Optional[type[containers.Container]] = None,
) -> writer.Writer:
    """Get a reader for the file."""

    return writer.Writer(path, format=format, container=container)
