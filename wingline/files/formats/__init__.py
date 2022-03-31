"""Formats."""

import pathlib
from typing import Optional

from wingline.files.formats._base import Format
from wingline.files.formats.csv import Csv
from wingline.files.formats.json_lines import JsonLines
from wingline.files.formats.msgpack import Msgpack

_FORMAT_TYPES: set[type[Format]] = {
    JsonLines,
    Msgpack,
    Csv,
}

FORMATS: dict[str, type[Format]] = {
    format.mime_type: format for format in _FORMAT_TYPES
}

SUFFIX_MIME_TYPES = {
    suffix: format.mime_type for format in _FORMAT_TYPES for suffix in format.suffixes
}


def get_format_by_mime_type(mime_type: Optional[str]) -> type[Format]:
    """Return a format matching the mime type or the default."""

    mime_type = mime_type if mime_type else "<unrecognized>"
    try:
        format = FORMATS[mime_type]
    except KeyError:
        raise ValueError("Unsupported format: %s", mime_type)
    return format


def get_mime_type_by_path(path: pathlib.Path) -> Optional[str]:
    """Get the mimetype by extension."""

    for part in path.suffixes:
        mime_type = SUFFIX_MIME_TYPES.get(part)
        if mime_type:
            return mime_type


__all__ = [
    "Format",
    "JsonLines",
    "Msgpack",
    "get_format_by_mime_type",
]
