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

FORMATS_BY_MIME_TYPE: dict[str, type[Format]] = {
    format.mime_type: format for format in _FORMAT_TYPES
}

FORMATS_BY_NAME: dict[str, type[Format]] = {
    format.__name__: format for format in _FORMAT_TYPES
}

FORMATS_BY_SUFFIX: dict[str, type[Format]] = {
    suffix: format for format in _FORMAT_TYPES for suffix in format.suffixes
}


def get_format_by_mime_type(mime_type: Optional[str]) -> type[Format]:
    """Return a format matching the mime type or the default."""

    mime_type = mime_type if mime_type else "<unrecognized>"
    try:
        format = FORMATS_BY_MIME_TYPE[mime_type]
    except KeyError:
        raise ValueError("Unsupported format: %s", mime_type)
    return format


def get_format_by_suffix(suffix: str) -> Optional[type[Format]]:
    """Return a format for a path suffix"""

    # Don't return the default here; calling code
    # may need None to know if the suffix has been consumed
    # or not.
    return FORMATS_BY_SUFFIX.get(suffix)


__all__ = [
    "Csv",
    "Format",
    "JsonLines",
    "Msgpack",
    "get_format_by_mime_type",
]
