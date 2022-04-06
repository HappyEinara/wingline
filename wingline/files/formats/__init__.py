"""Formats."""

from wingline.files.formats._base import Format
from wingline.files.formats.csv import Csv
from wingline.files.formats.json_lines import JsonLines
from wingline.files.formats.msgpack import Msgpack

FORMATS: set[type[Format]] = {
    Csv,
    JsonLines,
    Msgpack,
}

__all__ = [
    "Csv",
    "FORMATS",
    "Format",
    "JsonLines",
    "Msgpack",
]
