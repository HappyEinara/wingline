"""Intermediate cache helpers."""

import pathlib

from wingline.files import containers
from wingline.files import filetype as ft
from wingline.files import formats

CACHE_PATH_SUFFIX = ".wingline"
filetype = ft.Filetype(formats.Msgpack(), containers.Gzip())


def get_cache_path(content_hash: str, cache_dir: pathlib.Path) -> pathlib.Path:
    """Get the path for an intermedia cache file."""

    cache_path = cache_dir / content_hash[:2] / f"{content_hash}{CACHE_PATH_SUFFIX}"
    return cache_path
