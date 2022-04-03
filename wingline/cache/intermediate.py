"""Intermediate cache helpers."""

import pathlib

from wingline.files import containers, formats

CACHE_PATH_SUFFIX = ".wingline"
FORMAT = formats.Msgpack
CONTAINER = containers.Gzip

def get_cache_path(hash: str, cache_dir: pathlib.Path):
    """Get the path for an intermedia cache file."""

    cache_path = cache_dir / hash[:2] / f"{hash}{CACHE_PATH_SUFFIX}"
    return cache_path
