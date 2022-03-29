"""Caching wrapper for File objects."""

from __future__ import annotations

import functools
import pathlib
from typing import Any, Callable

import cachelib

from wingline.files import file


class CachingProperty:
    def __init__(self, func: Callable[[CachedFile], Any], key_prefix: str):
        self.key_prefix = key_prefix
        self.func = func

    def __get__(self, func: Callable[[CachedFile], Any]) -> Any:
        """Get an attribute."""

        def _get_cached(file: CachedFile):
            prop = func.__name__
            memo = file._memo.get(prop)
            if memo:
                return memo
            key = f"{self.key_prefix}|{prop}"
            cached = file._cache.get(key)
            if cached:
                file._memo[prop] = cached
                return cached
            value = func(file)
            file._memo[prop] = value
            file._cache.set(key, value)
            return value

        return _get_cached


class CachedFile(file.File):
    """A file wrapped with cacheing."""

    def __init__(self, path: pathlib.Path, cache: cachelib.BaseCache):
        self._cache = cache
        self._memo: dict[str, Any] = {}
        super().__init__(path)

    def _cache_result(self, func: Callable[[CachedFile], Any], key_prefix: str):
        prop = func.__name__
        memo = self._memo.get(prop)
        if memo:
            return memo
        key = f"{key_prefix}|{prop}"
        cached = self._cache.get(key)
        if cached:
            self._memo[prop] = cached
            return cached
        value = func(self)
        self._memo[prop] = value
        self._cache.set(key, value)
        return value

    @staticmethod
    def cache_by_stat(func: Callable[[CachedFile], Any]):
        """A property cached by path and stat.

        Only used for the content hash.
        """

        @functools.wraps(func)
        def _cached(file: CachedFile):
            return file._cache_result(func, file._stat_key)

        return property(_cached)

    @staticmethod
    def cache_by_hash(func: Callable[[CachedFile], Any]):
        """A property cached by content hash."""

        @functools.wraps(func)
        def _cached(file: CachedFile):
            return file._cache_result(func, file._hash_key)

        return property(_cached)

    @property
    def _stat_key(self) -> str:
        """A cache key based on the file's stat"""

        return f"F|{self.path}|{self.size}|{self.modified_at}"

    @property
    def _hash_key(self) -> str:
        """A cache key based on the file's stat"""

        return f"F|{self.content_hash}"

    @cache_by_stat
    def content_hash(self):
        return super().content_hash
