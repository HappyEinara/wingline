"""Helpers to filter and remove keys."""

import re

import inflection

from wingline.types import PayloadIterable, PayloadIterator, PipeProcess

RE_PYTHON_IDENTIFIER_INVALID_CHAR_REGEX = re.compile(r"[^0-9A-Za-z_]+")


def filter(*keys: str) -> PipeProcess:  # pylint: disable=redefined-builtin
    """Strip all but the specified keys."""

    def _filter_keys(payloads: PayloadIterable) -> PayloadIterator:
        """Strip all but the specified keys."""

        for payload in payloads:
            yield {k: v for k, v in payload.items() if k in keys}

    return _filter_keys


def remove(*keys: str) -> PipeProcess:
    """Strip the specified keys."""

    def _remove_keys(payloads: PayloadIterable) -> PayloadIterator:
        """Strip the specified keys."""

        for payload in payloads:
            yield {k: v for k, v in payload.items() if k not in keys}

    return _remove_keys


def snake(payloads: PayloadIterable) -> PayloadIterator:
    """Convert keys to snake case."""

    for payload in payloads:
        yield {inflection.underscore(k): v for k, v in payload.items()}


def python(payloads: PayloadIterable) -> PayloadIterator:
    """Convert keys to acceptable Python identifiers."""

    for payload in payloads:
        yield {
            RE_PYTHON_IDENTIFIER_INVALID_CHAR_REGEX.sub(
                "_", (inflection.underscore(inflection.transliterate(k)))
            ): v
            for k, v in payload.items()
        }


def camel(payloads: PayloadIterable) -> PayloadIterator:
    """Convert keys to camelCase"""
    for payload in payloads:
        yield {
            inflection.camelize(k, uppercase_first_letter=False): v
            for k, v in payload.items()
        }


def pascal(payloads: PayloadIterable) -> PayloadIterator:
    """Convert keys to PascalCase"""
    for payload in payloads:
        yield {
            inflection.camelize(k, uppercase_first_letter=True): v
            for k, v in payload.items()
        }


__all__ = [
    "filter",
    "remove",
    "snake",
    "python",
    "camel",
    "pascal",
]
