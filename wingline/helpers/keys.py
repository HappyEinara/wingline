"""Helpers to filter and remove keys."""

from wingline.types import PayloadIterable, PayloadIterator, PipeProcess


def filter_keys(*keys: str) -> PipeProcess:
    """Strip all but the specified keys."""

    def _filter_keys(payloads: PayloadIterable) -> PayloadIterator:
        """Strip all but the specified keys."""

        for payload in payloads:
            yield {k: v for k, v in payload.items() if k in keys}

    return _filter_keys


def remove_keys(*keys: str) -> PipeProcess:
    """Strip the specified keys."""

    def _remove_keys(payloads: PayloadIterable) -> PayloadIterator:
        """Strip the specified keys."""

        for payload in payloads:
            yield {k: v for k, v in payload.items() if k not in keys}

    return _remove_keys
