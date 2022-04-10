"""Printing helpers."""

import pprint

from wingline.types import PayloadIterable, PayloadIterator


def pretty(payloads: PayloadIterable) -> PayloadIterator:
    """Pretty print the payload"""

    for payload in payloads:
        pprint.pprint(payload)
        yield payload
