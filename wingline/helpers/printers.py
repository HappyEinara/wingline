"""Printing helpers."""

import pprint

from wingline.types import PayloadIterable


def pretty(parent: PayloadIterable) -> PayloadIterable:
    """Pretty print the payload"""

    for payload in parent:
        pprint.pprint(payload)
        yield payload
