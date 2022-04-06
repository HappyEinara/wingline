"""Printing helpers."""

import pprint

from wingline.types import PayloadIterable


def pretty(payloads: PayloadIterable) -> PayloadIterable:
    """Pretty print the payload"""

    for payload in payloads:
        pprint.pprint(payload)
        yield payload
