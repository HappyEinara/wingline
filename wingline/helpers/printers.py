"""Printing helpers."""

import logging
import pprint
import sys

from wingline.types import PayloadIterable, PayloadIterator

logger = logging.getLogger(__name__)


def pretty(payloads: PayloadIterable) -> PayloadIterator:
    """Pretty print the payload"""

    sys.stdout.flush()

    for payload in payloads:
        logger.debug("%s Got payload: %s", pretty, str(payload)[:20])
        pprint.pprint(payload)
        yield payload

    sys.stdout.flush()
