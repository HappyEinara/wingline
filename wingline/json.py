"""Wrapper for json or ujson if available."""

# Try to use yapic or ujson if available.

import logging

logger = logging.getLogger(__name__)

try:
    from yapic import json

    logger.debug("Optional dependency found: json is yapic.json")
except ImportError:  # pragma: no cover
    try:
        import ujson as json  # type: ignore

        logger.debug("Optional dependency found: json is ujson")
    except ImportError:
        import json  # type: ignore

        logger.debug("No optional JSON dependency found: json is stdlib")

__all__ = [
    "json",
]
