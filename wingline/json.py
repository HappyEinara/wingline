"""Wrapper for json or ujson if available."""

# Try to use ujson

try:
    import ujson as json
except ImportError:
    import json  # type: ignore

__all__ = [
    "json",
]
