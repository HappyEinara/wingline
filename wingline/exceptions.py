"""Exceptions."""


class WinglineError(Exception):
    """Base exception for all Wingline exceptions."""


class HashUnavailableError(WinglineError, RuntimeError):
    """Raised when a hash for a process is requested but is unavailable."""
