"""Configure logging."""
import logging
from pathlib import Path

from . import settings

stderr = logging.StreamHandler()

logger = logging.getLogger("wingline")

logger.setLevel(logging.DEBUG if settings.debug else logging.INFO)
logger.addHandler(stderr)

file_handlers = {}


def set_level(level: int) -> None:
    """Set the log level for the app."""

    logger.setLevel(level)


def log_to_file(filename: Path, disable_stderr: bool = True) -> None:
    """Enable file logging."""

    if disable_stderr:
        logger.removeHandler(stderr)

    filename.parent.mkdir(parents=True, exist_ok=True)
    handler = logging.FileHandler(filename)
    file_handlers[filename] = handler
    logger.addHandler(handler)
