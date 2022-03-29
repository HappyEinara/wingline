"""Logging hook."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from wingline.hooks._base import HookSpec, hook
from wingline.types import Payload

if TYPE_CHECKING:
    from wingline import pipe

logger = logging.getLogger(__name__)


@hook(HookSpec.START)
def start(pipe: pipe.Pipe):
    """Log the start of a pipe."""

    logger.debug("Starting pipe %s", pipe)


@hook(HookSpec.LINE)
def line(pipe: pipe.Pipe, payload: Payload):
    """Log a line read from a pipe."""

    logger.debug("%s: Read line %s/%s: %.10s", pipe, 0, 0, str(payload))


HOOKS = [
    start,
    line,
]
