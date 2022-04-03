"""Useful hooks for debugging."""
from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import msgpack

from wingline import hasher
from wingline.types import PayloadIterator

logger = logging.getLogger(__name__)
if TYPE_CHECKING:
    from wingline import plumbing
    from wingline.plumbing import base


def log_payloads(
    message: str = "",
) -> plumbing.PayloadIteratorHook:
    def _inner(
        plumbing: base.BasePlumbing, payloads: PayloadIterator
    ) -> PayloadIterator:
        for payload in payloads:
            _inner.lines += 1
            payload_hash = hasher.hasher(msgpack.packb(payload)).hexdigest()
            try:
                qsize = plumbing.input_queue.qsize()
            except:
                qsize = "??"
            logger.debug(
                "%s: Ls%s|Qs%s %s |%s| %.10s",
                plumbing,
                _inner.lines,
                qsize,
                message,
                payload_hash,
                payload,
            )
            yield payload

    _inner.lines = 0

    return _inner


def log_plumbing(message: str):
    def _inner(plumbing: base.BasePlumbing):
        logger.debug("%s: %s", plumbing, message)

    return _inner
