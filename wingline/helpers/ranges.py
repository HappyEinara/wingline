"""Head and tail ranges."""

import collections

from wingline.types import Payload, PayloadIterator, PipeProcess


def head(count: int = 10) -> PipeProcess:
    """Return the first `count` values seen."""

    state = {"count": count, "seen": 0}

    def _head(payloads: PayloadIterator) -> PayloadIterator:
        """Process the payloads."""
        for payload in payloads:
            if state["seen"] < state["count"]:
                yield payload
                state["seen"] += 1
            else:
                return

    return _head


def tail(count: int = 10) -> PipeProcess:
    """Return the last `count` values seen."""

    deque: collections.deque[Payload] = collections.deque(maxlen=count)

    def _tail(payloads: PayloadIterator) -> PayloadIterator:
        """Process the payloads."""
        for payload in payloads:
            deque.append(payload)
        for payload in deque:
            yield payload

    return _tail
