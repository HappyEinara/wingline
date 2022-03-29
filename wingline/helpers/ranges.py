"""Head and tail ranges."""

import collections

from wingline.types import PayloadIterable


class Head:
    def __init__(self, count=10):
        self.count = count
        self.seen = 0

    def __call__(self, parent: PayloadIterable) -> PayloadIterable:
        for payload in parent:
            if self.seen < self.count:
                yield payload
                self.seen += 1
            else:
                return


head = Head


class Tail:
    def __init__(self, count: int = 10):
        self.deque = collections.deque(maxlen=count)

    def __call__(self, parent: PayloadIterable) -> PayloadIterable:
        for payload in parent:
            self.deque.append(payload)
        for payload in self.deque:
            yield payload


tail = Tail
