"""Cases for pipe operation hashing."""

from wingline.helpers import head, tail
from wingline.types import PayloadIterable


def _add_a(parent: PayloadIterable) -> PayloadIterable:
    for item in parent:
        item["_a"] = "a"
        yield item


def _add_b(parent: PayloadIterable) -> PayloadIterable:
    for item in parent:
        item["_b"] = "b"
        yield item


_head_1 = head(1)
_head_2 = head(2)
_tail_1 = tail(1)
_tail_2 = tail(2)


def case_add_a():
    return (_add_a, "d5e4bbc670205f60")


def case_add_b():
    return (_add_b, "439d477eaf9c4a50")


def case_head_1():
    return _head_1, "23b362418a183441"


def case_head_2():
    return _head_2, "2368d8afb89a6e79"


def case_tail_1():
    return _tail_1, "b17afcbd2bd14646"


def case_tail_2():
    return _tail_2, "13bd182422f49e24"
