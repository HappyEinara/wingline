"""Test misc tap issues."""

import pytest

from wingline.plumbing import base


def test_join_before_start(simple_data):
    """Joining a tap before it's started raises."""

    test_tap = base.Tap(simple_data, "test_tap")
    with pytest.raises(RuntimeError):
        test_tap.join()
