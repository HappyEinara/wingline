"""Base hook implementation."""

from __future__ import annotations

import enum
import functools
from typing import TYPE_CHECKING, Callable

if TYPE_CHECKING:
    from wingline import pipe


class HookSpec(enum.Enum):
    START = "start"
    LINE = "line"
    END = "end"


def hook(hook_spec: HookSpec):
    """Register a callable as a hook."""

    def _outer(func: Callable[..., None]):
        @functools.wraps(func)
        def _inner(pipe: pipe.Pipe, *args, **kwargs):
            return func(pipe, *args, **kwargs)

        _inner._hook_spec = hook_spec
        return _inner

    return _outer
