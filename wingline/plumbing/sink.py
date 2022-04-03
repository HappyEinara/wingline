"""Sink base class."""

from __future__ import annotations

from wingline.plumbing import pipe


class Sink(pipe.Pipe):
    """A pipe that has an output and must always run.

    Note that a sink may still have downstream children.
    Subclassing this means that it will run even if it's the
    ancestor of a cached ProcessPipe
    """
