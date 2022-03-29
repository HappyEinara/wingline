"""Execution plan generator."""

from __future__ import annotations

from typing import Iterator, Optional

from wingline import plumbing
from wingline.plumbing import base, file, intermediate_cache, pipe, writer
from wingline.types import PayloadIterable, PayloadIterator, PipeOperation


class ExecutionPlan:
    def __init__(
        self,
        sink: base.BasePlumbing,
        cache: Optional[intermediate_cache.IntermediateCache] = None,
    ) -> None:
        """Plan the execution of a pipeline."""

        self._steps: list[base.BasePlumbing] = []
        self._raw_steps: list[base.BasePlumbing] = []
        self.cache = cache

        # The graph is typically linear but strictly it doesn't have to be.
        # Every pipeline has exactly one source though, so
        # defining our execution plan based on exactly one _sink_ means
        # there is a linear sequence of tasks achieving an explicit
        # end goal.
        # Exhaustively catering for a Tree/DAG pattern seems
        # overkill for now.
        self.sink: base.BasePlumbing = sink

        # The graph is defined by each element's relationship
        # to its parents, but a list of steps is easier to
        # handle inside this class.
        self._update_steps()

    def _update_steps(self):
        """Convert the graph of parent-child pipes into an unprocessed list of steps."""

        steps = []
        step = self.sink
        while step.parent:
            steps.append(step)
            step = step.parent
        steps.append(step)
        self._raw_steps = steps

    @property
    def steps(self):
        """Return the optimised steps."""

        if self._steps:
            return self._steps

        if self.cache is None:
            self._steps = self._raw_steps
            return self._steps

        new_steps = []
        for step in self._raw_steps:
            if not step.hash:
                raise RuntimeError("Encountered a step without a hash.")
            if isinstance(step, pipe.Pipe) and self.cache:
                new_steps.extend(self._resolve_cache_steps(step))
            else:
                new_steps.append(step)

        new_steps.reverse()
        self._steps = new_steps
        return self._steps

    def _resolve_cache_steps(self, step: pipe.Pipe) -> Iterator[base.BasePlumbing]:
        """Determine if a cached copy exists or one should be made."""

        if self.cache is None:
            raise RuntimeError(
                "Cache resolution was called but no cache dir was provided."
            )
        if step.hash is None:
            raise RuntimeError("Cache resolution was called on a step with no hash.")

        cache_path = self.cache.cache_path(step.hash)
        if cache_path.exists():
            cache_reader = file.File(cache_path)
            cache_reader.parent = step
            cache_reader.is_cached = True
            yield cache_reader
            current_step: base.BasePlumbing = step
            while current_step.parent:
                current_step.is_disabled = True
                yield current_step
                current_step = current_step.parent
            current_step.is_disabled = True
            yield current_step
        else:
            step.will_cache = True
            cache_writer = self.cache.get_writer_pipe(step)
            yield step

    @property
    def source(self):
        """Get the pipeline source after processing."""
        return self.steps[0]

    def pretty(self):
        """Pretty print the step graph."""

        output = ""
        for i, step in enumerate(reversed(self.steps)):
            output = f"{output}\n{' ' * i} ↳ {step}"
        return output
