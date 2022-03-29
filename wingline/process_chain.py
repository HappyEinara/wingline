"""Chained preprocessor for jsonl files."""
from __future__ import annotations

import collections
import functools
import json
import os
import pathlib
import tempfile
from typing import Any, BinaryIO, Callable, Iterable, Optional, TextIO

import cachelib
from rich import progress


class Stage:
    def __init__(
        self,
        chain: Chain,
        start_file: File,
        func: Callable[[dict[str, Any]], Iterable[dict[str, Any]]],
        previous_stage: Optional[Stage] = None,
        progress_callback: Optional[Callable[[int], None]] = None,
    ):
        self.chain = chain
        self.start_file = start_file
        self.func = func
        self.func_name = self.func.__name__
        self.func_hash = hasher(self.func.__code__.co_code).digest()
        self.previous_stage = previous_stage
        if self.previous_stage:
            self.stage_hash = hasher(
                self.previous_stage.stage_hash + self.func_hash
            ).digest()
            self.previous_stage.next_stage = self
        else:
            self.stage_hash = self.func_hash
        self.next_stage = None
        self._lines_processed = 0
        self._progress_callback = progress_callback

    @property
    def progress(self):
        return self._lines_processed, self.lines

    @property
    def input_file(self) -> File:
        return (
            self.previous_stage.output_file
            if self.previous_stage and self.previous_stage.output_file
            else self.start_file
        )

    @property
    def lines(self) -> int:
        lines = self.input_file.line_count
        if lines:
            return lines
        if self.previous_stage:
            return self.previous_stage.lines
        lines = self.start_file.line_count
        return lines

    @property
    def output_file(self) -> Optional[File]:
        if not self.input_file.exists:
            return None
        return File(
            self.chain.working_dir
            / (f"{self.start_file.content_hash}-{self.stage_hash.hex()}" ".jsonl"),
            cache=self.chain.cache,
        )

    @property
    def complete(self) -> bool:
        if self.next_stage is not None:
            return self.next_stage.complete
        return self.output_file is not None and self.output_file.exists

    @property
    def ancestry(self) -> list[Stage]:
        parent = self
        parents = []
        while parent:
            parents.append(parent)
            parent = parent.previous_stage
        return parents

    def execute(
        self, progress_callback: Optional[Callable[[int], None]] = None
    ) -> Optional[Stage]:
        input_file = self.input_file
        output_file = self.output_file
        output_file.path.parent.mkdir(parents=True, exist_ok=True)
        func = self.func

        with tempfile.NamedTemporaryFile(
            "w", delete=False, dir=output_file.path.parent
        ) as temp_file:
            temp_path = pathlib.Path(temp_file.name)
            with self.input_file.reader() as reader:
                for input_obj in reader:
                    for output_obj in func(input_obj):
                        output_line = json.dumps(
                            output_obj, sort_keys=True, default=str
                        )
                        temp_file.write(output_line + "\n")
                    if progress_callback:
                        progress_callback(1)

        temp_path.rename(output_file.path)
        logger.debug(
            "Wrote results of %s(%s) to %s", func.__name__, input_file, output_file
        )

        if self.next_stage:
            return self.next_stage
        return None

    def __str__(self):
        return f"<S {self.start_file}|{self.func_name}|{self.lines}>"

    def __repr__(self):
        return str(self)


class Chain:
    def __init__(
        self,
        source_files: Iterable[pathlib.Path],
        stages: Iterable[Callable[[dict[str, Any]], Iterable[dict[str, Any]]]],
        working_dir: pathlib.Path,
        cache: Optional[cachelib.BaseCache] = None,
    ):
        self.cache = cache
        self.source_files = [File(f, cache=self.cache) for f in source_files]
        self.stages = list(stages)
        self.working_dir = working_dir
        self.working_dir.mkdir(parents=True, exist_ok=True)
        self._all_tasks: Optional[list[Stage]] = None
        self._todo: Optional[list[Stage]] = None

    @property
    def todo(self) -> list[Stage]:
        if self._todo is not None:
            return self._todo
        self._todo = [task for task in self.all_tasks if not task.complete]
        self._todo.sort(key=lambda x: x.lines)
        return self._todo

    @property
    def all_tasks(self) -> list[Stage]:
        if self._all_tasks is not None:
            return self._all_tasks
        all_tasks = []
        for start_file in self.source_files:
            ft = start_file.filetypes
            previous_stage = None
            for stage_func in self.stages:
                stage = Stage(self, start_file, stage_func, previous_stage)
                if previous_stage is None:
                    all_tasks.append(stage)
                previous_stage = stage
        self._all_tasks = all_tasks
        return self._all_tasks

    @property
    def total_lines(self) -> int:
        return sum(s.lines for s in self.todo)

    def run(self):

        todo = collections.deque(self.todo)

        with progress.Progress(refresh_per_second=3) as progress_bar:
            overall_progress = progress_bar.add_task("Overall", total=self.total_lines)

            def _update_progress(
                progress_bar_task: progress.TaskID, advance: Optional[int] = None
            ):
                progress_bar.update(progress_bar_task, advance=advance)
                progress_bar.update(overall_progress, advance=advance)

            while todo:
                task = todo.popleft()
                progress_bar_task = progress_bar.add_task(
                    f"[red] {task.func_name}({task.start_file.path.name})",
                    total=task.lines,
                )
                progress_bar_update_callback = functools.partial(
                    _update_progress, progress_bar_task
                )
                next_task = task.execute(progress_callback=progress_bar_update_callback)
                func_ancestry = reversed([s.func_name for s in task.ancestry])
                progress_bar.console.log(
                    f"{task.start_file.path.name} ➡ {' ➡ '.join(func_ancestry)} ➡ {task.output_file.path.name}"
                )
                if next_task:
                    todo.append(next_task)
