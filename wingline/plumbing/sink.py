"""Sink class"""
from __future__ import annotations

from typing import TYPE_CHECKING, Optional

from wingline import plumbing
from wingline.plumbing import base, hooks, queue
from wingline.types import SENTINEL, PayloadIterator

if TYPE_CHECKING:
    from wingline.plumbing import PayloadIteratorHook


class Sink(base.BasePlumbing):

    emoji = "⭲"

    def __init__(
        self,
        parent: base.BasePlumbing,
        name: Optional[str] = None,
    ):
        self._name: str = name if name is not None else self.__class__.name
        # Initialize the thread and identification.
        super().__init__()
        self.name = self._name

        # Initialize connection to parent.
        self.parent = parent
        self.parent.subscribe(self)
        # Sinks are, by definition, no-ops so their hash
        # should be inherited from their parent.
        self.hash = parent.hash

        # Initialize queues.
        self.input_queue: queue.Queue = queue.Queue()
        self.iter_queue: queue.Queue = queue.Queue()

        # Initialize hooks.
        self.input_hooks: list[plumbing.PayloadIteratorHook] = []
        self.start_hooks: list[plumbing.PlumbingHook] = []
        self.end_hooks: list[plumbing.PlumbingHook] = []

    def run_payload_hook(
        self,
        hook: PayloadIteratorHook,
        pipe: base.BasePlumbing,
        payloads: PayloadIterator,
    ):
        return hook(pipe, (payload for payload in payloads if payload is not SENTINEL))

    def run(self):
        self.input_hooks.append(hooks.log_payloads("input"))
        self.start_hooks.append(hooks.log_plumbing("Started."))
        self.end_hooks.append(hooks.log_plumbing("Finished."))

        # Start hooks are called when a pipe or tap
        # starts generating items
        for hook in self.start_hooks:
            hook(self)

        terminate = False
        while True:
            payload = self.input_queue.get(timeout=30)
            if payload is SENTINEL:
                terminate = True

            iter_payload = iter((payload,))
            # Input hooks can read the input iter
            # but should not modify it.
            for hook in self.input_hooks:
                iter_payload = self.run_payload_hook(hook, self, iter_payload)

            for payload in iter_payload:
                self.iter_queue.put(payload)
            self.input_queue.task_done()
            if terminate:
                self.iter_queue.put(SENTINEL)
                break
        # End hooks are called when a pipe or tap
        # finishes generating items
        for hook in self.end_hooks:
            hook(self)

    def __iter__(self):
        terminate = False
        while True:
            payload = self.iter_queue.get(timeout=5)
            self.iter_queue.task_done()
            if payload is SENTINEL:
                terminate = True
            else:
                yield payload
            if terminate:
                break
