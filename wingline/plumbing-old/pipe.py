"""Plumbing/Pipe class."""
from __future__ import annotations

import logging
import pathlib
from typing import TYPE_CHECKING, Optional

import msgpack

from wingline import hasher, plumbing
from wingline.plumbing import base, hooks, queue
from wingline.types import SENTINEL, Payload, PayloadIterator, PipeOperation

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from wingline.plumbing import PayloadIteratorHook


class Pipe(base.BasePlumbing):
    def __init__(
        self,
        parent: base.BasePlumbing,
        operation: PipeOperation,
        cache_dir: Optional[pathlib.Path] = None,
        name: Optional[str] = None,
    ):
        # Initialize the thread and identification.
        super().__init__()
        self.name: str = name if name is not None else self.__class__.name
        self.operation = operation

        # Initialize connection to parent.
        self.parent = parent
        self.parent.subscribe(self)

        # Initialize hashing/caching
        self._hash: Optional[str] = None
        self.cache_dir: Optional[pathlib.Path] = cache_dir

        # Initialize queues.
        self.input_queue: queue.Queue = queue.Queue()
        self.subscribers: list[Pipe] = []

        # Initialize hooks.
        self.input_hooks: list[plumbing.PayloadIteratorHook] = []
        self.output_hooks: list[plumbing.PayloadIteratorHook] = []
        self.start_hooks: list[plumbing.PlumbingHook] = []
        self.end_hooks: list[plumbing.PlumbingHook] = []

    def run_payload_hook(
        self, hook: PayloadIteratorHook, pipe: Pipe, payloads: PayloadIterator
    ):
        return hook(pipe, (payload for payload in payloads if payload is not SENTINEL))

    def run(self):
        try:
            self._debug("Starting?")
            self.output_hooks.append(hooks.log_payloads("output"))
            self.input_hooks.append(hooks.log_payloads("input"))
            self.start_hooks.append(hooks.log_plumbing("Started."))
            self.end_hooks.append(hooks.log_plumbing("Finished."))
            self._debug("Starthooks added?")
            self._debug("Starthooks calling subscribers %s", self.subscribers)

            [subscriber.start() for subscriber in self.subscribers]

            self._debug("Started subscribers: %s", self.subscribers)

            # Start hooks are called when a pipe or tap
            # starts generating items
            for hook in self.start_hooks:
                hook(self)

            terminate = False
            logger.debug("Loop starting")
            while True:
                payload: Payload = self.input_queue.get(timeout=5)
                logger.debug("Got payload %.20s", payload)
                if payload is SENTINEL:
                    terminate = True

                iter_payload: PayloadIterator = iter([payload])
                # Input hooks can read the input iter
                # but should not modify it.
                for hook in self.input_hooks:
                    iter_payload = self.run_payload_hook(hook, self, iter_payload)

                # The main operation hook takes the iterable of input items and
                # return an iterable of output items.
                #
                # These may be, for example:
                #   - the items unchanged
                #   - the items modified in any way
                #   - multiple new items adding to or replacing any input items
                #   - the input filtered for specific items
                #   - an empty iterable (if the input is to be discarded)
                #
                # Only a single operations is permitted per pipe
                # So intermediate caches can be isolated.
                if payload is not SENTINEL:
                    iter_payload = self.operation(iter_payload)

                # Output hooks can again read the iterable
                # of processed output but must not modify it.
                for hook in self.output_hooks:
                    iter_payload = self.run_payload_hook(hook, self, iter_payload)

                for payload in iter_payload:
                    self.propagate(payload)
                self.input_queue.task_done()
                if terminate:
                    self.propagate(SENTINEL)
                    break
            # End hooks are called when a pipe or tap
            # finishes generating items
            for hook in self.end_hooks:
                hook(self)
        except Exception as exc:
            logger.error(exc)
            logger.exception(exc)
            raise

    @property
    def hash(self):
        """Return a hash tied to the ultimate data source and subsequent processes."""

        hash = self._hash
        if hash is not None:
            return hash

        # Get the hash of the parent if it's already available.
        # If the ultimate ancestor is a file tap then the hash
        # should be immediately available.
        # TODO this should also be the same for sequence types
        # (but not other iterables)

        # Note that parents are always instantiated before
        # children (the parent was passed in above)
        # so unless the ancestor tap is an unidentified
        # iterable, we should have a hash available for
        # the parent.
        parent_hash = self.parent.hash

        # Regardless of any input/output hooks for display
        # or writing, if the operation is a no-op, then
        # the hash of this pipe is the hash of its parents
        operation_hash = hasher.hash_callable(self.operation)
        self._operation_hash = operation_hash

        # If we have hashes for both the parent and the operation
        # then we already have the hash for this pipe.
        if parent_hash and operation_hash:
            pipe_hash = hasher.hasher(
                f"{parent_hash}{operation_hash}".encode("utf-8")
            ).hexdigest()
            self._hash = pipe_hash
            return self._hash

    @property
    def cache_path(self) -> Optional[pathlib.Path]:
        """Get the path for the intermediate caching of the file."""

        if self._cache_path is not None:
            return self._cache_path
        if not (self.hash and self.cache_dir):
            return None

        cache_path = get_cache_path(self.hash, self.cache_dir)
        self._cache_path = cache_path
        return self._cache_path

    def pipe(self, operation, name: Optional[str] = None) -> Pipe:
        return Pipe(self, operation, name=name)

    def propagate(self, item) -> None:
        payload_hash = hasher.hasher(msgpack.packb(item)).hexdigest()
        for subscriber in self.subscribers:
            self._debug(
                "Propagating |%s| to %s (%s)",
                payload_hash,
                subscriber,
                subscriber.input_queue,
            )
            self._debug(
                "Subscriber %s input qsize before: %s",
                subscriber,
                subscriber.input_queue.qsize(),
            )
            subscriber.input_queue.put(item)
            self._debug(
                "Subscriber %s input qsize after: %s",
                subscriber,
                subscriber.input_queue.qsize(),
            )


def get_cache_path(hash: str, base_dir: pathlib.Path) -> pathlib.Path:
    """Get the path for the intermediate cache file for a given pipe hash."""

    cache_dir = base_dir / hash[:2]
    cache_file = cache_dir / f"{hash}.wingline"

    return cache_file
