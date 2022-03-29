"""Utilities."""
import logging

logger = logging.getLogger(__name__)

import threading
import time

from wingline.plumbing import base


class ThreadMonitor(threading.Thread):
    def __init__(self, source: base.BasePlumbing):
        super().__init__()
        self.threads = list(self._find(source))

    def _find(self, source: base.BasePlumbing):
        yield source
        for thread in source.subscribers:
            for result in self._find(thread):
                yield result

    def run(self):
        def _details(thread: threading.Thread):
            details = f"{thread}|"
            details += "💓" if thread.is_alive() else "❌"
            queues = [getattr(thread, "input_queue", None)]
            output_queues = [getattr(thread, "output_queues", None)]
            queues += output_queues
            for queue in queues:
                if not queue:
                    continue
                details += f"|{queue.qsize()}"
            return details

        while True:
            for thread in self.threads:
                logger.debug("%s: %s |%s", self, thread, _details(thread))
            time.sleep(15)
