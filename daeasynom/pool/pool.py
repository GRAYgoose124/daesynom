from dataclasses import dataclass
import logging

from queue import Queue
import threading
import time
from typing import Literal
import pydantic
import uuid
import threading

from .meta import ActionsMeta, WorkRequest
from .thread import QueueingWorkThread, EOQTerminateThread


logger = logging.getLogger(__name__)


class QueueingThreadPool(threading.Thread, metaclass=ActionsMeta):
    class Settings:
        max_jobs = 100
        num_threads = 4

    class Actions:
        @staticmethod
        def action(arg: str, arg2: int):
            pass

    def __init__(self):
        super().__init__()
        self._todo_jobs = Queue()
        self._running_jobs = Queue()

        self._threads = [
            QueueingWorkThread(self._todo_jobs, self._running_jobs, self.Actions)
            for i in range(self.num_threads)
        ]

    def run(self):
        for thread in self._threads:
            thread.start()

        # munch on running jobs queue then join threads
        item = None
        while item is not EOQTerminateThread:
            item = self._running_jobs.get()
            if item is EOQTerminateThread:
                break

            if item is None:
                time.sleep(0.1)
            else:
                logger.debug(
                    "Found running job %s being worked by %s",
                    item.id,
                    item.meta["active_thread"],
                )

    def submit_work(self, act, args):
        item = WorkRequest(act=act, args=args)
        if self._todo_jobs.qsize() >= self.max_jobs:
            raise RuntimeError("Too many jobs in queue")

        self._todo_jobs.put(item)

    def join(self):
        for _ in self._threads:
            self._todo_jobs.put(EOQTerminateThread)
        for thread in self._threads:
            thread.join()
