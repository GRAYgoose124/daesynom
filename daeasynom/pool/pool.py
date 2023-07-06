from dataclasses import dataclass

from queue import Queue
from typing import Literal
import pydantic
import uuid

from .meta import ActionSettingMeta, WorkRequest
from .thread import QueueingWorkThread, EOQTerminateThread


class QueueingThreadPool(metaclass=ActionSettingMeta):
    class Settings:
        max_jobs = 100
        num_threads = 4

    class Actions:
        @staticmethod
        def action(arg: str, arg2: int):
            pass

    def __init__(self):
        self._todo_jobs = Queue()
        self._running_jobs = {}

        self._threads = [
            QueueingWorkThread(self._todo_jobs, self.Actions)
            for i in range(self.num_threads)
        ]

    def start(self):
        for thread in self._threads:
            thread.start()

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
