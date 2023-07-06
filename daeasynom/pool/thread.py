from abc import ABCMeta
import threading
import logging

from queue import Queue
import time
from .meta import WorkRequest


logger = logging.getLogger(__name__)


class EOQTerminateThread(object):
    pass


class QueueingWorkThread(threading.Thread, metaclass=ABCMeta):
    def __init__(self, queue: Queue, actions: dict):
        super().__init__()
        self._queue = queue
        self._actions: dict[callable] = actions

        self.failed_jobs = []
        self.finished_jobs = []

    def run(self):
        item = None
        while item is not EOQTerminateThread:
            item = self.get_work()

            failed = False
            resubmit = True

            if item.id in self.failed_jobs:
                logger.error(f"Skipping failed job {item.id}")
                failed = True
                resubmit = False
            elif item.id in self.finished_jobs:
                logger.debug(f"Skipping finished job {item.id}")
                resubmit = False
            elif item.act in self._actions:
                action = self._actions[item.act]
                result = action(*item.args)
                logger.debug(f"{action.__name__}({item.args}) return {result}")
                resubmit = False
            elif item.meta.get("destroy", False):
                logger.debug(f"Destroying {item.id}")
                resubmit = False
            else:
                logger.debug(f"Unknown action {item.act}")

            # TODO: Rather than just putting it back on the queue, we should probably request
            # invalidating the request. item needs to become Request and such.
            self.finish_work(item, failed=failed, resubmit=resubmit)

    def get_work(self) -> WorkRequest:
        item = self._queue.get()
        if item is not EOQTerminateThread:
            logger.debug(f"Got work {item.id}")
        return item

    def finish_work(
        self, item: WorkRequest, failed: bool = False, resubmit: bool = False
    ):
        self._queue.task_done()

        if failed:
            logger.error(f"UKN {item.act=} | {self.__class__.__name__}-{self.ident}")
            if item.id in self.failed_jobs:
                logger.debug(f"Already failed this job {item.id}")
                item.meta["destroy"] = True
            else:
                self.failed_jobs.append(item.id)

        else:
            self.finished_jobs.append(item.id)

        if resubmit:
            if failed:
                logger.debug(f"Resubmitting failed job {item.id}")

            self.submit_work(item, failed=failed)

    def submit_work(self, item: WorkRequest, failed: bool = True):
        self._queue.put(item)
