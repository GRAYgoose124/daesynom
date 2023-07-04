import json
import threading
import uuid
import logging

from dataclass_wizard import JSONWizard
from typing import Literal
from dataclasses import asdict, dataclass, field


logger = logging.getLogger(__name__)


def handle_keyboard_interrupt(signal, frame):
    raise KeyboardInterrupt


def hex_uuid():
    return uuid.uuid4().hex


@dataclass
class DataPacket(JSONWizard):
    id: str = field(default_factory=hex_uuid, hash=True)
    ty: Literal["request", "response", "unknown"] = "unknown"
    status: Literal[
        "fresh", "submitted", "pending", "working", "error", "dead", "complete"
    ] = "fresh"
    data: str = field(default_factory=str)
    errors: list[str] = field(default_factory=list)
    results: dict[str] = field(default_factory=dict)

    def add_error(self, error: str, ty: str = "default"):
        self.set_status("error")
        if ty not in self.errors:
            self.errors[ty] = [error]
        else:
            self.errors[ty].append(error)

    def update_result(self, name: str, result: str):
        if name not in self.results:
            self.add_error(f"{name=} is not in results, cannot update {result=}")
        else:
            self.results[name] = result

    def add_result(
        self,
        name: str,
        result: str,
        multitype: Literal["single", "multiple"] = "multiple",
    ):
        if name not in self.results:
            if multitype == "single":
                self.results[name] = result
            elif multitype == "multiple":
                self.results[name] = [result]
        else:
            if multitype == "single":
                self.add_error(f"{name=} is already in results, cannot add {result=}")
            elif multitype == "multiple":
                self.results[name].append(result)

    def set_status(self, status: str):
        if self.status == "error" or self.status == "dead" or self.status == "complete":
            self.errors.append(
                f"Cannot set status to {status=} when status is {self.status=}"
            )
        else:
            self.status = status

    def done(self):
        if self.status == "error":
            self.set_status("dead")
        else:
            self.set_status("complete")

        return self

    @staticmethod
    def from_json_str(json_str: str):
        return DataPacket(**json.loads(json_str))

    def to_json_str(self):
        return json.dumps(asdict(self))


class StoppableThread(threading.Thread):
    """Thread class with a stop() method. The thread itself has to check
    regularly for the stopped() condition."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._stop_event = threading.Event()

    def stop(self):
        self._stop_event.set()
        logger.debug(
            f"Attempting to stop {self.__class__.__name__}: Worker-{self.ident}"
        )

    @property
    def stopped(self):
        return self._stop_event.is_set()
