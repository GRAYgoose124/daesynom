from dataclasses import asdict, dataclass, field
import json
from dataclass_wizard import JSONWizard
from typing import Literal
import uuid

def handle_keyboard_interrupt(signal, frame):
    raise KeyboardInterrupt

def hex_uuid():
    return uuid.uuid4().hex


@dataclass
class DataPacket(JSONWizard):
    id: str = field(default_factory=hex_uuid, hash=True)
    ty: Literal["request", "response", "unknown"] = "unknown"
    status: Literal[
        "submitted", "working", "pending", "error", "timeout", "complete"
    ] = "pending"
    data: str = field(default_factory=str)
    errors: list[str] = field(default_factory=list)
    results: dict[str] = field(default_factory=dict)

    def add_error(self, error: str):
        if self.status == "complete":
            return

        self.status = "error"
        self.errors.append(error)

    def add_result(self, name: str, result: str):
        if self.status == "complete":
            return

        if name not in self.results:
            self.results[name] = [result]
        else:
            self.results[name].append(result)

    def done(self):
        self.status = "complete"
        return self

    @staticmethod
    def from_json_str(json_str: str):
        return DataPacket(**json.loads(json_str))

    def to_json_str(self):
        return json.dumps(asdict(self))
