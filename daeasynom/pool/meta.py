import enum
from dataclasses import dataclass, field
from typing import Any
import uuid


@dataclass
class WorkRequest:
    id: str = field(default_factory=lambda: uuid.uuid4().hex)
    act: str = field(default=str)
    args: tuple = field(default_factory=tuple)
    meta: dict = field(default_factory=dict)


def create_str_enum_type(name: str, values: list[str]) -> enum.Enum:
    values = [v.upper() for v in values]
    members = {v: v for v in values}
    enum_type = enum.Enum(name, members, type=str)

    return enum_type


class ActionSettingMeta(type):
    def __new__(cls, name, bases, attrs):
        if "Actions" in attrs:
            # add each action staticmethod to a dict of action, functions
            actions = {
                k: v.__func__
                for k, v in attrs["Actions"].__dict__.items()
                if isinstance(v, staticmethod)
            }
            attrs["Actions"] = actions

        if "Settings" in attrs:
            # add each setting to the class
            for k, v in attrs["Settings"].__dict__.items():
                if not k.startswith("__"):
                    attrs[k] = v

            del attrs["Settings"]

        return super().__new__(cls, name, bases, attrs)
