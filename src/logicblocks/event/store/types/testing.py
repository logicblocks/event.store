import string
import random

from dataclasses import dataclass
from typing import Mapping, Any
from types import MappingProxyType

from frozendict import frozendict

from .event import NewEvent


def random_event_name() -> str:
    first = random.choice(string.ascii_lowercase)
    last = random.choice(string.ascii_lowercase)
    rest = random.choices(string.ascii_lowercase + "-", k=18)

    return "".join([first] + rest + [last])


def random_event_payload() -> Mapping[str, Any]:
    return MappingProxyType(
        {
            "".join(random.choices(string.ascii_lowercase, k=10)): "".join(
                random.choices(string.ascii_letters + string.digits, k=10)
            )
            for _ in range(10)
        }
    )


@dataclass(frozen=True)
class NewEventBuilder:
    name: str
    payload: Mapping[str, Any]

    def __init__(
        self,
        *,
        name: str = random_event_name(),
        payload: Mapping[str, Any] = random_event_payload(),
    ):
        object.__setattr__(self, "name", name)
        object.__setattr__(self, "payload", frozendict(payload))

    def with_name(self, name: str):
        return NewEventBuilder(name=name, payload=self.payload)

    def with_payload(self, payload: Mapping[str, Any]):
        return NewEventBuilder(name=self.name, payload=payload)

    def build(self):
        return NewEvent(name=self.name, payload=self.payload)
