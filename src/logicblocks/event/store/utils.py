from abc import ABC, abstractmethod
from datetime import datetime, tzinfo
from typing import Optional


class Clock(ABC):
    @abstractmethod
    def now(self, tz: Optional[tzinfo] = None) -> datetime:
        raise NotImplementedError()


class SystemClock(Clock):
    def now(self, tz: Optional[tzinfo] = None) -> datetime:
        return datetime.now(tz)


class StaticClock(Clock):
    def __init__(self, now: datetime):
        self._now = now

    def now(self, tz: Optional[tzinfo] = None) -> datetime:
        return self._now
