from abc import ABC, abstractmethod
from datetime import datetime, tzinfo


class Clock(ABC):
    @abstractmethod
    def now(self, tz: tzinfo | None = None) -> datetime:
        raise NotImplementedError()


class SystemClock(Clock):
    def now(self, tz: tzinfo | None = None) -> datetime:
        return datetime.now(tz)


class StaticClock(Clock):
    def __init__(self, now: datetime):
        self._now = now

    def set(self, now: datetime) -> None:
        self._now = now

    def now(self, tz: tzinfo | None = None) -> datetime:
        return self._now
