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


class TimezoneRequiredStaticClock(StaticClock):
    def __init__(self, now: datetime, tz: tzinfo):
        super().__init__(now.astimezone(tz))
        self._tz = tz

    def set(self, now: datetime) -> None:
        super().set(now.astimezone(self._tz))

    def now(self, tz: tzinfo | None = None) -> datetime:
        if not tz == self._tz:
            raise ValueError(
                f"Clock supports only {self._tz} which must be provided."
            )
        return super().now(tz)
