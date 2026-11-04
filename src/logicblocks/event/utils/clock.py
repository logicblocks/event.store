from abc import ABC, abstractmethod
from datetime import datetime, tzinfo


class Clock(ABC):
    """Abstract source of the current time, replaceable in tests."""

    @abstractmethod
    def now(self, tz: tzinfo | None = None) -> datetime:
        """Get the current time.

        Args:
            tz: The time zone to return the time in. Defaults to `None`,
                giving a naive local time.

        Returns:
            the current time.
        """
        raise NotImplementedError()


class SystemClock(Clock):
    """A clock that returns the system time via `datetime.now`."""

    def now(self, tz: tzinfo | None = None) -> datetime:
        return datetime.now(tz)


class StaticClock(Clock):
    """A clock that always returns a fixed time until it is changed.

    `now` ignores its `tz` argument and returns the time exactly as set.

    Args:
        now: The time to return.
    """

    def __init__(self, now: datetime):
        self._now = now

    def set(self, now: datetime) -> None:
        """Change the time the clock returns."""
        self._now = now

    def now(self, tz: tzinfo | None = None) -> datetime:
        return self._now


class TimezoneRequiredStaticClock(StaticClock):
    """A static clock that requires callers to request a specific time zone.

    Useful for asserting that code asks for times in the right time zone.
    Times are converted to `tz` when set, and `now` raises a `ValueError` if
    called with any other time zone.

    Args:
        now: The time to return.
        tz: The only time zone `now` may be called with.
    """

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
