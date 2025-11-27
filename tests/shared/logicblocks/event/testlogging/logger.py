import asyncio
import sys
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from enum import IntEnum
from typing import Any

from structlog.typing import Context, ExcInfo, FilteringBoundLogger

type OptionalExceptionInfo = ExcInfo | tuple[None, None, None] | None


class LogLevel(IntEnum):
    NOTSET = 0
    DEBUG = 10
    INFO = 20
    WARNING = 30
    ERROR = 40
    CRITICAL = 50


@dataclass(frozen=True)
class LogEvent:
    event: str
    level: LogLevel
    context: Mapping[str, Any]
    args: Sequence[Any]
    is_async: bool
    exc_info: OptionalExceptionInfo


class CapturingLogger(FilteringBoundLogger):
    def find_events(
        self, event: str, filter: Callable[[LogEvent], bool] = lambda _: True
    ) -> Sequence[LogEvent]:
        return [
            log_event
            for log_event in self.events
            if (log_event.event == event and filter(log_event))
        ]

    def find_event(
        self, event: str, filter: Callable[[LogEvent], bool] = lambda x: True
    ) -> LogEvent | None:
        events = self.find_events(event, filter)
        if len(events) == 0:
            return None
        if len(events) > 1:
            raise ValueError(
                f"Expected only one log event with name {event}, "
                f"found {len(events)}."
            )
        return events[0]

    @classmethod
    def create(cls, log_level: int = LogLevel.NOTSET) -> "CapturingLogger":
        return cls([], {}, log_level)

    def __init__(
        self,
        events: list[LogEvent],
        context: dict[str, Any],
        log_level: int = LogLevel.NOTSET,
    ):
        self.events = events
        self.context = context
        self.log_level = log_level

    @property
    def _context(self) -> Context:
        return self.context

    def bind(self, **new_values: Any) -> FilteringBoundLogger:
        context = dict(self.context)
        context.update(new_values)

        return CapturingLogger(self.events, context, self.log_level)

    def unbind(self, *keys: str) -> FilteringBoundLogger:
        context = dict(self.context)
        for key in keys:
            if key not in context:
                raise KeyError(f"No such binding: {key}")
            context.pop(key)

        return CapturingLogger(self.events, context, self.log_level)

    def try_unbind(self, *keys: str) -> FilteringBoundLogger:
        context = dict(self.context)
        for key in keys:
            if key not in context:
                continue
            context.pop(key)

        return CapturingLogger(self.events, context, self.log_level)

    def new(self, **new_values: Any) -> FilteringBoundLogger:
        context = {}
        context.update(new_values)

        return CapturingLogger(self.events, context, self.log_level)

    def is_enabled_for(self, level: int) -> bool:
        return level >= self.log_level

    def get_effective_level(self) -> int:
        return self.log_level

    def debug(self, event: str, *args: Any, **kw: Any) -> Any:
        self.events.append(
            LogEvent(
                event=event,
                level=LogLevel.DEBUG,
                context={**self.context, **kw},
                args=args,
                is_async=False,
                exc_info=None,
            )
        )

    async def adebug(self, event: str, *args: Any, **kw: Any) -> Any:
        self.events.append(
            LogEvent(
                event=event,
                level=LogLevel.DEBUG,
                context={**self.context, **kw},
                args=args,
                is_async=True,
                exc_info=None,
            )
        )
        await asyncio.sleep(0)

    def info(self, event: str, *args: Any, **kw: Any) -> Any:
        self.events.append(
            LogEvent(
                event=event,
                level=LogLevel.INFO,
                context={**self.context, **kw},
                args=args,
                is_async=False,
                exc_info=None,
            )
        )

    async def ainfo(self, event: str, *args: Any, **kw: Any) -> Any:
        self.events.append(
            LogEvent(
                event=event,
                level=LogLevel.INFO,
                context={**self.context, **kw},
                args=args,
                is_async=True,
                exc_info=None,
            )
        )
        await asyncio.sleep(0)

    def warning(self, event: str, *args: Any, **kw: Any) -> Any:
        self.events.append(
            LogEvent(
                event=event,
                level=LogLevel.WARNING,
                context={**self.context, **kw},
                args=args,
                is_async=False,
                exc_info=None,
            )
        )

    async def awarning(self, event: str, *args: Any, **kw: Any) -> Any:
        self.events.append(
            LogEvent(
                event=event,
                level=LogLevel.WARNING,
                context={**self.context, **kw},
                args=args,
                is_async=True,
                exc_info=None,
            )
        )
        await asyncio.sleep(0)

    def warn(self, event: str, *args: Any, **kw: Any) -> Any:
        self.events.append(
            LogEvent(
                event=event,
                level=LogLevel.WARNING,
                context={**self.context, **kw},
                args=args,
                is_async=False,
                exc_info=None,
            )
        )

    async def awarn(self, event: str, *args: Any, **kw: Any) -> Any:
        self.events.append(
            LogEvent(
                event=event,
                level=LogLevel.WARNING,
                context={**self.context, **kw},
                args=args,
                is_async=True,
                exc_info=None,
            )
        )
        await asyncio.sleep(0)

    def error(self, event: str, *args: Any, **kw: Any) -> Any:
        self.events.append(
            LogEvent(
                event=event,
                level=LogLevel.ERROR,
                context={**self.context, **kw},
                args=args,
                is_async=False,
                exc_info=None,
            )
        )

    async def aerror(self, event: str, *args: Any, **kw: Any) -> Any:
        self.events.append(
            LogEvent(
                event=event,
                level=LogLevel.ERROR,
                context={**self.context, **kw},
                args=args,
                is_async=True,
                exc_info=None,
            )
        )
        await asyncio.sleep(0)

    def err(self, event: str, *args: Any, **kw: Any) -> Any:
        self.events.append(
            LogEvent(
                event=event,
                level=LogLevel.ERROR,
                context={**self.context, **kw},
                args=args,
                is_async=False,
                exc_info=None,
            )
        )

    def fatal(self, event: str, *args: Any, **kw: Any) -> Any:
        self.events.append(
            LogEvent(
                event=event,
                level=LogLevel.CRITICAL,
                context={**self.context, **kw},
                args=args,
                is_async=False,
                exc_info=None,
            )
        )

    async def afatal(self, event: str, *args: Any, **kw: Any) -> Any:
        self.events.append(
            LogEvent(
                event=event,
                level=LogLevel.CRITICAL,
                context={**self.context, **kw},
                args=args,
                is_async=True,
                exc_info=None,
            )
        )
        await asyncio.sleep(0)

    def exception(self, event: str, *args: Any, **kw: Any) -> Any:
        self.events.append(
            LogEvent(
                event=event,
                level=LogLevel.ERROR,
                context={**self.context, **kw},
                args=args,
                is_async=False,
                exc_info=sys.exc_info(),
            )
        )

    async def aexception(self, event: str, *args: Any, **kw: Any) -> Any:
        self.events.append(
            LogEvent(
                event=event,
                level=LogLevel.ERROR,
                context={**self.context, **kw},
                args=args,
                is_async=True,
                exc_info=sys.exc_info(),
            )
        )
        await asyncio.sleep(0)

    def critical(self, event: str, *args: Any, **kw: Any) -> Any:
        self.events.append(
            LogEvent(
                event=event,
                level=LogLevel.CRITICAL,
                context={**self.context, **kw},
                args=args,
                is_async=False,
                exc_info=None,
            )
        )

    async def acritical(self, event: str, *args: Any, **kw: Any) -> Any:
        self.events.append(
            LogEvent(
                event=event,
                level=LogLevel.CRITICAL,
                context={**self.context, **kw},
                args=args,
                is_async=True,
                exc_info=None,
            )
        )
        await asyncio.sleep(0)

    def msg(self, event: str, *args: Any, **kw: Any) -> Any:
        self.events.append(
            LogEvent(
                event=event,
                level=LogLevel.NOTSET,
                context={**self.context, **kw},
                args=args,
                is_async=False,
                exc_info=None,
            )
        )

    async def amsg(self, event: str, *args: Any, **kw: Any) -> Any:
        self.events.append(
            LogEvent(
                event=event,
                level=LogLevel.NOTSET,
                context={**self.context, **kw},
                args=args,
                is_async=True,
                exc_info=None,
            )
        )
        await asyncio.sleep(0)

    def log(self, level: int, event: str, *args: Any, **kw: Any) -> Any:
        self.events.append(
            LogEvent(
                event=event,
                level=LogLevel(level),
                context={**self.context, **kw},
                args=args,
                is_async=False,
                exc_info=None,
            )
        )

    async def alog(self, level: int, event: str, *args: Any, **kw: Any) -> Any:
        self.events.append(
            LogEvent(
                event=event,
                level=LogLevel(level),
                context={**self.context, **kw},
                args=args,
                is_async=True,
                exc_info=None,
            )
        )
        await asyncio.sleep(0)
