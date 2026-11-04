import asyncio
import warnings
from abc import ABC, abstractmethod
from builtins import callable as is_callable
from collections.abc import Awaitable, Callable, Mapping, Sequence
from datetime import timedelta
from types import MappingProxyType
from typing import Any, NotRequired, TypedDict

from ..callable import CallableService, CallableServiceCallable
from ..types import Service
from .error_handler import (
    ContinueErrorHandler,
    ErrorHandler,
    ExitErrorHandler,
    RaiseErrorHandler,
    RetryErrorHandler,
    default_exception_factory,
    default_exit_code_factory,
)
from .error_handler_decision import (
    ContinueErrorHandlerDecision,
    ErrorHandlerDecision,
    ExitErrorHandlerDecision,
    RaiseErrorHandlerDecision,
    RetryErrorHandlerDecision,
)
from .retry_strategy import RetryStrategy


class ErrorHandlingDefinition[T]:
    """Pairs an error handler with a callback invoked before handling.

    Args:
        handler: The error handler to use.
        callback: Called with the exception before the handler is used.
    """

    def __init__(
        self,
        handler: ErrorHandler[T],
        callback: Callable[[BaseException], None] = lambda _: None,
    ):
        self.handler = handler
        self.callback = callback


type TypeMappings[T] = Mapping[type[BaseException], ErrorHandlingDefinition[T]]


class TypeMappingDict(TypedDict):
    """Exception types to handle, with an optional callback."""

    types: Sequence[type[BaseException]]
    callback: NotRequired[Callable[[BaseException], None]]


class ExitFatallyTypeMappingDict(TypeMappingDict):
    exit_code_factory: NotRequired[Callable[[BaseException], int]]


class RaiseExceptionTypeMappingDict(TypeMappingDict):
    exception_factory: NotRequired[Callable[[BaseException], BaseException]]


class ContinueExecutionTypeMappingDict[T](TypeMappingDict):
    value_factory: Callable[[BaseException], T]


class RetryExecutionTypeMappingDict(TypeMappingDict):
    retry_strategy: NotRequired[RetryStrategy | None]


type ExitFatallyTypeMappingValue = (
    Sequence[type[BaseException]] | ExitFatallyTypeMappingDict | None
)
type RaiseExceptionTypeMappingValue = (
    Sequence[type[BaseException]] | RaiseExceptionTypeMappingDict | None
)
type ContinueExecutionTypeMappingValue[T] = (
    ContinueExecutionTypeMappingDict[T] | None
)
type RetryExecutionTypeMappingValue = (
    Sequence[type[BaseException]] | RetryExecutionTypeMappingDict | None
)


class TypeMappingsDict[T](TypedDict, total=False):
    exit_fatally: ExitFatallyTypeMappingValue
    raise_exception: RaiseExceptionTypeMappingValue
    continue_execution: ContinueExecutionTypeMappingValue[T]
    retry_execution: RetryExecutionTypeMappingValue


def resolve_exception_types(
    type_mapping_value: Sequence[type[BaseException]] | TypeMappingDict | None,
) -> Sequence[type[BaseException]]:
    if isinstance(type_mapping_value, Sequence):
        return type_mapping_value
    elif type_mapping_value is None:
        return []
    else:
        return type_mapping_value["types"]


def resolve_callback(
    type_mapping_value: Sequence[type[BaseException]] | TypeMappingDict | None,
):
    if isinstance(type_mapping_value, Sequence) or type_mapping_value is None:
        return lambda _: None
    return type_mapping_value["callback"]


def resolve_retry_strategy(
    type_mapping_value: Sequence[type[BaseException]]
    | RetryExecutionTypeMappingDict
    | None,
) -> RetryStrategy | None:
    if isinstance(type_mapping_value, Sequence) or type_mapping_value is None:
        return None
    return type_mapping_value.get("retry_strategy")


def resolve_exit_fatally_type_mapping_dict(
    type_mapping_value: ExitFatallyTypeMappingValue,
) -> ExitFatallyTypeMappingDict | None:
    if type_mapping_value is None:
        return None

    types = resolve_exception_types(type_mapping_value)
    callback = resolve_callback(type_mapping_value)

    if isinstance(type_mapping_value, Sequence) or type_mapping_value is None:
        exit_code_factory: Callable[[BaseException], int] = (
            default_exit_code_factory
        )
    else:
        exit_code_factory: Callable[[BaseException], int] = type_mapping_value[
            "exit_code_factory"
        ]

    return ExitFatallyTypeMappingDict(
        types=types,
        callback=callback,
        exit_code_factory=exit_code_factory,
    )


def resolve_raise_exception_type_mapping_dict(
    type_mapping_value: RaiseExceptionTypeMappingValue,
) -> RaiseExceptionTypeMappingDict | None:
    if type_mapping_value is None:
        return None

    types = resolve_exception_types(type_mapping_value)
    callback = resolve_callback(type_mapping_value)

    if isinstance(type_mapping_value, Sequence) or type_mapping_value is None:
        exception_factory: Callable[[BaseException], BaseException] = (
            default_exception_factory
        )
    else:
        exception_factory: Callable[[BaseException], BaseException] = (
            type_mapping_value["exception_factory"]
        )

    return RaiseExceptionTypeMappingDict(
        types=types,
        exception_factory=exception_factory,
        callback=callback,
    )


def resolve_continue_execution_type_mapping_dict[T](
    type_mapping_value: ContinueExecutionTypeMappingValue[T],
) -> ContinueExecutionTypeMappingDict[T] | None:
    if type_mapping_value is None:
        return None

    types = resolve_exception_types(type_mapping_value)
    callback = resolve_callback(type_mapping_value)
    value_factory = type_mapping_value["value_factory"]

    return ContinueExecutionTypeMappingDict[T](
        types=types,
        callback=callback,
        value_factory=value_factory,
    )


def resolve_retry_execution_type_mapping_dict(
    type_mapping_value: RetryExecutionTypeMappingValue,
) -> RetryExecutionTypeMappingDict | None:
    if type_mapping_value is None:
        return None

    return RetryExecutionTypeMappingDict(
        types=resolve_exception_types(type_mapping_value),
        callback=resolve_callback(type_mapping_value),
        retry_strategy=resolve_retry_strategy(type_mapping_value),
    )


def resolve_exit_fatally_type_mappings(
    type_mapping_value: ExitFatallyTypeMappingValue,
) -> TypeMappings[Any]:
    type_mapping_dict = resolve_exit_fatally_type_mapping_dict(
        type_mapping_value
    )

    if type_mapping_dict is None:
        return {}

    return {
        exception_type: ErrorHandlingDefinition[None](
            handler=ExitErrorHandler(
                exit_code_factory=type_mapping_dict["exit_code_factory"]
            ),
            callback=type_mapping_dict["callback"],
        )
        for exception_type in type_mapping_dict["types"]
    }


def resolve_raise_exception_type_mappings(
    type_mapping_value: RaiseExceptionTypeMappingValue,
) -> TypeMappings[Any]:
    type_mapping_dict = resolve_raise_exception_type_mapping_dict(
        type_mapping_value
    )

    if type_mapping_dict is None:
        return {}

    return {
        exception_type: ErrorHandlingDefinition[None](
            handler=RaiseErrorHandler(
                exception_factory=type_mapping_dict["exception_factory"]
            ),
            callback=type_mapping_dict["callback"],
        )
        for exception_type in type_mapping_dict["types"]
    }


def resolve_continue_execution_type_mappings[T](
    type_mapping_value: ContinueExecutionTypeMappingValue[T],
) -> TypeMappings[T]:
    type_mapping_dict = resolve_continue_execution_type_mapping_dict(
        type_mapping_value
    )

    if type_mapping_dict is None:
        return {}

    return {
        exception_type: ErrorHandlingDefinition[T](
            handler=ContinueErrorHandler(
                value_factory=type_mapping_dict["value_factory"]
            ),
            callback=type_mapping_dict["callback"],
        )
        for exception_type in type_mapping_dict["types"]
    }


def resolve_retry_execution_type_mappings(
    type_mapping_value: RetryExecutionTypeMappingValue,
) -> TypeMappings[Any]:
    type_mapping_dict = resolve_retry_execution_type_mapping_dict(
        type_mapping_value
    )

    if type_mapping_dict is None:
        return {}

    return {
        exception_type: ErrorHandlingDefinition[None](
            handler=RetryErrorHandler(
                retry_strategy=type_mapping_dict.get("retry_strategy")
            ),
            callback=type_mapping_dict["callback"],
        )
        for exception_type in type_mapping_dict["types"]
    }


def error_handler_type_mappings[T](
    exit_fatally: ExitFatallyTypeMappingValue = None,
    raise_exception: RaiseExceptionTypeMappingValue = None,
    continue_execution: ContinueExecutionTypeMappingValue[T] = None,
    retry_execution: RetryExecutionTypeMappingValue = None,
) -> TypeMappings[T]:
    """Build type mappings for a `TypeMappingErrorHandler`.

    Each argument is either a sequence of exception types, using the
    default behaviour for that kind of handling, or a mapping produced by
    the corresponding `*_type_mapping` function. If an exception type
    appears under more than one kind of handling, the later argument wins.

    Args:
        exit_fatally: Exception types that should exit the process.
        raise_exception: Exception types that should be raised.
        continue_execution: Exception types that should continue with a
            value.
        retry_execution: Exception types that should be retried.

    Returns:
        the mapping from exception type to error handling definition.
    """
    val = {
        **resolve_exit_fatally_type_mappings(exit_fatally),
        **resolve_raise_exception_type_mappings(raise_exception),
        **resolve_continue_execution_type_mappings(continue_execution),
        **resolve_retry_execution_type_mappings(retry_execution),
    }
    return val


def exit_fatally_type_mapping(
    types: Sequence[type[BaseException]],
    exit_code_factory: Callable[[BaseException], int] = (
        default_exit_code_factory
    ),
    callback: Callable[[BaseException], None] | None = None,
) -> ExitFatallyTypeMappingDict:
    """Build an exit fatally type mapping for `error_handler_type_mappings`.

    Args:
        types: The exception types to exit on.
        exit_code_factory: Returns the exit code for an exception. Defaults
            to always returning 1.
        callback: Called with the exception before exiting.

    Returns:
        the type mapping.
    """
    return ExitFatallyTypeMappingDict(
        types=types,
        exit_code_factory=exit_code_factory,
        callback=callback if callback is not None else lambda _: None,
    )


def raise_exception_type_mapping(
    types: Sequence[type[BaseException]],
    exception_factory: Callable[[BaseException], BaseException],
    callback: Callable[[BaseException], None] | None = None,
) -> RaiseExceptionTypeMappingDict:
    """Build a raise exception type mapping for
    `error_handler_type_mappings`.

    Args:
        types: The exception types to raise on.
        exception_factory: Returns the exception to raise for a handled
            exception.
        callback: Called with the exception before raising.

    Returns:
        the type mapping.
    """
    return RaiseExceptionTypeMappingDict(
        types=types,
        exception_factory=exception_factory,
        callback=callback if callback is not None else lambda _: None,
    )


def continue_execution_type_mapping[T](
    types: Sequence[type[BaseException]],
    value_factory: Callable[[BaseException], T],
    callback: Callable[[BaseException], None] | None = None,
) -> ContinueExecutionTypeMappingDict[T]:
    """Build a continue execution type mapping for
    `error_handler_type_mappings`.

    Args:
        types: The exception types to continue on.
        value_factory: Returns the value to continue with for an exception.
        callback: Called with the exception before continuing.

    Returns:
        the type mapping.
    """
    return ContinueExecutionTypeMappingDict[T](
        types=types,
        value_factory=value_factory,
        callback=callback if callback is not None else lambda _: None,
    )


def retry_execution_type_mapping(
    types: Sequence[type[BaseException]],
    callback: Callable[[BaseException], None] | None = None,
    retry_strategy: RetryStrategy | None = None,
) -> RetryExecutionTypeMappingDict:
    """Build a retry execution type mapping for
    `error_handler_type_mappings`.

    Args:
        types: The exception types to retry on.
        callback: Called with the exception before retrying.
        retry_strategy: Decides how to retry for each exception. If `None`,
            retries immediately.

    Returns:
        the type mapping.
    """
    return RetryExecutionTypeMappingDict(
        types=types,
        callback=callback if callback is not None else lambda _: None,
        retry_strategy=retry_strategy,
    )


class TypeMappingErrorHandler[T](ErrorHandler[T]):
    """Error handler that delegates based on the type of the exception.

    The most specific mapped type in the exception's class hierarchy
    determines the handler used. Its callback is invoked before handling.
    Unmapped exceptions are passed to the default error handler.

    Args:
        type_mappings: Error handling definitions keyed by exception type,
            typically built with `error_handler_type_mappings`.
        default_error_handler: The handler for unmapped exceptions.
            Defaults to raising them.
    """

    def __init__(
        self,
        type_mappings: TypeMappings[T] = MappingProxyType({}),
        default_error_handler: ErrorHandler[T] = RaiseErrorHandler(),
    ):
        self.type_mappings = type_mappings
        self.default_error_handler = default_error_handler

    def handle(self, exception: BaseException) -> ErrorHandlerDecision[T]:
        for cls in type(exception).__mro__:
            if cls in self.type_mappings:
                definition = self.type_mappings[cls]
                callback = definition.callback
                handler = definition.handler

                callback(exception)

                return handler.handle(exception)

        return self.default_error_handler.handle(exception)


@warnings.deprecated("Use composition with ErrorHandlingService instead")
class ErrorHandlingServiceMixin[T = Any](Service[T], ABC):
    """Mixin applying error handling to a service's `_do_execute` method.

    Deprecated: use composition with `ErrorHandlingService` instead.

    Args:
        error_handler: The error handler to apply.
    """

    def __init__(
        self,
        error_handler: ErrorHandler[T],
    ):
        self._error_handler = error_handler

    async def execute(self) -> T:
        return await ErrorHandlingService.apply_error_handling(
            self._do_execute, self._error_handler
        )

    @abstractmethod
    async def _do_execute(self) -> T:
        raise NotImplementedError


class ErrorHandlingService[T = Any](Service[T]):
    """Service that applies an error handler to a wrapped service.

    Args:
        service: The service to execute. Passing a bare callable is
            deprecated; wrap it in a `CallableService`.
        callable: Deprecated; use `service` instead.
        error_handler: The error handler to apply to any exception raised.
        sleep: The function used to wait before retrying.

    Raises:
        TypeError: If neither `service` nor `callable` is provided.
    """

    def __init__(
        self,
        service: Service[T] | CallableServiceCallable[T] | None = None,
        *,
        callable: CallableServiceCallable[T] | None = None,
        error_handler: ErrorHandler[T],
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ):
        if callable is not None:
            warnings.warn(
                "Use 'service' instead of 'callable'",
                DeprecationWarning,
                stacklevel=2,
            )
            service = service or callable
        elif is_callable(service):
            warnings.warn(
                "Wrap callable in CallableService",
                DeprecationWarning,
                stacklevel=2,
            )

        if service is None:
            raise TypeError("service is required")

        self._service = CallableService.from_maybe_callable(service)
        self._error_handler = error_handler
        self._sleep = sleep

    @classmethod
    async def apply_error_handling(
        cls,
        run: Callable[[], Awaitable[T]],
        error_handler: ErrorHandler[T],
        *,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> T:
        """Run a callable, responding to any exception as the handler decides.

        Retry decisions run the callable again, after waiting if required;
        continue decisions return their value; raise decisions raise their
        exception; and exit decisions raise `SystemExit`.

        Args:
            run: The callable to run.
            error_handler: Decides how to respond to each exception.
            sleep: The function used to wait before retrying.

        Returns:
            the result of the callable, or the value of a continue decision.

        Raises:
            SystemExit: If the handler decides to exit.
            ValueError: If the handler returns an unknown decision.
        """
        while True:
            try:
                return await run()
            except BaseException as exception:
                decision = error_handler.handle(exception)
                match decision:
                    case RaiseErrorHandlerDecision(exception):
                        raise exception
                    case ContinueErrorHandlerDecision(value):
                        return value
                    case ExitErrorHandlerDecision(exit_code):
                        raise SystemExit(exit_code)
                    case RetryErrorHandlerDecision(wait_before_retry):
                        if (
                            wait_before_retry is not None
                            and wait_before_retry > timedelta()
                        ):
                            await sleep(wait_before_retry.total_seconds())

                        continue
                    case _:
                        raise ValueError(
                            f"Unknown error handler decision: {decision}"
                        )

    async def execute(self) -> T:
        return await self.apply_error_handling(
            self._service.execute,
            self._error_handler,
            sleep=self._sleep,
        )

    def __repr__(self):
        return f"{self.__class__.__name__}({self._service!r})"
