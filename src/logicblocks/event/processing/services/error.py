from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, NotRequired, TypedDict

from .types import Service


class ErrorHandlerDecision[T]:
    @staticmethod
    def exit_fatally(*, exit_code: int = 1) -> "ExitErrorHandlerDecision":
        return ExitErrorHandlerDecision(exit_code=exit_code)

    @staticmethod
    def raise_exception(
        exception: BaseException,
    ) -> "RaiseErrorHandlerDecision":
        return RaiseErrorHandlerDecision(exception=exception)

    @staticmethod
    def continue_execution[R](
        *, value: R
    ) -> "ContinueErrorHandlerDecision[R]":
        return ContinueErrorHandlerDecision[R](value=value)

    @staticmethod
    def retry_execution() -> "RetryErrorHandlerDecision":
        return RetryErrorHandlerDecision()


@dataclass(frozen=True)
class ExitErrorHandlerDecision(ErrorHandlerDecision[Any]):
    exit_code: int


@dataclass(frozen=True)
class RaiseErrorHandlerDecision(ErrorHandlerDecision[Any]):
    exception: BaseException


@dataclass(frozen=True)
class ContinueErrorHandlerDecision[T](ErrorHandlerDecision[T]):
    value: T


@dataclass(frozen=True)
class RetryErrorHandlerDecision(ErrorHandlerDecision[Any]):
    pass


class ErrorHandler[T](ABC):
    @abstractmethod
    def handle(self, exception: BaseException) -> ErrorHandlerDecision[T]:
        raise NotImplementedError


def default_exit_code_factory(_: BaseException) -> int:
    return 1


def default_exception_factory(exception: BaseException) -> BaseException:
    return exception


class ExitErrorHandler(ErrorHandler[Any]):
    def __init__(
        self,
        exit_code_factory: Callable[[BaseException], int] = (
            default_exit_code_factory
        ),
    ):
        self.exit_code_factory = exit_code_factory

    def handle(self, exception: BaseException) -> ErrorHandlerDecision[Any]:
        return ErrorHandlerDecision.exit_fatally(
            exit_code=self.exit_code_factory(exception)
        )


class RaiseErrorHandler(ErrorHandler[Any]):
    def __init__(
        self,
        exception_factory: Callable[[BaseException], BaseException] = (
            default_exception_factory
        ),
    ):
        self.exception_factory = exception_factory

    def handle(self, exception: BaseException) -> ErrorHandlerDecision[Any]:
        resolved_exception = exception
        if self.exception_factory is not None:
            resolved_exception = self.exception_factory(exception)

        return ErrorHandlerDecision.raise_exception(resolved_exception)


class ContinueErrorHandler[T](ErrorHandler[T]):
    def __init__(
        self,
        value_factory: Callable[[BaseException], T],
    ):
        self.value_factory: Callable[[BaseException], T] = value_factory

    def handle(self, exception: BaseException) -> ErrorHandlerDecision[T]:
        if isinstance(exception, Exception):
            return ErrorHandlerDecision.continue_execution(
                value=self.value_factory(exception)
            )
        else:
            return ErrorHandlerDecision.raise_exception(exception)


class RetryErrorHandler(ErrorHandler[Any]):
    def handle(self, exception: BaseException) -> ErrorHandlerDecision[Any]:
        if isinstance(exception, Exception):
            return ErrorHandlerDecision.retry_execution()
        else:
            return ErrorHandlerDecision.raise_exception(exception)


class ErrorHandlingDefinition[T]:
    def __init__(
        self,
        handler: ErrorHandler[T],
        callback: Callable[[BaseException], None] = lambda _: None,
    ):
        self.handler = handler
        self.callback = callback


type TypeMappings[T] = Mapping[type[BaseException], ErrorHandlingDefinition[T]]


class TypeMappingDict(TypedDict):
    types: Sequence[type[BaseException]]
    callback: NotRequired[Callable[[BaseException], None]]


class ExitFatallyTypeMappingDict(TypeMappingDict):
    exit_code_factory: NotRequired[Callable[[BaseException], int]]


class RaiseExceptionTypeMappingDict(TypeMappingDict):
    exception_factory: NotRequired[Callable[[BaseException], BaseException]]


class ContinueExecutionTypeMappingDict[T](TypeMappingDict):
    value_factory: Callable[[BaseException], T]


class RetryExecutionTypeMappingDict(TypeMappingDict):
    pass


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
            handler=RetryErrorHandler(),
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
    return ContinueExecutionTypeMappingDict[T](
        types=types,
        value_factory=value_factory,
        callback=callback if callback is not None else lambda _: None,
    )


def retry_execution_type_mapping(
    types: Sequence[type[BaseException]],
    callback: Callable[[BaseException], None] | None = None,
) -> RetryExecutionTypeMappingDict:
    return RetryExecutionTypeMappingDict(
        types=types,
        callback=callback if callback is not None else lambda _: None,
    )


class TypeMappingErrorHandler[T](ErrorHandler[T]):
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


class ErrorHandlingServiceMixin[T = Any](Service[T], ABC):
    def __init__(
        self,
        error_handler: ErrorHandler[T],
    ):
        self._error_handler = error_handler

    async def execute(self) -> T:
        while True:
            try:
                return await self._do_execute()
            except BaseException as exception:
                decision = self._error_handler.handle(exception)
                match decision:
                    case RaiseErrorHandlerDecision(exception):
                        raise exception
                    case ContinueErrorHandlerDecision(value):
                        return value
                    case ExitErrorHandlerDecision(exit_code):
                        raise SystemExit(exit_code)
                    case RetryErrorHandlerDecision():
                        continue
                    case _:
                        raise ValueError(
                            f"Unknown error handler decision: {decision}"
                        )

    @abstractmethod
    async def _do_execute(self) -> T:
        raise NotImplementedError


class ErrorHandlingService[T = Any](ErrorHandlingServiceMixin[T], Service[T]):
    def __init__(
        self,
        callable: Callable[[], Awaitable[T]],
        error_handler: ErrorHandler[T],
    ):
        super().__init__(error_handler=error_handler)
        self._callable = callable

    async def _do_execute(self) -> T:
        return await self._callable()
