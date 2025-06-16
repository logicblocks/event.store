from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, TypedDict, cast

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
    def continue_execution[R = None](
        *, value: R = None
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


class ExitErrorHandler(ErrorHandler[Any]):
    def __init__(self, exit_code: int = 1):
        self.exit_code = exit_code

    def handle(self, exception: BaseException) -> ErrorHandlerDecision[Any]:
        return ErrorHandlerDecision.exit_fatally(exit_code=self.exit_code)


class RaiseErrorHandler(ErrorHandler[Any]):
    def __init__(
        self,
        exception_factory: Callable[[BaseException], BaseException]
        | None = None,
    ):
        self.exception_factory = exception_factory

    def handle(self, exception: BaseException) -> ErrorHandlerDecision[Any]:
        resolved_exception = exception
        if self.exception_factory is not None:
            resolved_exception = self.exception_factory(exception)

        return ErrorHandlerDecision.raise_exception(resolved_exception)


class ContinueErrorHandler[T = None](ErrorHandler[T]):
    def __init__(
        self,
        value_factory: Callable[[BaseException], T] = lambda _: None,
    ):
        self.value_factory = value_factory

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


class TypeMappingDict(TypedDict):
    types: Sequence[type[BaseException]]
    callback: Callable[[BaseException], None]


type TypeMappingValue = Sequence[type[BaseException]] | TypeMappingDict | None


class TypeMappingsDict(TypedDict):
    exit_fatally: TypeMappingValue
    raise_exception: TypeMappingValue
    continue_execution: TypeMappingValue
    retry_execution: TypeMappingValue


def error_handler_type_mappings(
    exit_fatally: TypeMappingValue = None,
    raise_exception: TypeMappingValue = None,
    continue_execution: TypeMappingValue = None,
    retry_execution: TypeMappingValue = None,
) -> TypeMappingsDict:
    return TypeMappingsDict(
        exit_fatally=exit_fatally,
        raise_exception=raise_exception,
        continue_execution=continue_execution,
        retry_execution=retry_execution,
    )


def error_handler_type_mapping(
    types: Sequence[type[BaseException]],
    callback: Callable[[BaseException], None] | None = None,
) -> TypeMappingDict:
    return TypeMappingDict(
        types=types,
        callback=callback if callback is not None else lambda _: None,
    )


def decision_factory_for(
    decision_type: str,
) -> Callable[[BaseException], ErrorHandlerDecision[Any]]:
    match decision_type:
        case "exit_fatally":
            return lambda _: ErrorHandlerDecision.exit_fatally()
        case "raise_exception":
            return lambda ex: ErrorHandlerDecision.raise_exception(ex)
        case "continue_execution":
            return lambda _: ErrorHandlerDecision.continue_execution()
        case "retry_execution":
            return lambda _: ErrorHandlerDecision.retry_execution()
        case _:
            raise ValueError(f"Unknown decision type: {decision_type}")


def normalise_type_mapping_value(
    type_mapping_value: TypeMappingValue,
) -> TypeMappingDict:
    if isinstance(type_mapping_value, Sequence):
        return error_handler_type_mapping(types=type_mapping_value)
    elif type_mapping_value is None:
        return error_handler_type_mapping(types=[])
    else:
        return type_mapping_value


type DecisionFactory[T] = Callable[[BaseException], ErrorHandlerDecision[T]]


def raise_decision_factory(
    exception: BaseException,
) -> ErrorHandlerDecision[Any]:
    return ErrorHandlerDecision.raise_exception(exception)


class TypeMappingHandlersDict(TypedDict):
    factory: DecisionFactory[Any]
    callback: Callable[[BaseException], None]


class TypeMappingErrorHandler[T = Any](ErrorHandler[T]):
    def __init__(
        self,
        type_mappings: TypeMappingsDict | None = None,
        default_decision_factory: DecisionFactory[T] | None = None,
    ):
        defaulted_type_mappings = (
            type_mappings
            if type_mappings is not None
            else error_handler_type_mappings()
        )
        normalised_type_mappings: Mapping[str, TypeMappingDict] = {
            decision_type: normalise_type_mapping_value(
                cast(TypeMappingDict, type_mapping)
            )
            for decision_type, type_mapping in defaulted_type_mappings.items()
        }

        self.type_definitions = {
            exception_type: TypeMappingHandlersDict(
                factory=decision_factory_for(decision_type),
                callback=type_mapping["callback"],
            )
            for decision_type, type_mapping in normalised_type_mappings.items()
            for exception_type in type_mapping["types"]
        }
        self.default_decision_factory = (
            default_decision_factory
            if default_decision_factory is not None
            else raise_decision_factory
        )

    def handle(self, exception: BaseException) -> ErrorHandlerDecision[T]:
        for cls in type(exception).__mro__:
            if cls in self.type_definitions:
                type_definition = self.type_definitions[cls]
                callback = type_definition["callback"]
                factory = type_definition["factory"]

                callback(exception)

                return factory(exception)

        return self.default_decision_factory(exception)


class ErrorHandlingServiceMixin[T = None](Service[T], ABC):
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
