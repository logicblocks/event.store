from dataclasses import dataclass
from typing import Any, Mapping, Self, TypeVar

from logicblocks.event.projection import Projector
from logicblocks.event.types import (
    Codec,
    CodecOrMapping,
    EventSourceIdentifier,
    LogIdentifier,
)


@dataclass
class PositionMetadata(Codec):
    position: int

    def serialise(self) -> Mapping[str, Any]:
        return {"position": self.position}

    @classmethod
    def deserialise(cls, value: Mapping[str, Any]) -> Self:
        return cls(position=value.get("position", 0))


class WithPositionMetadataMixin:
    def initial_metadata_factory(self) -> PositionMetadata:
        return PositionMetadata(position=0)

    def update_metadata(
        self,
        state: Any,
        metadata: PositionMetadata,
        event: Any,
    ) -> PositionMetadata:
        return PositionMetadata(position=event.position)


P = TypeVar(
    "P",
    bound=Projector[CodecOrMapping, EventSourceIdentifier, PositionMetadata],
    infer_variance=True,
)


def with_position_metadata[P](projector: type[P], /):
    class WrappedProjectorWithPositionMetadata(
        WithPositionMetadataMixin, projector
    ):
        pass

    WrappedProjectorWithPositionMetadata.__name__ = projector.__name__
    WrappedProjectorWithPositionMetadata.__module__ = projector.__module__
    WrappedProjectorWithPositionMetadata.__doc__ = projector.__doc__

    return WrappedProjectorWithPositionMetadata


@with_position_metadata
class Test(Projector[Mapping[str, Any], LogIdentifier, PositionMetadata]):
    def initial_state_factory(self) -> Mapping[str, Any]:
        return {}

    def id_factory(
        self, state: Mapping[str, Any], source: LogIdentifier
    ) -> str:
        return source.json()


t = Test()
metadata = t.initial_metadata_factory()


class Test2(
    WithPositionMetadataMixin,
    Projector[Mapping[str, Any], LogIdentifier, PositionMetadata],
):
    def initial_state_factory(self) -> Mapping[str, Any]:
        return {}

    def id_factory(
        self, state: Mapping[str, Any], source: LogIdentifier
    ) -> str:
        return source.json()


t2 = Test2()
