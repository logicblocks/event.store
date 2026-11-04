---
title: Custom payload and state types
description: Publish events with custom payload types and store projections with custom state types.
---

Event payloads, event metadata and projection state are stored as JSON.
Plain dictionaries, lists, strings, numbers, booleans and `None` work as they
are. To use your own types, give them methods that convert to and from JSON
values. This guide covers the JSON types and protocols in
`logicblocks.event.types`, the conversion functions, and how they apply to
events and projections.

## JSON values

[`JsonValue`](/event.store/reference/types/#logicblocks.event.types.JsonValue)
is the type of any JSON value. It is built from three aliases:

- [`JsonPrimitive`](/event.store/reference/types/#logicblocks.event.types.JsonPrimitive):
  `str | int | float | bool | None`;
- [`JsonArray`](/event.store/reference/types/#logicblocks.event.types.JsonArray):
  a `Sequence` of JSON values;
- [`JsonObject`](/event.store/reference/types/#logicblocks.event.types.JsonObject):
  a `Mapping` from `str` to JSON values.

`JsonValue` is a `type` statement alias, so it can't be passed where a class
is expected.
[`JsonValueType`](/event.store/reference/types/#logicblocks.event.types.JsonValueType)
is its runtime value. Pass it, for example as `state_type`, to ask for an
untyped JSON value. It is the default wherever the library takes a type to
deserialise into.

## Checking values

The predicates
[`is_json_value`](/event.store/reference/types/#logicblocks.event.types.is_json_value),
[`is_json_object`](/event.store/reference/types/#logicblocks.event.types.is_json_object),
[`is_json_array`](/event.store/reference/types/#logicblocks.event.types.is_json_array)
and
[`is_json_primitive`](/event.store/reference/types/#logicblocks.event.types.is_json_primitive)
check whether a value has that shape. They check containers recursively and
act as type guards.

```python
from logicblocks.event.types import (
    is_json_array,
    is_json_object,
    is_json_primitive,
    is_json_value,
)

print(is_json_object({"name": "Joe", "tags": [1, None]}))  # True
print(is_json_object({1: "one"}))  # False: keys must be strings
print(is_json_array("abc"))  # False: strings aren't arrays
print(is_json_array((1, 2)))  # True: any sequence counts
print(is_json_value({1, 2}))  # False: sets aren't JSON
print(is_json_primitive([1]))  # False
```

## Protocols

A custom type takes part in serialisation by implementing one of these
protocols:

- [`JsonValueSerialisable`](/event.store/reference/types/#logicblocks.event.types.JsonValueSerialisable):
  an instance method `serialise(self, fallback) -> JsonValue`;
- [`JsonValueDeserialisable`](/event.store/reference/types/#logicblocks.event.types.JsonValueDeserialisable):
  a class method `deserialise(cls, value, fallback) -> Self`;
- [`JsonValueConvertible`](/event.store/reference/types/#logicblocks.event.types.JsonValueConvertible):
  both.

The string equivalents,
[`StringSerialisable`](/event.store/reference/types/#logicblocks.event.types.StringSerialisable),
[`StringDeserialisable`](/event.store/reference/types/#logicblocks.event.types.StringDeserialisable)
and
[`StringConvertible`](/event.store/reference/types/#logicblocks.event.types.StringConvertible),
have the same methods but convert to and from `str`. The library uses them
for event names.

The `fallback` argument is a function to call for any value your method
can't handle, usually a nested value. `serialise` passes the value to it.
`deserialise` passes the target type and the value.

Four aliases name the values the library accepts:

- [`JsonPersistable`](/event.store/reference/types/#logicblocks.event.types.JsonPersistable):
  `JsonValueConvertible | JsonValue`, for values that are stored and read
  back, such as payloads, metadata and projection state;
- [`JsonLoggable`](/event.store/reference/types/#logicblocks.event.types.JsonLoggable):
  `JsonValueSerialisable | JsonValue`, for values that only need writing out;
- [`StringPersistable`](/event.store/reference/types/#logicblocks.event.types.StringPersistable):
  `StringConvertible | str`, for event names;
- [`StringLoggable`](/event.store/reference/types/#logicblocks.event.types.StringLoggable):
  `StringSerialisable | str`.

The protocols are runtime checkable, so your types don't need to inherit
from them. A runtime `isinstance` check only looks for a method with the
right name, though. Any object with a `serialise` method passes as
`JsonValueSerialisable` and `StringSerialisable` alike, whatever that method
returns.

## Converting values

[`serialise_to_json_value`](/event.store/reference/types/#logicblocks.event.types.serialise_to_json_value)
converts a value to JSON:

1. if the value has a `serialise` method, it returns the result of calling
   it;
2. otherwise, if the value is already a JSON value, it returns it unchanged;
3. otherwise, it returns `fallback(value)`.

It doesn't look inside containers. A dictionary that holds a custom object
isn't a JSON value, so the whole dictionary goes to the fallback. Make the
custom type the top-level value, and serialise nested values in its own
`serialise` method.

[`deserialise_from_json_value`](/event.store/reference/types/#logicblocks.event.types.deserialise_from_json_value)
takes the type to convert to and a value. It handles:

- `None`, `str`, `int`, `float` and `bool`, when the value is already an
  instance of that type;
- sequence and mapping types, including parameterised ones such as
  `list[int]` or `Mapping[str, int]`;
- `JsonValueType`, which accepts any JSON value;
- classes with a `deserialise` class method, which it calls.

Anything else goes to the fallback. Some cases to be aware of:

- For parameterised sequences and mappings, each item is checked against the
  item type, but the value is returned as it is. Items aren't converted, so
  `Mapping[str, Money]` returns a mapping of dictionaries, not of `Money`.
- Union types, such as `str | None`, aren't supported and go to the
  fallback.
- An `int` isn't accepted where a `float` is expected.

[`serialise_to_string`](/event.store/reference/types/#logicblocks.event.types.serialise_to_string)
and
[`deserialise_from_string`](/event.store/reference/types/#logicblocks.event.types.deserialise_from_string)
do the same for strings. Strings pass through, objects with `serialise` or
`deserialise` methods are converted with them, and anything else goes to the
fallback.

The library provides these fallbacks:

- [`raising_serialisation_fallback`](/event.store/reference/types/#logicblocks.event.types.raising_serialisation_fallback)
  and
  [`raising_deserialisation_fallback`](/event.store/reference/types/#logicblocks.event.types.raising_deserialisation_fallback)
  raise a `ValueError`. They are the defaults, also available as
  [`default_serialisation_fallback`](/event.store/reference/types/#logicblocks.event.types.default_serialisation_fallback)
  and
  [`default_deserialisation_fallback`](/event.store/reference/types/#logicblocks.event.types.default_deserialisation_fallback).
- [`str_serialisation_fallback`](/event.store/reference/types/#logicblocks.event.types.str_serialisation_fallback)
  converts the value with `str()`. The library uses it when logging, where a
  lossy representation is acceptable.

The storage adapters always use the raising defaults.

```python
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any, Self

from logicblocks.event.types import (
    JsonValue,
    deserialise_from_json_value,
    serialise_to_json_value,
    str_serialisation_fallback,
)


@dataclass(frozen=True)
class Money:
    amount: int
    currency: str

    def serialise(self, fallback: Callable[[object], JsonValue]) -> JsonValue:
        return {"amount": self.amount, "currency": self.currency}

    @classmethod
    def deserialise(
        cls, value: JsonValue, fallback: Callable[[type[Any], JsonValue], Any]
    ) -> Self:
        if not isinstance(value, Mapping):
            return fallback(cls, value)
        return cls(amount=int(value["amount"]), currency=str(value["currency"]))


price = Money(amount=1000, currency="GBP")

print(serialise_to_json_value(price))
# {'amount': 1000, 'currency': 'GBP'}

print(deserialise_from_json_value(Money, {"amount": 1000, "currency": "GBP"}))
# Money(amount=1000, currency='GBP')

try:
    serialise_to_json_value({"price": price})
except ValueError as error:
    print(error)
    # Cannot serialise {'price': Money(amount=1000, currency='GBP')}.

print(repr(serialise_to_json_value({"price": price}, str_serialisation_fallback)))
# "{'price': Money(amount=1000, currency='GBP')}"
```

The last call shows the `str` fallback turning the whole dictionary into a
string.

## Event type parameters

[`NewEvent`](/event.store/reference/types/#logicblocks.event.types.NewEvent)
and
[`StoredEvent`](/event.store/reference/types/#logicblocks.event.types.StoredEvent)
take three type parameters, `Name`, `Payload` and `Metadata`. They default to
`str`, `JsonValue` and `JsonValue`, which is why `NewEvent(...)` works
without them. When publishing, the store requires `Name` to be
`StringPersistable`, and `Payload` and `Metadata` to be `JsonPersistable`.

Publishing returns `StoredEvent`s with the same type parameters as the
events you published, holding your original objects. Everything read back
from the store, through `read`, `iterate`, `latest` or a projector, is a
`StoredEvent[str, JsonValue, JsonValue]` holding the serialised form.

## Publishing custom payloads

This example publishes an event whose payload is a dataclass, then reads it
back and converts the payload again:

```python
import asyncio
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any, Self

from logicblocks.event.store import EventStore, adapters
from logicblocks.event.types import (
    JsonValue,
    NewEvent,
    deserialise_from_json_value,
)


@dataclass(frozen=True)
class ProfileCreated:
    name: str
    email: str

    def serialise(self, fallback: Callable[[object], JsonValue]) -> JsonValue:
        return {"name": self.name, "email": self.email}

    @classmethod
    def deserialise(
        cls, value: JsonValue, fallback: Callable[[type[Any], JsonValue], Any]
    ) -> Self:
        if not isinstance(value, Mapping):
            return fallback(cls, value)
        return cls(name=str(value["name"]), email=str(value["email"]))


async def main():
    store = EventStore(adapters.InMemoryEventStorageAdapter())
    stream = store.stream(category="profiles", stream="joe.bloggs")

    event = NewEvent[str, ProfileCreated, JsonValue](
        name="profile-created",
        payload=ProfileCreated(name="Joe Bloggs", email="joe@example.com"),
        metadata=None,
    )

    published = await stream.publish(events=[event])
    print(repr(published[0].payload))
    # ProfileCreated(name='Joe Bloggs', email='joe@example.com')

    stored = await stream.latest()
    print(repr(stored.payload))
    # {'name': 'Joe Bloggs', 'email': 'joe@example.com'}

    print(repr(deserialise_from_json_value(ProfileCreated, stored.payload)))
    # ProfileCreated(name='Joe Bloggs', email='joe@example.com')


asyncio.run(main())
```

If a payload or metadata value can't be serialised, `publish` raises a
`ValueError` and none of the events in that call are stored.

Projector handlers receive events read from the store, so their payloads are
JSON values. Convert them in the handler if you want the custom type, for
example with `deserialise_from_json_value(ProfileCreated, event.payload)`.

## Custom projection state

Projection state and metadata are serialised the same way.
[`serialise_projection`](/event.store/reference/types/#logicblocks.event.types.serialise_projection)
returns a copy of a projection with its state and metadata converted by
`serialise_to_json_value`.
[`deserialise_projection`](/event.store/reference/types/#logicblocks.event.types.deserialise_projection)
reverses it, given a `state_type` and `metadata_type`, using
`deserialise_from_json_value`. Both default to `JsonValueType`. The projection
storage adapters call these functions for you.

A projection's `source` is a
[`Projectable`](/event.store/reference/types/#logicblocks.event.types.Projectable),
which is an alias for `EventSourceIdentifier`. The source is serialised by the
identifier's own `serialise` method and isn't affected by these functions.

To read a projection back as your type, pass `state_type` (and
`metadata_type`, if needed) to the
[`ProjectionStore`](/event.store/reference/projection/#logicblocks.event.projection.ProjectionStore)
methods `locate`, `load` and `search`. Without it, you get the JSON form.

```python
import asyncio
from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from typing import Any, Self

from logicblocks.event.projection import (
    InMemoryProjectionStorageAdapter,
    ProjectionStore,
    Projector,
)
from logicblocks.event.store import EventStore, adapters
from logicblocks.event.types import JsonValue, NewEvent, StreamIdentifier


@dataclass(frozen=True)
class Profile:
    name: str | None = None
    dob: str | None = None

    def serialise(self, fallback: Callable[[object], JsonValue]) -> JsonValue:
        return {"name": self.name, "dob": self.dob}

    @classmethod
    def deserialise(
        cls, value: JsonValue, fallback: Callable[[type[Any], JsonValue], Any]
    ) -> Self:
        if not isinstance(value, Mapping):
            return fallback(cls, value)
        return cls(name=value.get("name"), dob=value.get("dob"))


class ProfileProjector(Projector[StreamIdentifier, Profile]):
    def initial_state_factory(self) -> Profile:
        return Profile()

    def initial_metadata_factory(self) -> JsonValue:
        return {}

    def id_factory(self, state: Profile, source: StreamIdentifier) -> str:
        return source.stream

    def profile_created(self, state: Profile, event) -> Profile:
        return replace(state, name=event.payload["name"])

    def date_of_birth_set(self, state: Profile, event) -> Profile:
        return replace(state, dob=event.payload["dob"])


async def main():
    event_store = EventStore(adapters.InMemoryEventStorageAdapter())
    projection_store = ProjectionStore(
        adapter=InMemoryProjectionStorageAdapter()
    )

    stream = event_store.stream(category="profiles", stream="joe.bloggs")
    await stream.publish(
        events=[
            NewEvent(
                name="profile-created",
                payload={"name": "Joe Bloggs"},
                metadata=None,
            ),
            NewEvent(
                name="date-of-birth-set",
                payload={"dob": "1992-07-10"},
                metadata=None,
            ),
        ]
    )

    projection = await ProfileProjector().project(source=stream)
    await projection_store.save(projection=projection)

    typed = await projection_store.locate(
        source=stream.identifier, name="profile", state_type=Profile
    )
    print(typed.state)
    # Profile(name='Joe Bloggs', dob='1992-07-10')

    untyped = await projection_store.locate(
        source=stream.identifier, name="profile"
    )
    print(untyped.state)
    # {'name': 'Joe Bloggs', 'dob': '1992-07-10'}


asyncio.run(main())
```

[`ProjectionEventProcessor`](/event.store/reference/processing/#logicblocks.event.processing.ProjectionEventProcessor)
takes `state_type` and `metadata_type` for the same reason. It loads the
stored projection before applying each event, so without `state_type` your
handlers receive a dictionary instead of your state type.

Queries against stored projections, such as a `FilterClause` on
`Path("state", "name")`, run against the serialised form. Use the keys your
`serialise` method writes.
