"""Random test data generators.

Generators are not seeded, so values differ between runs. Unless stated
otherwise, generated names are lowercase ASCII letters and hyphens, never
starting or ending with a hyphen, and generated mappings have between 1
and 10 entries mapping 10 character lowercase keys to 20 character
alphanumeric values.
"""

import random
import string
from collections.abc import Mapping, Sequence
from typing import Any
from uuid import uuid4


def _join_strings(chars: Sequence[str]) -> str:
    return "".join(chars)


def random_int(min: int = 0, max: int = 99999999999999) -> int:
    """Generate a random integer between `min` and `max` inclusive."""
    return random.randint(min, max)


def random_string(characters: str, length: int = 1) -> str:
    """Generate a random string of `length` drawn from `characters`."""
    return _join_strings(random.choices(characters, k=length))


def random_lowercase_ascii_alphabetics_string(length: int = 1) -> str:
    """Generate a random string of lowercase ASCII letters."""
    return random_string(string.ascii_lowercase, length)


def random_uppercase_ascii_alphabetics_string(length: int = 1) -> str:
    """Generate a random string of uppercase ASCII letters."""
    return random_string(string.ascii_uppercase, length)


def random_ascii_alphanumerics_string(length: int = 1) -> str:
    """Generate a random string of ASCII letters and digits."""
    return random_string(string.ascii_letters + string.digits, length)


def random_hyphenated_lowercase_ascii_alphabetics_string(
    length: int = 1,
) -> str:
    """Generate a random name of lowercase ASCII letters and hyphens."""
    if length == 1:
        return random_lowercase_ascii_alphabetics_string(length)

    first = random_lowercase_ascii_alphabetics_string()
    last = random_lowercase_ascii_alphabetics_string()
    rest = random_string(string.ascii_lowercase + "-", length - 2)

    return first + rest + last


def random_uuid4_string() -> str:
    """Generate a random UUID4 as a string."""
    return str(uuid4())


def random_event_category_name() -> str:
    """Generate a random 10 character category name."""
    return random_hyphenated_lowercase_ascii_alphabetics_string(length=10)


def random_event_stream_name() -> str:
    """Generate a random 42 character stream name."""
    return random_hyphenated_lowercase_ascii_alphabetics_string(length=42)


def random_event_name() -> str:
    """Generate a random 15 character event name."""
    return random_hyphenated_lowercase_ascii_alphabetics_string(length=15)


def random_event_id() -> str:
    """Generate a random event id, as a UUID4 string."""
    return random_uuid4_string()


def random_event_position() -> int:
    """Generate a random stream position."""
    return random_int(0, 9999999)


def random_event_sequence_number() -> int:
    """Generate a random sequence number."""
    return random_int(0, 999999999999999999)


def random_event_payload() -> Mapping[str, Any]:
    """Generate a random event payload mapping."""
    return {
        random_lowercase_ascii_alphabetics_string(
            length=10
        ): random_ascii_alphanumerics_string(length=20)
        for _ in range(random_int(1, 10))
    }


def random_event_metadata() -> Mapping[str, Any]:
    """Generate a random event metadata mapping."""
    return {
        random_lowercase_ascii_alphabetics_string(
            length=10
        ): random_ascii_alphanumerics_string(length=20)
        for _ in range(random_int(1, 10))
    }


def random_projection_id() -> str:
    """Generate a random projection id, as a UUID4 string."""
    return random_uuid4_string()


def random_projection_name() -> str:
    """Generate a random 15 character projection name."""
    return random_hyphenated_lowercase_ascii_alphabetics_string(length=15)


def random_projection_state() -> Mapping[str, Any]:
    """Generate a random projection state mapping."""
    return {
        random_lowercase_ascii_alphabetics_string(
            length=10
        ): random_ascii_alphanumerics_string(length=20)
        for _ in range(random_int(1, 10))
    }


def random_projection_metadata() -> Mapping[str, Any]:
    """Generate a random projection metadata mapping."""
    return {
        random_lowercase_ascii_alphabetics_string(
            length=10
        ): random_ascii_alphanumerics_string(length=20)
        for _ in range(random_int(1, 10))
    }


def random_node_id() -> str:
    """Generate a random node id, as a UUID4 string."""
    return random_uuid4_string()


def random_subscriber_group() -> str:
    """Generate a random 15 character subscriber group name."""
    return random_hyphenated_lowercase_ascii_alphabetics_string(length=15)


def random_subscriber_id() -> str:
    """Generate a random subscriber id, as a UUID4 string."""
    return random_uuid4_string()
