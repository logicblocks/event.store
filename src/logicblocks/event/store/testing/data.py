import string
import random

from uuid import uuid4
from typing import Mapping, Any, Sequence


def _join_strings(chars: Sequence[str]) -> str:
    return "".join(chars)


def random_int(min: int = 0, max: int = 99999999999999) -> int:
    return random.randint(min, max)


def random_string(characters: str, length: int = 1) -> str:
    return _join_strings(random.choices(characters, k=length))


def random_lowercase_ascii_alphabetics_string(length: int = 1) -> str:
    return random_string(string.ascii_lowercase, length)


def random_uppercase_ascii_alphabetics_string(length: int = 1) -> str:
    return random_string(string.ascii_uppercase, length)


def random_ascii_alphanumerics_string(length: int = 1) -> str:
    return random_string(string.ascii_letters + string.digits, length)


def random_hyphenated_lowercase_ascii_alphabetics_string(
    length: int = 1,
) -> str:
    if length == 1:
        return random_lowercase_ascii_alphabetics_string(length)

    first = random_lowercase_ascii_alphabetics_string()
    last = random_lowercase_ascii_alphabetics_string()
    rest = random_string(string.ascii_lowercase + "-", length - 2)

    return first + rest + last


def random_uuid4_string() -> str:
    return uuid4().hex


def random_event_category_name() -> str:
    return random_hyphenated_lowercase_ascii_alphabetics_string(length=10)


def random_event_stream_name() -> str:
    return random_hyphenated_lowercase_ascii_alphabetics_string(length=42)


def random_event_name() -> str:
    return random_hyphenated_lowercase_ascii_alphabetics_string(length=15)


def random_event_id() -> str:
    return random_uuid4_string()


def random_event_payload() -> Mapping[str, Any]:
    return {
        random_lowercase_ascii_alphabetics_string(
            length=10
        ): random_ascii_alphanumerics_string(length=20)
        for _ in range(random_int(1, 10))
    }