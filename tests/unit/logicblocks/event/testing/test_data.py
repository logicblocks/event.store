import re
import sys

import pytest

from logicblocks.event.testing import data


def test_generates_random_integers():
    ints = [data.random_int() for _ in range(100)]

    assert all(isinstance(i, int) for i in ints)
    assert len(set(ints)) == 100


def test_generates_random_integers_with_specified_minimum():
    ints = [data.random_int(min=10000) for _ in range(100)]

    assert all(isinstance(i, int) for i in ints)
    assert len(set(ints)) == 100
    assert all(i >= 10000 for i in ints)


def test_generates_random_integers_with_specified_maximum():
    ints = [data.random_int(max=200000) for _ in range(100)]

    assert all(isinstance(i, int) for i in ints)
    assert len(set(ints)) == 100
    assert all(i <= 200000 for i in ints)


def test_generates_random_characters_from_provided_character_string_by_default():
    strings = [data.random_string("abc") for _ in range(100)]

    assert all(isinstance(s, str) for s in strings)
    assert set(strings) == {"a", "b", "c"}


def test_generates_random_strings_of_specified_length():
    strings = [data.random_string("abc", length=100) for _ in range(100)]

    assert all(isinstance(s, str) for s in strings)
    assert all(len(s) == 100 for s in strings)
    assert all(
        char in {"a", "b", "c"} for string in strings for char in string
    )


def test_generates_random_lowercase_ascii_alphabetics_characters_by_default():
    strings = [
        data.random_lowercase_ascii_alphabetics_string() for _ in range(100)
    ]

    assert all(isinstance(s, str) for s in strings)
    assert all(s.islower() for s in strings)
    assert all(len(s) == 1 for s in strings)
    assert all(char in set("abcdefghijklmnopqrstuvwxyz") for char in strings)


def test_generates_random_lowercase_ascii_alphabetics_strings_of_specified_length():
    strings = [
        data.random_lowercase_ascii_alphabetics_string(length=100)
        for _ in range(100)
    ]

    assert all(isinstance(s, str) for s in strings)
    assert all(all(char.islower() for char in s) for s in strings)
    assert all(len(s) == 100 for s in strings)
    assert all(
        char in set("abcdefghijklmnopqrstuvwxyz")
        for string in strings
        for char in string
    )


def test_generates_random_uppercase_ascii_alphabetics_characters_by_default():
    strings = [
        data.random_uppercase_ascii_alphabetics_string() for _ in range(100)
    ]

    assert all(isinstance(s, str) for s in strings)
    assert all(len(s) == 1 for s in strings)
    assert all(char in set("ABCDEFGHIJKLMNOPQRSTUVWXYZ") for char in strings)


def test_generates_random_uppercase_ascii_alphabetics_strings_of_specified_length():
    strings = [
        data.random_uppercase_ascii_alphabetics_string(length=100)
        for _ in range(100)
    ]

    assert all(isinstance(s, str) for s in strings)
    assert all(len(s) == 100 for s in strings)
    assert all(
        char in set("ABCDEFGHIJKLMNOPQRSTUVWXYZ")
        for string in strings
        for char in string
    )


def test_generates_random_ascii_alphanumerics_characters_by_default():
    strings = [data.random_ascii_alphanumerics_string() for _ in range(100)]

    assert all(isinstance(s, str) for s in strings)
    assert all(len(s) == 1 for s in strings)
    assert all(
        char
        in set(
            "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
        )
        for char in strings
    )


def test_generates_random_ascii_alphanumerics_strings_of_specified_length():
    strings = [
        data.random_ascii_alphanumerics_string(length=100) for _ in range(100)
    ]

    assert all(isinstance(s, str) for s in strings)
    assert all(len(s) == 100 for s in strings)
    assert all(
        char
        in set(
            "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
        )
        for string in strings
        for char in string
    )


def test_generates_random_hyphenated_lowercase_ascii_alphabetics_characters_by_default():
    strings = [
        data.random_hyphenated_lowercase_ascii_alphabetics_string()
        for _ in range(100)
    ]

    assert all(isinstance(s, str) for s in strings)
    assert all(len(s) == 1 for s in strings)
    assert all(char in set("abcdefghijklmnopqrstuvwxyz") for char in strings)


def test_generates_random_hyphenated_lowercase_ascii_alphabetics_strings_of_specified_length():
    strings = [
        data.random_hyphenated_lowercase_ascii_alphabetics_string(length=100)
        for _ in range(100)
    ]

    assert all(isinstance(s, str) for s in strings)
    assert all(len(s) == 100 for s in strings)
    assert all(
        string[0] in set("abcdefghijklmnopqrstuvwxyz") for string in strings
    )
    assert all(
        string[-1] in set("abcdefghijklmnopqrstuvwxyz") for string in strings
    )
    assert all(
        char in set("abcdefghijklmnopqrstuvwxyz-")
        for string in strings
        for char in string
    )


def test_generates_random_uuid4_strings():
    strings = [data.random_uuid4_string() for _ in range(100)]

    assert all(isinstance(s, str) for s in strings)
    assert len(set(strings)) == 100
    assert all(
        re.fullmatch(
            "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",
            string,
        )
        is not None
        for string in strings
    )


def test_generates_random_event_category_names():
    names = [data.random_event_category_name() for _ in range(100)]

    assert all(isinstance(n, str) for n in names)
    assert len(set(names)) == 100
    assert all(
        re.fullmatch("[a-z][a-z-]{8}[a-z]", name) is not None for name in names
    )


def test_generates_random_event_stream_names():
    names = [data.random_event_stream_name() for _ in range(100)]

    assert all(isinstance(n, str) for n in names)
    assert len(set(names)) == 100
    assert all(
        re.fullmatch("[a-z][a-z-]{40}[a-z]", name) is not None
        for name in names
    )


def test_generates_random_event_names():
    names = [data.random_event_name() for _ in range(100)]

    assert all(isinstance(n, str) for n in names)
    assert len(set(names)) == 100
    assert all(
        re.fullmatch("[a-z][a-z-]{13}[a-z]", name) is not None
        for name in names
    )


def test_generates_random_event_ids():
    ids = [data.random_event_id() for _ in range(100)]

    assert all(isinstance(s, str) for s in ids)
    assert len(set(ids)) == 100
    assert all(
        re.fullmatch(
            "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",
            string,
        )
        is not None
        for string in ids
    )


def test_generates_random_event_positions():
    positions = [data.random_event_position() for _ in range(100)]

    assert all(isinstance(i, int) for i in positions)
    assert len(set(positions)) == 100
    assert all(0 <= i <= 9999999 for i in positions)


def test_generates_random_event_sequence_numbers():
    sequence_numbers = [
        data.random_event_sequence_number() for _ in range(100)
    ]

    assert all(isinstance(i, int) for i in sequence_numbers)
    assert len(set(sequence_numbers)) == 100
    assert all(0 <= i <= 999999999999999999 for i in sequence_numbers)


def test_generates_random_event_payloads():
    payloads = [data.random_event_payload() for _ in range(100)]

    assert all(isinstance(p, dict) for p in payloads)

    key_lists = [list(map(str, payload.keys())) for payload in payloads]
    val_lists = [list(map(str, payload.values())) for payload in payloads]

    all_keys = list(key for key_list in key_lists for key in key_list)
    all_vals = list(val for val_list in val_lists for val in val_list)
    unique_keys = set(key for key_list in key_lists for key in key_list)
    unique_vals = set(val for val_list in val_lists for val in val_list)

    assert all(1 <= len(key_list) <= 10 for key_list in key_lists)
    assert len(all_keys) == len(unique_keys)
    assert len(all_vals) == len(unique_vals)


def test_generates_random_projection_ids():
    ids = [data.random_projection_id() for _ in range(100)]

    assert all(isinstance(s, str) for s in ids)
    assert len(set(ids)) == 100
    assert all(
        re.fullmatch(
            "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",
            string,
        )
        is not None
        for string in ids
    )


def test_generates_random_projection_names():
    names = [data.random_projection_name() for _ in range(100)]

    assert all(isinstance(n, str) for n in names)
    assert len(set(names)) == 100
    assert all(
        re.fullmatch("[a-z][a-z-]{13}[a-z]", name) is not None
        for name in names
    )


def test_generates_random_projection_states():
    states = [data.random_projection_state() for _ in range(100)]

    assert all(isinstance(p, dict) for p in states)

    key_lists = [list(map(str, state.keys())) for state in states]
    val_lists = [list(map(str, state.values())) for state in states]

    all_keys = list(key for key_list in key_lists for key in key_list)
    all_vals = list(val for val_list in val_lists for val in val_list)
    unique_keys = set(key for key_list in key_lists for key in key_list)
    unique_vals = set(val for val_list in val_lists for val in val_list)

    assert all(1 <= len(key_list) <= 10 for key_list in key_lists)
    assert len(all_keys) == len(unique_keys)
    assert len(all_vals) == len(unique_vals)


def test_generates_random_projection_versions():
    versions = [data.random_projection_version() for _ in range(100)]

    assert all(isinstance(i, int) for i in versions)
    assert len(set(versions)) == 100
    assert all(0 <= i <= 100000 for i in versions)


if __name__ == "__main__":
    sys.exit(pytest.main([__file__]))
