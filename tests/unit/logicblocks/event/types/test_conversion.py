from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from types import NoneType
from typing import Any, Self

import pytest

from logicblocks.event.types import (
    JsonValue,
    JsonValueDeserialisable,
    JsonValueSerialisable,
    JsonValueType,
    deserialise_from_json_value,
    is_json_object,
    serialise_to_json_value,
)


@dataclass(frozen=True)
class Thing:
    value: int = 5


class TestSerialise:
    def test_serialises_none_to_none(self):
        assert serialise_to_json_value(None) is None

    def test_serialises_string_to_string(self):
        assert serialise_to_json_value("string") == "string"

    def test_serialises_int_to_int(self):
        assert serialise_to_json_value(1) == 1

    def test_serialises_float_to_float(self):
        assert serialise_to_json_value(1.0) == 1.0

    def test_serialises_true_to_true(self):
        assert serialise_to_json_value(True) is True

    def test_serialises_false_to_false(self):
        assert serialise_to_json_value(False) is False

    def test_raises_by_default_when_serialising_custom_class_instance(self):
        with pytest.raises(ValueError):
            serialise_to_json_value(Thing())

    def test_raises_by_default_when_serialising_datetime(self):
        with pytest.raises(ValueError):
            serialise_to_json_value(datetime.now())

    def test_raises_by_default_when_serialising_complex(self):
        with pytest.raises(ValueError):
            serialise_to_json_value(1 + 2j)

    @pytest.mark.parametrize(
        "values",
        [
            [None, None, None],
            ["a", "b", "c"],
            [1, 2, 3],
            [1.1, 2.1, 3.1],
            [True, False, True],
            [1 + 2j, 2 + 3j],
            [datetime.now(), datetime.now()],
            [Thing(), Thing()],
        ],
    )
    def test_raises_by_default_when_serialising_set_of_values(self, values):
        with pytest.raises(ValueError):
            serialise_to_json_value({*values})

    @pytest.mark.parametrize(
        "values",
        [
            [None, None, None],
            ["a", "b", "c"],
            [1, 2, 3],
            [1.1, 2.1, 3.1],
            [True, False, True],
        ],
    )
    def test_serialises_list_of_json_primitive_values_untouched(self, values):
        assert serialise_to_json_value([*values]) == [*values]

    @pytest.mark.parametrize(
        "values",
        [
            [1 + 2j, 2 + 3j, 3 + 4j],
            [datetime.now(), datetime.now()],
            [Thing(), Thing()],
        ],
    )
    def test_raises_by_default_when_serialising_list_of_non_json_primitive_values(
        self, values
    ):
        with pytest.raises(ValueError):
            serialise_to_json_value([*values])

    @pytest.mark.parametrize(
        "values",
        [
            [None, None, None],
            ["a", "b", "c"],
            [1, 2, 3],
            [1.1, 2.1, 3.1],
            [True, False, True],
        ],
    )
    def test_serialises_tuple_of_json_primitive_values_untouched(self, values):
        assert serialise_to_json_value(tuple(values)) == tuple(values)

    @pytest.mark.parametrize(
        "values",
        [
            [1 + 2j, 2 + 3j, 3 + 4j],
            [datetime.now(), datetime.now()],
            [Thing(), Thing()],
        ],
    )
    def test_raises_by_default_when_serialising_tuple_of_non_json_primitive_values(
        self, values
    ):
        with pytest.raises(ValueError):
            serialise_to_json_value(tuple(values))

    @pytest.mark.parametrize(
        "values",
        [
            [None, None, None],
            ["a", "b", "c"],
            [1, 2, 3],
            [1.1, 2.1, 3.1],
            [True, False, True],
        ],
    )
    def test_serialises_list_of_lists_of_json_primitive_values_untouched(
        self, values
    ):
        assert serialise_to_json_value([[*values], [*values]]) == [
            [*values],
            [*values],
        ]

    @pytest.mark.parametrize(
        "values",
        [
            [1 + 2j, 2 + 3j, 3 + 4j],
            [datetime.now(), datetime.now()],
            [Thing(), Thing()],
        ],
    )
    def test_raises_by_default_when_serialising_list_of_lists_of_non_json_primitive_values(
        self, values
    ):
        with pytest.raises(ValueError):
            serialise_to_json_value([[*values], [*values]])

    @pytest.mark.parametrize(
        "values",
        [
            [None, None, None],
            ["a", "b", "c"],
            [1, 2, 3],
            [1.1, 2.1, 3.1],
            [True, False, True],
        ],
    )
    def test_serialises_tuple_of_tuples_of_json_primitive_values_untouched(
        self, values
    ):
        assert serialise_to_json_value((tuple(values), tuple(values))) == (
            tuple(values),
            tuple(values),
        )

    @pytest.mark.parametrize(
        "values",
        [
            [1 + 2j, 2 + 3j, 3 + 4j],
            [datetime.now(), datetime.now()],
            [Thing(), Thing()],
        ],
    )
    def test_raises_by_default_when_serialising_tuple_of_tuples_of_non_json_primitive_values(
        self, values
    ):
        with pytest.raises(ValueError):
            serialise_to_json_value((tuple(values), tuple(values)))

    def test_serialises_deeply_nested_list_with_mixed_json_values_untouched(
        self,
    ):
        value = [
            {"nested1": [{"value": 1}, {"value": 2}]},
            {
                "nested2": {
                    "deeply_nested0": None,
                    "deeply_nested1": {"value": [1, 2, 3]},
                    "deeply_nested2": {"value": False},
                }
            },
            [1, "2", True],
        ]
        assert serialise_to_json_value(value) == value

    def test_raises_by_default_when_serialising_deeply_nested_list_with_mixed_non_json_values(
        self,
    ):
        with pytest.raises(ValueError):
            serialise_to_json_value(
                [
                    {"nested1": [Thing(), {"value": datetime.now()}]},
                    {
                        "nested2": {
                            "deeply_nested1": {"value": {1, 2, 3}},
                            "deeply_nested2": {"value": 1 + 1j},
                        }
                    },
                    [1, "2", Thing()],
                ]
            )

    @pytest.mark.parametrize(
        "values", [[None, None], ["a", "b"], [1, 2], [1.1, 2.1], [True, False]]
    )
    def test_serialises_dict_of_string_keys_and_json_primitive_values_untouched(
        self, values
    ):
        value = {"key1": values[0], "key2": values[1]}
        assert serialise_to_json_value(value) == value

    @pytest.mark.parametrize(
        "values",
        [
            [1 + 2j, 2 + 3j],
            [datetime.now(), datetime.now()],
            [Thing(), Thing()],
        ],
    )
    def test_raises_by_default_when_serialising_dict_of_string_keys_and_non_json_primitive_values(
        self, values
    ):
        with pytest.raises(ValueError):
            serialise_to_json_value({"key1": values[0], "key2": values[1]})

    @pytest.mark.parametrize(
        "keys", [[1, 2], [True, False], [Thing(), Thing()], [(1, 2), (3, 4)]]
    )
    def test_raises_by_default_when_serialising_dict_of_non_string_keys_and_json_primitive_values(
        self, keys
    ):
        with pytest.raises(ValueError):
            serialise_to_json_value({keys[0]: 1, keys[1]: 2})

    @pytest.mark.parametrize(
        "values", [[None, None], ["a", "b"], [1, 2], [1.1, 2.1], [True, False]]
    )
    def test_serialises_dict_of_dicts_of_string_keys_and_json_primitive_values_untouched(
        self, values
    ):
        value = {
            "key1": {
                "nested1": values[0],
                "nested2": values[1],
            },
            "key2": {
                "nested1": values[0],
                "nested2": values[1],
            },
        }
        assert serialise_to_json_value(value) == value

    @pytest.mark.parametrize(
        "values",
        [
            [1 + 2j, 2 + 3j],
            [datetime.now(), datetime.now()],
            [Thing(), Thing()],
        ],
    )
    def test_raises_by_default_when_serialising_dict_of_dicts_of_string_keys_and_non_json_primitive_values(
        self, values
    ):
        with pytest.raises(ValueError):
            serialise_to_json_value(
                {
                    "key1": {
                        "nested1": values[0],
                        "nested2": values[1],
                    },
                    "key2": {
                        "nested1": values[0],
                        "nested2": values[1],
                    },
                }
            )

    @pytest.mark.parametrize(
        "keys", [[1, 2], [True, False], [Thing(), Thing()], [(1, 2), (3, 4)]]
    )
    def test_raises_by_default_when_serialising_dict_of_dicts_of_non_string_keys_and_json_primitive_values(
        self, keys
    ):
        with pytest.raises(ValueError):
            serialise_to_json_value(
                {
                    "key1": {keys[0]: 1, keys[1]: 2},
                    "key2": {keys[0]: 1, keys[1]: 2},
                }
            )

    def test_serialises_deeply_nested_dict_with_mixed_json_values_untouched(
        self,
    ):
        value = {
            "key1": {
                "nested1": [{"value": 1}, {"value": 2}],
                "nested2": {
                    "deeply_nested0": None,
                    "deeply_nested1": {"value": [1, 2, 3]},
                    "deeply_nested2": {"value": False},
                },
            },
            "key2": [1, "2", True],
        }
        assert serialise_to_json_value(value) == value

    def test_raises_by_default_when_serialising_deeply_nested_dict_with_mixed_non_json_values(
        self,
    ):
        with pytest.raises(ValueError):
            serialise_to_json_value(
                {
                    "key1": {
                        "nested1": [Thing(), {"value": datetime.now()}],
                        "nested2": {
                            "deeply_nested1": {"value": {1, 2, 3}},
                            "deeply_nested2": {"value": 1 + 1j},
                        },
                    },
                    "key2": [1, "2", Thing()],
                }
            )

    def test_serialises_json_value_serialisable_using_serialise_method(self):
        class Serialisable(JsonValueSerialisable):
            def serialise(
                self, fallback: Callable[[object], JsonValue]
            ) -> JsonValue:
                return {"value": 10}

        instance = Serialisable()

        assert serialise_to_json_value(instance) == {"value": 10}

    def test_uses_provided_fallback_when_serialising_if_unable_to_serialise(
        self,
    ):
        def fallback(klass: type[Thing], value: object) -> Thing:
            if (
                not is_json_object(value)
                or "value" not in value
                or not isinstance(value["value"], int)
            ):
                raise ValueError(f"Cannot deserialise {value} as {klass}.")

            return Thing(value=value["value"])

        value = {"value": 10}

        assert deserialise_from_json_value(Thing, value, fallback) == Thing(
            value=10
        )


class TestDeserialise:
    def test_deserialises_none_to_json_value(self):
        assert deserialise_from_json_value(JsonValueType, None) is None

    def test_deserialises_none_to_none_type(self):
        assert deserialise_from_json_value(NoneType, None) is None

    def test_deserialises_string_to_json_value(self):
        assert deserialise_from_json_value(JsonValueType, "string") == "string"

    def test_deserialises_string_to_str_type(self):
        assert deserialise_from_json_value(str, "string") == "string"

    def test_deserialises_int_to_json_value(self):
        assert deserialise_from_json_value(JsonValueType, 10) == 10

    def test_deserialises_int_to_int_type(self):
        assert deserialise_from_json_value(int, 10) == 10

    def test_deserialises_float_to_json_value(self):
        assert deserialise_from_json_value(JsonValueType, 10.2) == 10.2

    def test_deserialises_float_to_float_type(self):
        assert deserialise_from_json_value(float, 10.2) == 10.2

    def test_deserialises_true_to_json_value(self):
        assert deserialise_from_json_value(JsonValueType, True) is True

    def test_deserialises_true_to_bool_type(self):
        assert deserialise_from_json_value(bool, True) is True

    def test_deserialises_false_to_json_value(self):
        assert deserialise_from_json_value(JsonValueType, False) is False

    def test_deserialises_false_to_bool_type(self):
        assert deserialise_from_json_value(bool, False) is False

    def test_raises_by_default_when_deserialising_custom_class_instance(self):
        with pytest.raises(ValueError):
            deserialise_from_json_value(Thing, {"a": 2})

    def test_raises_by_default_when_deserialising_datetime(self):
        with pytest.raises(ValueError):
            deserialise_from_json_value(JsonValueType, datetime.now())

    def test_raises_by_default_when_deserialising_complex(self):
        with pytest.raises(ValueError):
            deserialise_from_json_value(JsonValueType, 1 + 2j)

    @pytest.mark.parametrize(
        "values",
        [
            [None, None, None],
            ["a", "b", "c"],
            [1, 2, 3],
            [1.1, 2.1, 3.1],
            [True, False, True],
            [1 + 2j, 2 + 3j],
            [datetime.now(), datetime.now()],
            [Thing(), Thing()],
        ],
    )
    def test_raises_by_default_when_deserialising_set_of_values(self, values):
        with pytest.raises(ValueError):
            deserialise_from_json_value(Sequence[JsonValue], {*values})

    @pytest.mark.parametrize(
        "values,type",
        [
            [values, type]
            for values in [
                [None, None, None],
                ["a", "b", "c"],
                [1, 2, 3],
                [1.1, 2.1, 3.1],
                [True, False, True],
            ]
            for type in [
                JsonValueType,
                Sequence,
                Sequence[JsonValue],
                Sequence[Any],
            ]
        ]
        + [
            [[None, None, None], Sequence[NoneType]],
            [["a", "b", "c"], Sequence[str]],
            [[1, 2, 3], Sequence[int]],
            [[1.1, 2.1, 3.1], Sequence[float]],
            [[True, False, True], Sequence[bool]],
        ],
    )
    def test_deserialises_list_of_json_primitive_values(self, values, type):
        assert deserialise_from_json_value(type, [*values]) == [*values]

    @pytest.mark.parametrize(
        "values,type",
        [
            [values, type]
            for values in [
                [1 + 2j, 2 + 3j, 3 + 4j],
                [datetime.now(), datetime.now()],
                [Thing(), Thing()],
            ]
            for type in [
                JsonValueType,
                Sequence,
                Sequence[JsonValue],
                Sequence[Any],
            ]
        ]
        + [
            [[1 + 2j, 2 + 3j, 3 + 4j], Sequence[complex]],
            [[datetime.now(), datetime.now()], Sequence[datetime]],
            [[Thing(), Thing()], Sequence[Thing]],
        ],
    )
    def test_raises_by_default_when_deserialising_list_of_non_json_primitive_values(
        self, values, type
    ):
        with pytest.raises(ValueError):
            deserialise_from_json_value(type, [*values])

    def test_raises_by_default_when_deserialising_list_of_one_json_primitive_to_sequence_of_other_json_primitive(
        self,
    ):
        with pytest.raises(ValueError):
            deserialise_from_json_value(Sequence[str], [1, 2, 3])

    @pytest.mark.parametrize(
        "values,type",
        [
            [values, type]
            for values in [
                [None, None, None],
                ["a", "b", "c"],
                [1, 2, 3],
                [1.1, 2.1, 3.1],
                [True, False, True],
            ]
            for type in [
                JsonValueType,
                Sequence,
                Sequence[JsonValue],
                Sequence[Any],
            ]
        ],
    )
    def test_deserialises_tuple_of_json_primitive_values(self, values, type):
        assert deserialise_from_json_value(type, tuple(values)) == tuple(
            values
        )

    @pytest.mark.parametrize(
        "values,type",
        [
            [values, type]
            for values in [
                [1 + 2j, 2 + 3j, 3 + 4j],
                [datetime.now(), datetime.now()],
                [Thing(), Thing()],
            ]
            for type in [
                JsonValueType,
                Sequence,
                Sequence[JsonValue],
                Sequence[Any],
            ]
        ]
        + [
            [[1 + 2j, 2 + 3j, 3 + 4j], Sequence[complex]],
            [[datetime.now(), datetime.now()], Sequence[datetime]],
            [[Thing(), Thing()], Sequence[Thing]],
        ],
    )
    def test_raises_by_default_when_deserialising_tuple_of_non_json_primitive_values(
        self, values, type
    ):
        with pytest.raises(ValueError):
            deserialise_from_json_value(type, tuple(values))

    def test_raises_by_default_when_deserialising_tuple_of_one_json_primitive_to_sequence_of_other_json_primitive(
        self,
    ):
        with pytest.raises(ValueError):
            deserialise_from_json_value(Sequence[str], (1, 2, 3))

    @pytest.mark.parametrize(
        "values,type",
        [
            [values, type]
            for values in [
                [None, None, None],
                ["a", "b", "c"],
                [1, 2, 3],
                [1.1, 2.1, 3.1],
                [True, False, True],
            ]
            for type in [
                JsonValueType,
                Sequence[Sequence],
                Sequence[Sequence[JsonValue]],
                Sequence[Sequence[Any]],
            ]
        ]
        + [
            [[None, None, None], Sequence[Sequence[NoneType]]],
            [["a", "b", "c"], Sequence[Sequence[str]]],
            [[1, 2, 3], Sequence[Sequence[int]]],
            [[1.1, 2.1, 3.1], Sequence[Sequence[float]]],
            [[True, False, True], Sequence[Sequence[bool]]],
        ],
    )
    def test_deserialises_list_of_lists_of_json_primitive_values(
        self, values, type
    ):
        assert deserialise_from_json_value(type, [[*values], [*values]]) == [
            [*values],
            [*values],
        ]

    @pytest.mark.parametrize(
        "values,type",
        [
            [values, type]
            for values in [
                [1 + 2j, 2 + 3j, 3 + 4j],
                [datetime.now(), datetime.now()],
                [Thing(), Thing()],
            ]
            for type in [
                JsonValueType,
                Sequence[Sequence],
                Sequence[Sequence[JsonValue]],
                Sequence[Sequence[Any]],
            ]
        ]
        + [
            [[1 + 2j, 2 + 3j, 3 + 4j], Sequence[Sequence[complex]]],
            [[datetime.now(), datetime.now()], Sequence[Sequence[datetime]]],
            [[Thing(), Thing()], Sequence[Sequence[Thing]]],
        ],
    )
    def test_raises_by_default_when_deserialising_list_of_lists_of_non_json_primitive_values(
        self, values, type
    ):
        with pytest.raises(ValueError):
            deserialise_from_json_value(type, [[*values], [*values]])

    def test_raises_by_default_when_deserialising_list_of_lists_of_one_json_primitive_to_sequence_of_sequence_of_other_json_primitive(
        self,
    ):
        with pytest.raises(ValueError):
            deserialise_from_json_value(
                Sequence[Sequence[Sequence[str]]], [[1, 2, 3], [1, 2, 3]]
            )

    @pytest.mark.parametrize(
        "values,type",
        [
            [values, type]
            for values in [
                [None, None, None],
                ["a", "b", "c"],
                [1, 2, 3],
                [1.1, 2.1, 3.1],
                [True, False, True],
            ]
            for type in [
                JsonValueType,
                Sequence[Sequence],
                Sequence[Sequence[JsonValue]],
                Sequence[Sequence[Any]],
            ]
        ]
        + [
            [[None, None, None], Sequence[Sequence[NoneType]]],
            [["a", "b", "c"], Sequence[Sequence[str]]],
            [[1, 2, 3], Sequence[Sequence[int]]],
            [[1.1, 2.1, 3.1], Sequence[Sequence[float]]],
            [[True, False, True], Sequence[Sequence[bool]]],
        ],
    )
    def test_deserialises_tuple_of_tuples_of_json_primitive_values(
        self, values, type
    ):
        assert deserialise_from_json_value(
            type, (tuple(values), tuple(values))
        ) == (
            tuple(values),
            tuple(values),
        )

    @pytest.mark.parametrize(
        "values,type",
        [
            [values, type]
            for values in [
                [1 + 2j, 2 + 3j, 3 + 4j],
                [datetime.now(), datetime.now()],
                [Thing(), Thing()],
            ]
            for type in [
                JsonValueType,
                Sequence[Sequence],
                Sequence[Sequence[JsonValue]],
                Sequence[Sequence[Any]],
            ]
        ]
        + [
            [[1 + 2j, 2 + 3j, 3 + 4j], Sequence[Sequence[complex]]],
            [[datetime.now(), datetime.now()], Sequence[Sequence[datetime]]],
            [[Thing(), Thing()], Sequence[Sequence[Thing]]],
        ],
    )
    def test_raises_by_default_when_deserialising_tuple_of_tuples_of_non_json_primitive_values(
        self, values, type
    ):
        with pytest.raises(ValueError):
            deserialise_from_json_value(type, (tuple(values), tuple(values)))

    def test_deserialises_deeply_nested_list_with_mixed_json_values(
        self,
    ):
        value = [
            {"nested1": [{"value": 1}, {"value": 2}]},
            {
                "nested2": {
                    "deeply_nested0": None,
                    "deeply_nested1": {"value": [1, 2, 3]},
                    "deeply_nested2": {"value": False},
                }
            },
            [1, "2", True],
        ]
        assert deserialise_from_json_value(JsonValueType, value) == value

    def test_raises_by_default_when_deserialising_deeply_nested_list_with_mixed_non_json_values(
        self,
    ):
        with pytest.raises(ValueError):
            deserialise_from_json_value(
                JsonValueType,
                [
                    {"nested1": [Thing(), {"value": datetime.now()}]},
                    {
                        "nested2": {
                            "deeply_nested1": {"value": {1, 2, 3}},
                            "deeply_nested2": {"value": 1 + 1j},
                        }
                    },
                    [1, "2", Thing()],
                ],
            )

    @pytest.mark.parametrize(
        "values,type",
        [
            [values, type]
            for values in [
                [None, None],
                ["a", "b"],
                [1, 2],
                [1.1, 2.1],
                [True, False],
            ]
            for type in [
                JsonValueType,
                Mapping,
                Mapping[str, JsonValue],
                Mapping[str, Any],
            ]
        ]
        + [
            [[None, None], Mapping[str, NoneType]],
            [["a", "b"], Mapping[str, str]],
            [[1, 2], Mapping[str, int]],
            [[1.1, 2.1], Mapping[str, float]],
            [[True, False], Mapping[str, bool]],
        ],
    )
    def test_deserialises_dict_of_string_keys_and_json_primitive_values(
        self, values, type
    ):
        value = {"key1": values[0], "key2": values[1]}
        assert deserialise_from_json_value(type, value) == value

    @pytest.mark.parametrize(
        "values,type",
        [
            [values, type]
            for values in [
                [1 + 2j, 2 + 3j],
                [datetime.now(), datetime.now()],
                [Thing(), Thing()],
            ]
            for type in [
                JsonValueType,
                Mapping,
                Mapping[str, JsonValue],
                Mapping[str, Any],
            ]
        ]
        + [
            [[1 + 2j, 2 + 3j], Mapping[str, complex]],
            [[datetime.now(), datetime.now()], Mapping[str, datetime]],
            [[Thing(), Thing()], Mapping[str, Thing]],
        ],
    )
    def test_raises_by_default_when_deserialising_dict_of_string_keys_and_non_json_primitive_values(
        self, values, type
    ):
        with pytest.raises(ValueError):
            deserialise_from_json_value(
                type, {"key1": values[0], "key2": values[1]}
            )

    @pytest.mark.parametrize(
        "keys,type",
        [
            [keys, type]
            for keys in [
                [1, 2],
                [True, False],
                [Thing(), Thing()],
                [(1, 2), (3, 4)],
            ]
            for type in [
                JsonValueType,
                Mapping,
                Mapping[str, JsonValue],
                Mapping[str, Any],
            ]
        ],
    )
    def test_raises_by_default_when_deserialising_dict_of_non_string_keys_and_json_primitive_values(
        self, keys, type
    ):
        with pytest.raises(ValueError):
            deserialise_from_json_value(type, {keys[0]: 1, keys[1]: 2})

    @pytest.mark.parametrize(
        "values,type",
        [
            [values, type]
            for values in [
                [None, None],
                ["a", "b"],
                [1, 2],
                [1.1, 2.1],
                [True, False],
            ]
            for type in [
                JsonValueType,
                Mapping,
                Mapping[str, Mapping],
                Mapping[str, Mapping[str, JsonValue]],
                Mapping[str, Mapping[str, Any]],
            ]
        ]
        + [
            [[None, None], Mapping[str, Mapping[str, NoneType]]],
            [["a", "b"], Mapping[str, Mapping[str, str]]],
            [[1, 2], Mapping[str, Mapping[str, int]]],
            [[1.1, 2.1], Mapping[str, Mapping[str, float]]],
            [[True, False], Mapping[str, Mapping[str, bool]]],
        ],
    )
    def test_deserialises_dict_of_dicts_of_string_keys_and_json_primitive_values(
        self, values, type
    ):
        value = {
            "key1": {
                "nested1": values[0],
                "nested2": values[1],
            },
            "key2": {
                "nested1": values[0],
                "nested2": values[1],
            },
        }
        assert deserialise_from_json_value(type, value) == value

    @pytest.mark.parametrize(
        "values,type",
        [
            [values, type]
            for values in [
                [1 + 2j, 2 + 3j],
                [datetime.now(), datetime.now()],
                [Thing(), Thing()],
            ]
            for type in [
                JsonValueType,
                Mapping,
                Mapping[str, Mapping],
                Mapping[str, Mapping[str, JsonValue]],
                Mapping[str, Mapping[str, Any]],
            ]
        ]
        + [
            [[1 + 2j, 2 + 3j], Mapping[str, Mapping[str, complex]]],
            [
                [datetime.now(), datetime.now()],
                Mapping[str, Mapping[str, datetime]],
            ],
            [[Thing(), Thing()], Mapping[str, Mapping[str, Thing]]],
        ],
    )
    def test_raises_by_default_when_deserialising_dict_of_dicts_of_string_keys_and_non_json_primitive_values(
        self, values, type
    ):
        with pytest.raises(ValueError):
            deserialise_from_json_value(
                type,
                {
                    "key1": {
                        "nested1": values[0],
                        "nested2": values[1],
                    },
                    "key2": {
                        "nested1": values[0],
                        "nested2": values[1],
                    },
                },
            )

    @pytest.mark.parametrize(
        "keys,type",
        [
            [keys, type]
            for keys in [
                [1, 2],
                [True, False],
                [Thing(), Thing()],
                [(1, 2), (3, 4)],
            ]
            for type in [
                JsonValueType,
                Mapping,
                Mapping[str, JsonValue],
                Mapping[str, Any],
            ]
        ],
    )
    def test_raises_by_default_when_deserialising_dict_of_dicts_of_non_string_keys_and_json_primitive_values(
        self, keys, type
    ):
        with pytest.raises(ValueError):
            deserialise_from_json_value(
                type,
                {
                    "key1": {keys[0]: 1, keys[1]: 2},
                    "key2": {keys[0]: 1, keys[1]: 2},
                },
            )

    def test_deserialises_deeply_nested_dict_with_mixed_json_values_untouched(
        self,
    ):
        value = {
            "key1": {
                "nested1": [{"value": 1}, {"value": 2}],
                "nested2": {
                    "deeply_nested0": None,
                    "deeply_nested1": {"value": [1, 2, 3]},
                    "deeply_nested2": {"value": False},
                },
            },
            "key2": [1, "2", True],
        }
        assert deserialise_from_json_value(JsonValueType, value) == value

    def test_raises_by_default_when_deserialising_deeply_nested_dict_with_mixed_non_json_values(
        self,
    ):
        with pytest.raises(ValueError):
            deserialise_from_json_value(
                JsonValueType,
                {
                    "key1": {
                        "nested1": [Thing(), {"value": datetime.now()}],
                        "nested2": {
                            "deeply_nested1": {"value": {1, 2, 3}},
                            "deeply_nested2": {"value": 1 + 1j},
                        },
                    },
                    "key2": [1, "2", Thing()],
                },
            )

    def test_deserialises_json_value_deserialisable_using_deserialise_method(
        self,
    ):
        @dataclass
        class Deserialisable(JsonValueDeserialisable):
            value: int

            @classmethod
            def deserialise(
                cls,
                value: JsonValue,
                fallback: Callable[[Any, JsonValue], Any],
            ) -> Self:
                if (
                    not is_json_object(value)
                    or "value" not in value
                    or not isinstance(value["value"], int)
                ):
                    raise ValueError

                return cls(value=value["value"])

        value = {"value": 10}

        assert deserialise_from_json_value(
            Deserialisable, value
        ) == Deserialisable(value=10)

    def test_uses_provided_fallback_when_deserialising_if_unable_to_deserialise(
        self,
    ):
        def fallback(klass: type[Thing], value: object) -> Thing:
            if (
                not is_json_object(value)
                or "value" not in value
                or not isinstance(value["value"], int)
            ):
                raise ValueError(f"Cannot deserialise {value} as {klass}.")

            return Thing(value=value["value"])

        value = {"value": 10}

        assert deserialise_from_json_value(Thing, value, fallback) == Thing(
            value=10
        )
