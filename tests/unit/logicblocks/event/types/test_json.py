from datetime import datetime

import pytest

from logicblocks.event.types import (
    is_json_array,
    is_json_object,
    is_json_primitive,
    is_json_value,
)


class Thing:
    value: int = 5


class TestIsJsonObject:
    def test_returns_false_for_none(self):
        assert is_json_object(None) is False

    def test_returns_false_for_string(self):
        assert is_json_object("string") is False

    def test_returns_false_for_int(self):
        assert is_json_object(1) is False

    def test_returns_false_for_float(self):
        assert is_json_object(1.0) is False

    def test_returns_false_for_true(self):
        assert is_json_object(True) is False

    def test_returns_false_for_false(self):
        assert is_json_object(False) is False

    def test_returns_false_for_custom_class_instance(self):
        assert is_json_object(Thing()) is False

    def test_returns_false_for_datetime(self):
        assert is_json_object(datetime.now()) is False

    def test_returns_false_for_complex(self):
        assert is_json_object(1 + 2j) is False

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
    def test_returns_false_for_set_of_values(self, values):
        assert is_json_object({*values}) is False

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
    def test_returns_false_for_list_of_json_primitive_values(self, values):
        assert is_json_object([*values]) is False

    @pytest.mark.parametrize(
        "values",
        [
            [1 + 2j, 2 + 3j, 3 + 4j],
            [datetime.now(), datetime.now()],
            [Thing(), Thing()],
        ],
    )
    def test_returns_false_for_list_of_non_json_primitive_values(self, values):
        assert is_json_object([*values]) is False

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
    def test_returns_false_for_tuple_of_json_primitive_values(self, values):
        assert is_json_object(tuple(values)) is False

    @pytest.mark.parametrize(
        "values",
        [
            [1 + 2j, 2 + 3j, 3 + 4j],
            [datetime.now(), datetime.now()],
            [Thing(), Thing()],
        ],
    )
    def test_returns_false_for_tuple_of_non_json_primitive_values(
        self, values
    ):
        assert is_json_object(tuple(values)) is False

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
    def test_returns_false_for_list_of_lists_of_json_primitive_values(
        self, values
    ):
        assert is_json_object([[*values], [*values]]) is False

    @pytest.mark.parametrize(
        "values",
        [
            [1 + 2j, 2 + 3j, 3 + 4j],
            [datetime.now(), datetime.now()],
            [Thing(), Thing()],
        ],
    )
    def test_returns_false_for_list_of_lists_of_non_json_primitive_values(
        self, values
    ):
        assert is_json_object([[*values], [*values]]) is False

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
    def test_returns_false_for_tuple_of_tuples_of_json_primitive_values(
        self, values
    ):
        assert is_json_object((tuple(values), tuple(values))) is False

    @pytest.mark.parametrize(
        "values",
        [
            [1 + 2j, 2 + 3j, 3 + 4j],
            [datetime.now(), datetime.now()],
            [Thing(), Thing()],
        ],
    )
    def test_returns_false_for_tuple_of_tuples_of_non_json_primitive_values(
        self, values
    ):
        assert is_json_object((tuple(values), tuple(values))) is False

    def test_returns_true_for_deeply_nested_list_with_mixed_json_values(self):
        assert (
            is_json_object(
                [
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
            )
            is False
        )

    def test_returns_false_for_deeply_nested_list_with_mixed_non_json_values(
        self,
    ):
        assert (
            is_json_object(
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
            is False
        )

    @pytest.mark.parametrize(
        "values", [[None, None], ["a", "b"], [1, 2], [1.1, 2.1], [True, False]]
    )
    def test_returns_true_for_dict_of_string_keys_and_json_primitive_values(
        self, values
    ):
        assert is_json_object({"key1": values[0], "key2": values[1]}) is True

    @pytest.mark.parametrize(
        "values",
        [
            [1 + 2j, 2 + 3j],
            [datetime.now(), datetime.now()],
            [Thing(), Thing()],
        ],
    )
    def test_returns_false_for_dict_of_string_keys_and_non_json_primitive_values(
        self, values
    ):
        assert is_json_object({"key1": values[0], "key2": values[1]}) is False

    @pytest.mark.parametrize(
        "keys", [[1, 2], [True, False], [Thing(), Thing()], [(1, 2), (3, 4)]]
    )
    def test_returns_false_for_dict_of_non_string_keys_and_json_primitive_values(
        self, keys
    ):
        assert is_json_object({keys[0]: 1, keys[1]: 2}) is False

    @pytest.mark.parametrize(
        "values", [[None, None], ["a", "b"], [1, 2], [1.1, 2.1], [True, False]]
    )
    def test_returns_true_for_dict_of_dicts_of_string_keys_and_json_primitive_values(
        self, values
    ):
        assert (
            is_json_object(
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
            is True
        )

    @pytest.mark.parametrize(
        "values",
        [
            [1 + 2j, 2 + 3j],
            [datetime.now(), datetime.now()],
            [Thing(), Thing()],
        ],
    )
    def test_returns_false_for_dict_of_dicts_of_string_keys_and_non_json_primitive_values(
        self, values
    ):
        assert (
            is_json_object(
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
            is False
        )

    @pytest.mark.parametrize(
        "keys", [[1, 2], [True, False], [Thing(), Thing()], [(1, 2), (3, 4)]]
    )
    def test_returns_false_for_dict_of_dicts_of_non_string_keys_and_json_primitive_values(
        self, keys
    ):
        assert (
            is_json_object(
                {
                    "key1": {keys[0]: 1, keys[1]: 2},
                    "key2": {keys[0]: 1, keys[1]: 2},
                }
            )
            is False
        )

    def test_returns_true_for_deeply_nested_dict_with_mixed_json_values(self):
        assert (
            is_json_object(
                {
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
            )
            is True
        )

    def test_returns_false_for_deeply_nested_dict_with_mixed_non_json_values(
        self,
    ):
        assert (
            is_json_object(
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
            is False
        )


class TestIsJsonArray:
    def test_returns_false_for_none(self):
        assert is_json_array(None) is False

    def test_returns_false_for_string(self):
        assert is_json_array("string") is False

    def test_returns_false_for_int(self):
        assert is_json_array(1) is False

    def test_returns_false_for_float(self):
        assert is_json_array(1.0) is False

    def test_returns_false_for_true(self):
        assert is_json_array(True) is False

    def test_returns_false_for_false(self):
        assert is_json_array(False) is False

    def test_returns_false_for_custom_class_instance(self):
        assert is_json_array(Thing()) is False

    def test_returns_false_for_datetime(self):
        assert is_json_array(datetime.now()) is False

    def test_returns_false_for_complex(self):
        assert is_json_array(1 + 2j) is False

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
    def test_returns_false_for_set_of_values(self, values):
        assert is_json_array({*values}) is False

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
    def test_returns_true_for_list_of_json_primitive_values(self, values):
        assert is_json_array([*values]) is True

    @pytest.mark.parametrize(
        "values",
        [
            [1 + 2j, 2 + 3j, 3 + 4j],
            [datetime.now(), datetime.now()],
            [Thing(), Thing()],
        ],
    )
    def test_returns_false_for_list_of_non_json_primitive_values(self, values):
        assert is_json_array([*values]) is False

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
    def test_returns_true_for_tuple_of_json_primitive_values(self, values):
        assert is_json_array(tuple(values)) is True

    @pytest.mark.parametrize(
        "values",
        [
            [1 + 2j, 2 + 3j, 3 + 4j],
            [datetime.now(), datetime.now()],
            [Thing(), Thing()],
        ],
    )
    def test_returns_false_for_tuple_of_non_json_primitive_values(
        self, values
    ):
        assert is_json_array(tuple(values)) is False

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
    def test_returns_true_for_list_of_lists_of_json_primitive_values(
        self, values
    ):
        assert is_json_array([[*values], [*values]]) is True

    @pytest.mark.parametrize(
        "values",
        [
            [1 + 2j, 2 + 3j, 3 + 4j],
            [datetime.now(), datetime.now()],
            [Thing(), Thing()],
        ],
    )
    def test_returns_false_for_list_of_lists_of_non_json_primitive_values(
        self, values
    ):
        assert is_json_array([[*values], [*values]]) is False

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
    def test_returns_true_for_tuple_of_tuples_of_json_primitive_values(
        self, values
    ):
        assert is_json_array((tuple(values), tuple(values))) is True

    @pytest.mark.parametrize(
        "values",
        [
            [1 + 2j, 2 + 3j, 3 + 4j],
            [datetime.now(), datetime.now()],
            [Thing(), Thing()],
        ],
    )
    def test_returns_false_for_tuple_of_tuples_of_non_json_primitive_values(
        self, values
    ):
        assert is_json_array((tuple(values), tuple(values))) is False

    def test_returns_true_for_deeply_nested_list_with_mixed_json_values(self):
        assert (
            is_json_array(
                [
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
            )
            is True
        )

    def test_returns_false_for_deeply_nested_list_with_mixed_non_json_values(
        self,
    ):
        assert (
            is_json_array(
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
            is False
        )

    @pytest.mark.parametrize(
        "values", [[None, None], ["a", "b"], [1, 2], [1.1, 2.1], [True, False]]
    )
    def test_returns_false_for_dict_of_string_keys_and_json_primitive_values(
        self, values
    ):
        assert is_json_array({"key1": values[0], "key2": values[1]}) is False

    @pytest.mark.parametrize(
        "values",
        [
            [1 + 2j, 2 + 3j],
            [datetime.now(), datetime.now()],
            [Thing(), Thing()],
        ],
    )
    def test_returns_false_for_dict_of_string_keys_and_non_json_primitive_values(
        self, values
    ):
        assert is_json_array({"key1": values[0], "key2": values[1]}) is False

    @pytest.mark.parametrize(
        "keys", [[1, 2], [True, False], [Thing(), Thing()], [(1, 2), (3, 4)]]
    )
    def test_returns_false_for_dict_of_non_string_keys_and_json_primitive_values(
        self, keys
    ):
        assert is_json_array({keys[0]: 1, keys[1]: 2}) is False

    @pytest.mark.parametrize(
        "values", [[None, None], ["a", "b"], [1, 2], [1.1, 2.1], [True, False]]
    )
    def test_returns_false_for_dict_of_dicts_of_string_keys_and_json_primitive_values(
        self, values
    ):
        assert (
            is_json_array(
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
            is False
        )

    @pytest.mark.parametrize(
        "values",
        [
            [1 + 2j, 2 + 3j],
            [datetime.now(), datetime.now()],
            [Thing(), Thing()],
        ],
    )
    def test_returns_false_for_dict_of_dicts_of_string_keys_and_non_json_primitive_values(
        self, values
    ):
        assert (
            is_json_array(
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
            is False
        )

    @pytest.mark.parametrize(
        "keys", [[1, 2], [True, False], [Thing(), Thing()], [(1, 2), (3, 4)]]
    )
    def test_returns_false_for_dict_of_dicts_of_non_string_keys_and_json_primitive_values(
        self, keys
    ):
        assert (
            is_json_array(
                {
                    "key1": {keys[0]: 1, keys[1]: 2},
                    "key2": {keys[0]: 1, keys[1]: 2},
                }
            )
            is False
        )

    def test_returns_false_for_deeply_nested_dict_with_mixed_json_values(self):
        assert (
            is_json_array(
                {
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
            )
            is False
        )

    def test_returns_false_for_deeply_nested_dict_with_mixed_non_json_values(
        self,
    ):
        assert (
            is_json_array(
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
            is False
        )


class TestIsJsonPrimitive:
    def test_returns_true_for_none(self):
        assert is_json_primitive(None) is True

    def test_returns_true_for_string(self):
        assert is_json_primitive("string") is True

    def test_returns_true_for_int(self):
        assert is_json_primitive(1) is True

    def test_returns_true_for_float(self):
        assert is_json_primitive(1.0) is True

    def test_returns_true_for_true(self):
        assert is_json_primitive(True) is True

    def test_returns_true_for_false(self):
        assert is_json_primitive(False) is True

    def test_returns_false_for_custom_class_instance(self):
        assert is_json_primitive(Thing()) is False

    def test_returns_false_for_datetime(self):
        assert is_json_primitive(datetime.now()) is False

    def test_returns_false_for_complex(self):
        assert is_json_primitive(1 + 2j) is False

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
    def test_returns_false_for_set_of_values(self, values):
        assert is_json_primitive({*values}) is False

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
    def test_returns_false_for_list_of_json_primitive_values(self, values):
        assert is_json_primitive([*values]) is False

    @pytest.mark.parametrize(
        "values",
        [
            [1 + 2j, 2 + 3j, 3 + 4j],
            [datetime.now(), datetime.now()],
            [Thing(), Thing()],
        ],
    )
    def test_returns_false_for_list_of_non_json_primitive_values(self, values):
        assert is_json_primitive([*values]) is False

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
    def test_returns_false_for_tuple_of_json_primitive_values(self, values):
        assert is_json_primitive(tuple(values)) is False

    @pytest.mark.parametrize(
        "values",
        [
            [1 + 2j, 2 + 3j, 3 + 4j],
            [datetime.now(), datetime.now()],
            [Thing(), Thing()],
        ],
    )
    def test_returns_false_for_tuple_of_non_json_primitive_values(
        self, values
    ):
        assert is_json_primitive(tuple(values)) is False

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
    def test_returns_false_for_list_of_lists_of_json_primitive_values(
        self, values
    ):
        assert is_json_primitive([[*values], [*values]]) is False

    @pytest.mark.parametrize(
        "values",
        [
            [1 + 2j, 2 + 3j, 3 + 4j],
            [datetime.now(), datetime.now()],
            [Thing(), Thing()],
        ],
    )
    def test_returns_false_for_list_of_lists_of_non_json_primitive_values(
        self, values
    ):
        assert is_json_primitive([[*values], [*values]]) is False

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
    def test_returns_false_for_tuple_of_tuples_of_json_primitive_values(
        self, values
    ):
        assert is_json_primitive((tuple(values), tuple(values))) is False

    @pytest.mark.parametrize(
        "values",
        [
            [1 + 2j, 2 + 3j, 3 + 4j],
            [datetime.now(), datetime.now()],
            [Thing(), Thing()],
        ],
    )
    def test_returns_false_for_tuple_of_tuples_of_non_json_primitive_values(
        self, values
    ):
        assert is_json_primitive((tuple(values), tuple(values))) is False

    def test_returns_false_for_deeply_nested_list_with_mixed_json_values(self):
        assert (
            is_json_primitive(
                [
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
            )
            is False
        )

    def test_returns_false_for_deeply_nested_list_with_mixed_non_json_values(
        self,
    ):
        assert (
            is_json_primitive(
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
            is False
        )

    @pytest.mark.parametrize(
        "values", [[None, None], ["a", "b"], [1, 2], [1.1, 2.1], [True, False]]
    )
    def test_returns_false_for_dict_of_string_keys_and_json_primitive_values(
        self, values
    ):
        assert (
            is_json_primitive({"key1": values[0], "key2": values[1]}) is False
        )

    @pytest.mark.parametrize(
        "values",
        [
            [1 + 2j, 2 + 3j],
            [datetime.now(), datetime.now()],
            [Thing(), Thing()],
        ],
    )
    def test_returns_false_for_dict_of_string_keys_and_non_json_primitive_values(
        self, values
    ):
        assert (
            is_json_primitive({"key1": values[0], "key2": values[1]}) is False
        )

    @pytest.mark.parametrize(
        "keys", [[1, 2], [True, False], [Thing(), Thing()], [(1, 2), (3, 4)]]
    )
    def test_returns_false_for_dict_of_non_string_keys_and_json_primitive_values(
        self, keys
    ):
        assert is_json_primitive({keys[0]: 1, keys[1]: 2}) is False

    @pytest.mark.parametrize(
        "values", [[None, None], ["a", "b"], [1, 2], [1.1, 2.1], [True, False]]
    )
    def test_returns_false_for_dict_of_dicts_of_string_keys_and_json_primitive_values(
        self, values
    ):
        assert (
            is_json_primitive(
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
            is False
        )

    @pytest.mark.parametrize(
        "values",
        [
            [1 + 2j, 2 + 3j],
            [datetime.now(), datetime.now()],
            [Thing(), Thing()],
        ],
    )
    def test_returns_false_for_dict_of_dicts_of_string_keys_and_non_json_primitive_values(
        self, values
    ):
        assert (
            is_json_primitive(
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
            is False
        )

    @pytest.mark.parametrize(
        "keys", [[1, 2], [True, False], [Thing(), Thing()], [(1, 2), (3, 4)]]
    )
    def test_returns_false_for_dict_of_dicts_of_non_string_keys_and_json_primitive_values(
        self, keys
    ):
        assert (
            is_json_primitive(
                {
                    "key1": {keys[0]: 1, keys[1]: 2},
                    "key2": {keys[0]: 1, keys[1]: 2},
                }
            )
            is False
        )

    def test_returns_false_for_deeply_nested_dict_with_mixed_json_values(self):
        assert (
            is_json_primitive(
                {
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
            )
            is False
        )

    def test_returns_false_for_deeply_nested_dict_with_mixed_non_json_values(
        self,
    ):
        assert (
            is_json_primitive(
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
            is False
        )


class TestIsJsonValue:
    def test_returns_true_for_none(self):
        assert is_json_value(None) is True

    def test_returns_true_for_string(self):
        assert is_json_value("string") is True

    def test_returns_true_for_int(self):
        assert is_json_value(1) is True

    def test_returns_true_for_float(self):
        assert is_json_value(1.0) is True

    def test_returns_true_for_true(self):
        assert is_json_value(True) is True

    def test_returns_true_for_false(self):
        assert is_json_value(False) is True

    def test_returns_false_for_custom_class_instance(self):
        assert is_json_value(Thing()) is False

    def test_returns_false_for_datetime(self):
        assert is_json_value(datetime.now()) is False

    def test_returns_false_for_complex(self):
        assert is_json_value(1 + 2j) is False

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
    def test_returns_false_for_set_of_values(self, values):
        assert is_json_value({*values}) is False

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
    def test_returns_true_for_list_of_json_primitive_values(self, values):
        assert is_json_value([*values]) is True

    @pytest.mark.parametrize(
        "values",
        [
            [1 + 2j, 2 + 3j, 3 + 4j],
            [datetime.now(), datetime.now()],
            [Thing(), Thing()],
        ],
    )
    def test_returns_false_for_list_of_non_json_primitive_values(self, values):
        assert is_json_value([*values]) is False

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
    def test_returns_true_for_tuple_of_json_primitive_values(self, values):
        assert is_json_value(tuple(values)) is True

    @pytest.mark.parametrize(
        "values",
        [
            [1 + 2j, 2 + 3j, 3 + 4j],
            [datetime.now(), datetime.now()],
            [Thing(), Thing()],
        ],
    )
    def test_returns_false_for_tuple_of_non_json_primitive_values(
        self, values
    ):
        assert is_json_value(tuple(values)) is False

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
    def test_returns_true_for_list_of_lists_of_json_primitive_values(
        self, values
    ):
        assert is_json_value([[*values], [*values]]) is True

    @pytest.mark.parametrize(
        "values",
        [
            [1 + 2j, 2 + 3j, 3 + 4j],
            [datetime.now(), datetime.now()],
            [Thing(), Thing()],
        ],
    )
    def test_returns_false_for_list_of_lists_of_non_json_primitive_values(
        self, values
    ):
        assert is_json_value([[*values], [*values]]) is False

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
    def test_returns_true_for_tuple_of_tuples_of_json_primitive_values(
        self, values
    ):
        assert is_json_value((tuple(values), tuple(values))) is True

    @pytest.mark.parametrize(
        "values",
        [
            [1 + 2j, 2 + 3j, 3 + 4j],
            [datetime.now(), datetime.now()],
            [Thing(), Thing()],
        ],
    )
    def test_returns_false_for_tuple_of_tuples_of_non_json_primitive_values(
        self, values
    ):
        assert is_json_value((tuple(values), tuple(values))) is False

    def test_returns_true_for_deeply_nested_list_with_mixed_json_values(self):
        assert (
            is_json_value(
                [
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
            )
            is True
        )

    def test_returns_false_for_deeply_nested_list_with_mixed_non_json_values(
        self,
    ):
        assert (
            is_json_value(
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
            is False
        )

    @pytest.mark.parametrize(
        "values", [[None, None], ["a", "b"], [1, 2], [1.1, 2.1], [True, False]]
    )
    def test_returns_true_for_dict_of_string_keys_and_json_primitive_values(
        self, values
    ):
        assert is_json_value({"key1": values[0], "key2": values[1]}) is True

    @pytest.mark.parametrize(
        "values",
        [
            [1 + 2j, 2 + 3j],
            [datetime.now(), datetime.now()],
            [Thing(), Thing()],
        ],
    )
    def test_returns_false_for_dict_of_string_keys_and_non_json_primitive_values(
        self, values
    ):
        assert is_json_value({"key1": values[0], "key2": values[1]}) is False

    @pytest.mark.parametrize(
        "keys", [[1, 2], [True, False], [Thing(), Thing()], [(1, 2), (3, 4)]]
    )
    def test_returns_false_for_dict_of_non_string_keys_and_json_primitive_values(
        self, keys
    ):
        assert is_json_value({keys[0]: 1, keys[1]: 2}) is False

    @pytest.mark.parametrize(
        "values", [[None, None], ["a", "b"], [1, 2], [1.1, 2.1], [True, False]]
    )
    def test_returns_true_for_dict_of_dicts_of_string_keys_and_json_primitive_values(
        self, values
    ):
        assert (
            is_json_value(
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
            is True
        )

    @pytest.mark.parametrize(
        "values",
        [
            [1 + 2j, 2 + 3j],
            [datetime.now(), datetime.now()],
            [Thing(), Thing()],
        ],
    )
    def test_returns_false_for_dict_of_dicts_of_string_keys_and_non_json_primitive_values(
        self, values
    ):
        assert (
            is_json_value(
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
            is False
        )

    @pytest.mark.parametrize(
        "keys", [[1, 2], [True, False], [Thing(), Thing()], [(1, 2), (3, 4)]]
    )
    def test_returns_false_for_dict_of_dicts_of_non_string_keys_and_json_primitive_values(
        self, keys
    ):
        assert (
            is_json_value(
                {
                    "key1": {keys[0]: 1, keys[1]: 2},
                    "key2": {keys[0]: 1, keys[1]: 2},
                }
            )
            is False
        )

    def test_returns_true_for_deeply_nested_dict_with_mixed_json_values(self):
        assert (
            is_json_value(
                {
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
            )
            is True
        )

    def test_returns_false_for_deeply_nested_dict_with_mixed_non_json_values(
        self,
    ):
        assert (
            is_json_value(
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
            is False
        )
