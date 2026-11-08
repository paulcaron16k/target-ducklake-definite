"""JSON text in object and array properties: one rule (DUCKLKD-123)."""

from __future__ import annotations

import io
import json
from decimal import Decimal
from typing import TYPE_CHECKING, Any

import pytest

from target_ducklake.conform import conform_name
from target_ducklake.flatten import (
    JsonTextRule,
    decode_json_text,
    flatten_record,
    flatten_schema,
    json_text_rule,
)
from target_ducklake.target import Targetducklake

if TYPE_CHECKING:
    from tests.catalogs import Lake

ARRAY = {"type": ["null", "array"], "items": {"type": "string"}}
OBJECT = {"type": ["null", "object"]}
# Postgres catalogs truncate this name to 63 characters.
LONG = "long_array_column_name_" * 3
# Over 63 characters raw, under 64 once conformed (the "é" are dropped).
ACCENTED = "Tags" + "é" * 70

# A string in an object or array property, and the JSON its column holds.
# Text that encodes an object or array is that value, written as a dict or
# list sent as such would be; any other string is a JSON string.
VALUES = [
    ("[1,2]", "[1, 2]"),
    ('{"a":1}', '{"a": 1}'),
    ("abc", '"abc"'),
    ('"q"', '"\\"q\\""'),
    ("3", '"3"'),
    ("null", '"null"'),
    ("NaN", '"NaN"'),
    ("", '""'),
]


def _run(lake: Lake, properties: dict, records: list[dict], **config: object) -> None:
    """Load ``records`` into s.t in one run, keyed on id."""
    messages: list[dict[str, Any]] = [
        {
            "type": "SCHEMA",
            "stream": "t",
            "schema": {
                "type": "object",
                "properties": {"id": {"type": "integer"}, **properties},
            },
            "key_properties": ["id"],
        },
        *(
            {"type": "RECORD", "stream": "t", "record": {"id": i, **r}}
            for i, r in enumerate(records)
        ),
    ]
    target = Targetducklake(config={**lake.config(), **config})
    try:
        target.listen(
            file_input=io.StringIO("".join(json.dumps(m) + "\n" for m in messages))
        )
    finally:
        target.target_connector.close()


def _column(lake: Lake, column: str) -> list[str]:
    """The column's values as text, in id order."""
    return [
        row[0]
        for row in lake.query(
            f'SELECT "{column}"::VARCHAR FROM lake.s.t ORDER BY id'  # noqa: S608
        )
    ]


class TestRule:
    """Which properties hold JSON text: declared an object or array, not a string."""

    @pytest.mark.parametrize(
        "prop",
        [
            {"type": "array"},
            {"type": "object"},
            ARRAY,
            OBJECT,
            {"type": ["null", "object", "array"]},
            {"anyOf": [{"type": "object"}, {"type": "null"}]},
            {"oneOf": [{"type": "array", "items": {"type": "integer"}}]},
        ],
    )
    def test_containers_decode(self, prop: dict):
        """A string here is the JSON text of the property's value."""
        rule = json_text_rule(prop)
        assert rule is not None
        assert rule.decode

    @pytest.mark.parametrize(
        "prop",
        [
            {},
            True,
            {"type": "string"},
            {"type": ["null", "string", "object"]},
            {"anyOf": [{"type": "object"}, {"type": "string"}]},
            {"type": "integer"},
        ],
        ids=["untyped", "boolean", "string", "string-or-object", "choice", "int"],
    )
    def test_a_string_property_holds_strings(self, prop: object):
        """A string is a string where the schema allows one, or says nothing."""
        assert json_text_rule(prop) is None

    def test_reaches_properties_and_items_at_any_depth(self):
        """An object's properties and an array's items have rules of their own."""
        schema = {
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "meta": {
                    "type": ["null", "string", "object"],
                    "properties": {"lst": ARRAY, "n": {"type": "integer"}},
                },
                "rows": {"type": "array", "items": {"type": "object"}},
            },
        }
        rule = json_text_rule(schema)
        assert rule is not None
        assert set(rule.properties) == {"meta", "rows"}
        meta = rule.properties["meta"]
        assert not meta.decode
        assert set(meta.properties) == {"lst"}
        rows = rule.properties["rows"]
        assert rows.items == JsonTextRule(decode=True)


class TestDecode:
    """decode_json_text replaces object and array text with its value."""

    RULE = JsonTextRule(decode=True)

    @pytest.mark.parametrize(
        ("text", "value"),
        [("[1,2]", [1, 2]), (' {"a": [true]} ', {"a": [True]})],
    )
    def test_object_and_array_text_is_its_value(self, text: str, value: object):
        """Text of an object or array is that object or array."""
        assert decode_json_text(text, self.RULE) == value

    @pytest.mark.parametrize(
        "text", ["abc", '"q"', "3", "true", "null", "NaN", "[1,", ""]
    )
    def test_other_text_stays_text(self, text: str):
        """A JSON null would become SQL NULL; NaN isn't JSON."""
        assert decode_json_text(text, self.RULE) == text

    def test_fractions_keep_their_digits(self):
        """As the singer-sdk parses a message's numbers."""
        assert decode_json_text("[0.1234567890123456789]", self.RULE) == [
            Decimal("0.1234567890123456789")
        ]

    def test_decoded_value_is_decoded_inside_too(self):
        """The text inside a decoded value follows its properties' rules."""
        rule = json_text_rule(
            {"type": "object", "properties": {"lst": {"type": "array"}}}
        )
        assert rule is not None
        assert decode_json_text('{"lst": "[1]"}', rule) == {"lst": [1]}

    def test_record_is_not_changed(self):
        """The caller's record is copied, not changed."""
        rule = json_text_rule({"type": "object", "properties": {"t": ARRAY}})
        assert rule is not None
        record = {"t": "[1]", "u": "[2]"}
        assert decode_json_text(record, rule) == {"t": [1], "u": "[2]"}
        assert record == {"t": "[1]", "u": "[2]"}


def test_flattened_json_leaf_dumps_text_as_a_json_string():
    """A flattened leaf's column type decides as a top-level column's does."""
    schema = {
        "type": "object",
        "properties": {"m": {"type": "object", "properties": {"j": OBJECT}}},
    }
    flat = flatten_schema(schema, max_level=1)
    assert flatten_record({"m": {"j": "abc"}}, flat, max_level=1) == {"m__j": '"abc"'}


@pytest.mark.parametrize(("text", "stored"), VALUES)
@pytest.mark.parametrize(
    "path",
    ["top-level", "flattened", "truncated", "conformed-off", "conformed-on"],
)
def test_one_rule_on_every_path(lake: Lake, path: str, text: str, stored: str):
    """The same text loads the same JSON wherever the property's column is.

    Before DUCKLKD-123 a top-level column stored every string as a JSON
    string, while a flattened leaf or a truncated name loaded the text as
    is (and failed on text that isn't JSON).
    """
    config: dict[str, object] = {"nested_types": "json"}
    if path == "flattened":
        properties: dict = {"meta": {"type": "object", "properties": {"lst": ARRAY}}}
        record: dict = {"meta": {"lst": text}}
        column = "meta__lst"
        config["flatten_max_level"] = 1
    elif path == "top-level":
        properties, record, column = {"tags": ARRAY}, {"tags": text}, "tags"
    else:
        if lake.kind != "postgres":
            pytest.skip("names are truncated on Postgres catalogs only")
        name = LONG if path == "truncated" else ACCENTED
        properties, record, column = {name: ARRAY}, {name: text}, name[:63]
        if path == "conformed-on":
            config["conform_column_names"] = True
            column = conform_name(name)
    _run(lake, properties, [record], **config)
    assert _column(lake, column) == [stored]


def test_text_loads_as_the_value_itself(lake: Lake):
    """Text and the list it encodes are one value; a string property keeps text."""
    _run(
        lake,
        {"tags": ARRAY, "either": {"type": ["null", "string", "array"]}},
        [
            {"tags": "[1, 2]", "either": "[1,2]"},
            {"tags": [1, 2], "either": [1, 2]},
        ],
        nested_types="json",
    )
    assert _column(lake, "tags") == ["[1, 2]", "[1, 2]"]
    assert _column(lake, "either") == ['"[1,2]"', "[1, 2]"]


def test_text_inside_a_json_column(lake: Lake):
    """A nested property declared an array is decoded in its parent's JSON too."""
    properties = {"meta": {"type": "object", "properties": {"lst": ARRAY}}}
    _run(lake, properties, [{"meta": {"lst": "[1,2]"}}], nested_types="json")
    assert _column(lake, "meta") == ['{"lst": [1, 2]}']


class TestNativeColumns:
    """STRUCT and LIST columns (nested_types: native) follow the same rule."""

    def test_list_text_loads_as_a_list(self, lake: Lake):
        """JSON text of a list loads as the list."""
        _run(lake, {"tags": ARRAY}, [{"tags": '["a","b"]'}], nested_types="native")
        assert lake.query("SELECT tags FROM lake.s.t") == [(["a", "b"],)]

    def test_struct_text_loads_as_a_struct(self, lake: Lake):
        """JSON text of an object loads as the struct; its JSON field by the rule."""
        properties = {
            "m": {
                "type": "object",
                "properties": {"a": {"type": "integer"}, "j": OBJECT},
            }
        }
        _run(
            lake,
            properties,
            [{"m": '{"a": 1, "j": "[1,2]"}'}, {"m": {"a": 2, "j": "abc"}}],
            nested_types="native",
        )
        assert lake.query("SELECT m.a, m.j::VARCHAR FROM lake.s.t ORDER BY id") == [
            (1, "[1, 2]"),
            (2, '"abc"'),
        ]

    def test_text_that_is_not_a_list_is_refused(self, lake: Lake):
        """A LIST column can't hold a string, as before."""
        with pytest.raises(ValueError, match="holds lists"):
            _run(lake, {"tags": ARRAY}, [{"tags": "abc"}], nested_types="native")


def test_leaf_sharing_a_property_name_takes_its_own_type(lake: Lake):
    """'meta.tags' (a string) was quoted as JSON because 'tags' is an array."""
    properties = {
        "tags": ARRAY,
        "meta": {"type": "object", "properties": {"tags": {"type": "string"}}},
    }
    _run(
        lake, properties, [{"tags": ["a"], "meta": {"tags": "x"}}], flatten_max_level=1
    )
    assert _column(lake, "meta__tags") == ["x"]
