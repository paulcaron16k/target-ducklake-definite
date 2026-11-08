"""JSON text in object and array properties: one rule (DUCKLKD-123)."""

from __future__ import annotations

import io
import json
import logging
from decimal import Decimal
from typing import TYPE_CHECKING, Any, ClassVar

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
    from collections.abc import Iterator
    from pathlib import Path

    from tests.catalogs import Lake

ARRAY = {"type": ["null", "array"], "items": {"type": "string"}}
OBJECT = {"type": ["null", "object"]}
# Postgres catalogs truncate this name to 63 characters.
LONG = "long_array_column_name_" * 3
# Over 63 characters raw, under 64 once conformed (the "é" are dropped).
ACCENTED = "Tags" + "é" * 70

# A string in an array property, and the JSON its column holds. Text that
# encodes an array is that array, written as a list sent as such would be;
# any other string (an object's text too) is a JSON string.
VALUES = [
    ("[1,2]", "[1, 2]"),
    ('{"a":1}', '"{\\"a\\":1}"'),
    ("abc", '"abc"'),
    ('"q"', '"\\"q\\""'),
    ("3", '"3"'),
    ("null", '"null"'),
    ("NaN", '"NaN"'),
    ("", '""'),
]


def _run(
    lake: Lake,
    properties: dict,
    records: list[dict],
    *,
    keys: list[str] | None = None,
    batch: Path | None = None,
    **config: object,
) -> None:
    """Load ``records`` into s.t in one run, each with its index as id.

    Keyed on ``keys`` (id by default). With ``batch``, the records are
    written to that JSONL file and sent as one BATCH message.
    """
    records = [{"id": i, **r} for i, r in enumerate(records)]
    loads: list[dict[str, Any]] = (
        [{"type": "RECORD", "stream": "t", "record": r} for r in records]
        if batch is None
        else [
            {
                "type": "BATCH",
                "stream": "t",
                "encoding": {"format": "jsonl", "compression": "none"},
                "manifest": [_jsonl(batch, records)],
            }
        ]
    )
    messages: list[dict[str, Any]] = [
        {
            "type": "SCHEMA",
            "stream": "t",
            "schema": {
                "type": "object",
                "properties": {"id": {"type": "integer"}, **properties},
            },
            "key_properties": ["id"] if keys is None else keys,
        },
        *loads,
    ]
    target = Targetducklake(config={**lake.config(), **config})
    try:
        target.listen(
            file_input=io.StringIO("".join(json.dumps(m) + "\n" for m in messages))
        )
    finally:
        target.target_connector.close()


def _jsonl(path: Path, records: list[dict]) -> str:
    """Write records to a JSONL file; its URL."""
    path.write_text("".join(json.dumps(r) + "\n" for r in records))
    return path.as_uri()


def _column(lake: Lake, column: str) -> list[str]:
    """The column's values as text, in id order."""
    return [
        row[0]
        for row in lake.query(
            f'SELECT "{column}"::VARCHAR FROM lake.s.t ORDER BY id'  # noqa: S608
        )
    ]


def _rule_of(prop: object) -> JsonTextRule | None:
    """The rule of a stream's one property ``p``, if it has one."""
    rule = json_text_rule({"type": "object", "properties": {"p": prop}})
    return None if rule is None else rule.properties["p"]


class TestRule:
    """Which properties hold JSON text: declared an object or array, not a string."""

    @pytest.mark.parametrize(
        ("prop", "decodes"),
        [
            ({"type": "array"}, {"array"}),
            ({"type": "object"}, {"object"}),
            (ARRAY, {"array"}),
            (OBJECT, {"object"}),
            ({"type": ["null", "object", "array"]}, {"object", "array"}),
            ({"anyOf": [{"type": "object"}, {"type": "null"}]}, {"object"}),
            ({"oneOf": [{"type": "array", "items": {"type": "integer"}}]}, {"array"}),
            ({"anyOf": [{"anyOf": [{"type": "array"}]}, {"type": "null"}]}, {"array"}),
            ({"allOf": [{"type": "array"}, {"minItems": 1}]}, {"array"}),
        ],
    )
    def test_containers_decode(self, prop: dict, decodes: set[str]):
        """A string here is the JSON text of the property's declared container."""
        rule = _rule_of(prop)
        assert rule is not None
        assert rule.decodes == decodes

    @pytest.mark.parametrize(
        "prop",
        [
            {},
            True,
            {"$ref": "#/definitions/x"},
            {"type": "string"},
            {"type": "integer"},
            {"type": ["null", "string", "object"]},
            {"anyOf": [{"type": "object"}, {"type": "string"}]},
            {"anyOf": [{"anyOf": [{"type": "string"}]}, {"type": "object"}]},
            {"anyOf": [{"type": "object"}, {"$ref": "#/definitions/s"}]},
            {"oneOf": [{"type": "array"}, {}]},
            {"type": "object", "enum": ["a", {"b": 1}]},
            {"type": ["array"], "const": "[1]"},
        ],
        ids=[
            "untyped",
            "boolean",
            "ref",
            "string",
            "int",
            "string-or-object",
            "choice",
            "nested-choice",
            "ref-branch",
            "untyped-branch",
            "enum",
            "const",
        ],
    )
    def test_a_string_property_holds_strings(self, prop: object):
        """A string is a string where the schema allows one, or may say nothing."""
        assert _rule_of(prop) is None

    def test_a_stream_without_containers_has_none(self):
        """A record is an object, but only its properties count."""
        schema = {
            "type": "object",
            "properties": {"a": {"type": "string"}, "b": {"type": "integer"}},
        }
        assert json_text_rule(schema) is None
        assert json_text_rule({"type": "object"}) is None

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
        assert not meta.decodes
        assert set(meta.properties) == {"lst"}
        rows = rule.properties["rows"]
        assert rows.items == JsonTextRule(decodes=frozenset({"object"}))


class TestDecode:
    """decode_json_text replaces object and array text with its value."""

    RULE = JsonTextRule(decodes=frozenset({"object", "array"}))

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

    def test_only_the_declared_container(self):
        """Object text in an array property stays text, and the other way round."""
        array = JsonTextRule(decodes=frozenset({"array"}))
        obj = JsonTextRule(decodes=frozenset({"object"}))
        assert decode_json_text('{"a": 1}', array) == '{"a": 1}'
        assert decode_json_text("[1]", obj) == "[1]"
        assert decode_json_text("[1]", array) == [1]

    def test_fractions_keep_their_digits(self):
        """As the singer-sdk parses a message's numbers."""
        assert decode_json_text("[0.1234567890123456789]", self.RULE) == [
            Decimal("0.1234567890123456789")
        ]

    def test_decoded_value_is_decoded_inside_too(self):
        """The text inside a decoded value follows its properties' rules."""
        rule = _rule_of({"type": "object", "properties": {"lst": {"type": "array"}}})
        assert rule is not None
        assert decode_json_text('{"lst": "[1]"}', rule) == {"lst": [1]}

    def test_record_is_not_changed(self):
        """The caller's record is copied, not changed; other keys are kept."""
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
    config: dict[str, Any] = {"nested_types": "json"}
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
            [{"m": '{"a": 1, "j": "{\\"b\\": [1]}"}'}, {"m": {"a": 2, "j": "abc"}}],
            nested_types="native",
        )
        assert lake.query("SELECT m.a, m.j::VARCHAR FROM lake.s.t ORDER BY id") == [
            (1, '{"b": [1]}'),
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


@pytest.fixture
def warnings(caplog: pytest.LogCaptureFixture) -> Iterator[pytest.LogCaptureFixture]:
    """The target's warnings, caught on its own logger."""
    logger = logging.getLogger("target-ducklake")
    logger.addHandler(caplog.handler)
    caplog.handler.setLevel(logging.WARNING)
    try:
        yield caplog
    finally:
        logger.removeHandler(caplog.handler)


class TestKeys:
    """A key declared an object or array (README: Existing tables)."""

    PROPERTIES: ClassVar[dict] = {"k": ARRAY, "v": {"type": "string"}}

    def test_text_and_its_value_are_one_key(self, lake: Lake):
        """In one run, a key sent as text and as the list is one row."""
        _run(
            lake,
            self.PROPERTIES,
            [{"k": "[1,2]", "v": "text"}, {"k": [1, 2], "v": "list"}],
            keys=["k"],
            nested_types="json",
        )
        assert lake.query("SELECT k::VARCHAR, v FROM lake.s.t") == [("[1, 2]", "list")]

    def test_keys_stored_as_json_strings_no_longer_match(
        self, lake: Lake, warnings: pytest.LogCaptureFixture
    ):
        """Rows keyed by a JSON string, as before the fix, are loaded again.

        A key that allowed a string stores the text as a JSON string, as every
        array key did before DUCKLKD-123; declared an array only, the same
        text is now the array, which a merge doesn't match. Setup warns.
        """
        either = {"k": {"type": ["null", "string", "array"]}, "v": {"type": "string"}}
        _run(
            lake, either, [{"k": "[1,2]", "v": "old"}], keys=["k"], nested_types="json"
        )
        _run(
            lake,
            self.PROPERTIES,
            [{"k": "[1,2]", "v": "new"}],
            keys=["k"],
            nested_types="json",
        )
        assert lake.query("SELECT k::VARCHAR, v FROM lake.s.t ORDER BY v") == [
            ("[1, 2]", "new"),
            ('"[1,2]"', "old"),
        ]
        warned = [
            r.getMessage() for r in warnings.records if "key property" in r.getMessage()
        ]
        assert len(warned) == 1
        assert "'k' is declared array" in warned[0]

    def test_no_warning_for_a_table_loaded_the_new_way(
        self, lake: Lake, warnings: pytest.LogCaptureFixture
    ):
        """Nothing was loaded the old way: no warning, on any run.

        A JSON string key that isn't a container's text (``abc``) is stored
        the same way before and after, so it isn't warned about either.
        """
        for _ in range(2):
            _run(
                lake,
                self.PROPERTIES,
                [{"k": "[1]"}, {"k": "abc"}],
                keys=["k"],
                nested_types="json",
            )
        assert lake.query("SELECT k::VARCHAR FROM lake.s.t ORDER BY 1") == [
            ('"abc"',),
            ("[1]",),
        ]
        assert not [r for r in warnings.records if "key property" in r.getMessage()]


class TestFlattenedObjectText:
    """An object property sent as text, then flattened (flatten_max_level)."""

    PROPERTIES: ClassVar[dict] = {
        "m": {
            "type": "object",
            "properties": {
                "l": ARRAY,
                "n": {"type": "integer"},
                "s": {"type": "string"},
            },
        }
    }

    def test_its_fields_load(self, lake: Lake):
        """Before DUCKLKD-123 every flattened field was NULL."""
        _run(
            lake,
            self.PROPERTIES,
            [{"m": '{"l": "[1]", "n": 5, "s": "x"}'}],
            flatten_max_level=1,
            nested_types="json",
        )
        assert lake.query("SELECT m__l::VARCHAR, m__n, m__s FROM lake.s.t") == [
            ("[1]", 5, "x")
        ]

    def test_text_that_is_not_an_object_leaves_its_fields_null(self, lake: Lake):
        """As before: there is no object to flatten (README)."""
        _run(
            lake,
            self.PROPERTIES,
            [{"m": "garbage"}],
            flatten_max_level=1,
            nested_types="json",
        )
        assert lake.query("SELECT m__l, m__n, m__s FROM lake.s.t") == [
            (None, None, None)
        ]


def test_validate_records_still_refuses_text(lake: Lake):
    """Records are validated as the tap sent them, before decoding."""
    with pytest.raises(Exception, match="is not of type 'null', 'array'"):
        _run(lake, {"tags": ARRAY}, [{"tags": "[1,2]"}], validate_records=True)


def test_batch_messages_follow_the_rule(lake: Lake, tmp_path: Path):
    """BATCH records take the RECORD path, so their text is decoded too."""
    _run(
        lake,
        {"tags": ARRAY},
        [{"tags": "[1,2]"}, {"tags": "abc"}],
        batch=tmp_path / "b.jsonl",
        nested_types="json",
    )
    assert _column(lake, "tags") == ["[1, 2]", '"abc"']


def test_type_source_string_keeps_text(lake: Lake):
    """A property type_source declares a string isn't decoded."""
    _run(
        lake,
        {"tags": ARRAY},
        [{"tags": "[1,2]"}],
        stream_defaults={"type_source": {"tags": "string"}},
    )
    assert lake.query("SELECT tags, typeof(tags) FROM lake.s.t") == [
        ("[1,2]", "VARCHAR")
    ]


def test_normalised_as_the_value_it_encodes(lake: Lake):
    """Decoded text is written as its value is: numbers and repeated keys."""
    _run(
        lake,
        {"o": OBJECT},
        [{"o": '{"a": 1.0e-5, "b": [1.10], "c": 1, "c": 2}'}],
        nested_types="json",
    )
    assert _column(lake, "o") == ['{"a": 0.000010, "b": [1.10], "c": 2}']
