"""Adapted from target-duckdb and Meltano SDK
https://github.com/jwills/target-duckdb/blob/36c8ce68a0b2584c4bbb07325482968b1edc0c40/target_duckdb/db_sync.py#L74
https://github.com/meltano/sdk/blob/35b499d6163f91752fd674f2a3e46a7e22a1fb47/singer_sdk/helpers/_flattening.py#L342
"""

import collections
import copy
import itertools
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import date, datetime

import simplejson

from target_ducklake.type_hints import sql_datatype_hint

TIMESTAMP_COLUMN_NAMES = {
    "timestamp",
    "created_at",
    "updated_at",
    "deleted_at",
    "modified_at",
    "last_modified",
    "updatedat",
    "createdat",
    "created_time",
    "updated_time",
}


def _json_default(obj: object) -> str:
    """Encode the values JSON has no type for: dates and datetimes, as ISO text."""
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    msg = f"Object of type {type(obj).__name__} is not JSON serializable"
    raise TypeError(msg)


def dump_json_value(value: object) -> str:
    """Serialise a value stored in a JSON column as JSON text.

    The SDK parses every JSON number with a fraction as a Decimal. It is
    written as a JSON number with all its digits (``use_decimal``), not
    through float, which keeps only about 15 significant digits. Every other
    value is written as the stdlib ``json.dumps`` writes it: NaN and Infinity
    as such, and namedtuples as arrays.
    """
    return simplejson.dumps(
        value,
        use_decimal=True,
        default=_json_default,
        allow_nan=True,
        namedtuple_as_object=False,
    )


def flatten_key(k, parent_key, sep):
    return sep.join(parent_key + [k])


def _should_auto_cast_to_timestamp(
    column_name: str, schema_entry: dict, auto_cast_timestamps: bool
) -> bool:
    """Check if a column should be auto-cast to timestamp format."""
    if not auto_cast_timestamps:
        return False

    # Skip if format is already specified
    if schema_entry.get("format") is not None:
        return False

    # Check if column name matches timestamp patterns
    if column_name.lower() not in TIMESTAMP_COLUMN_NAMES:
        return False

    # Check if it's a string type (either single string or list containing string)
    entry_type = schema_entry.get("type", [])
    if isinstance(entry_type, str):
        return entry_type == "string"
    if isinstance(entry_type, list):
        return "string" in entry_type

    return False


def _apply_timestamp_format(schema_entry: dict) -> dict:
    """Apply timestamp format to a schema entry."""
    result = dict(schema_entry)  # Create a copy
    result["format"] = "date-time"
    return result


# pylint: disable-msg=too-many-arguments
def flatten_record(  # noqa: PLR0913
    d,
    flatten_schema=None,
    parent_key=[],
    sep="__",
    level=0,
    max_level=0,
    native_columns: frozenset[str] = frozenset(),
    renamed: Mapping[str, str] | None = None,
):
    """Flatten a record to its columns, objects and arrays as JSON text.

    ``renamed`` maps flattened names to the column names they are loaded as
    (conformed or truncated); ``flatten_schema`` and ``native_columns`` use
    the column names. A column in ``native_columns`` (a STRUCT or LIST
    column) keeps its dicts and lists, for staging to build the nested value.

    Dicts and lists are dumped as JSON text. A JSON column (one declared an
    object or array) has any other value dumped too, so text is a JSON
    string; this holds for every column, top-level or flattened, whatever
    its name became. JSON text the property's declared type says is an
    object or array was decoded before (``decode_json_text``).
    """
    renamed = renamed or {}
    items = []
    for k, v in d.items():
        new_key = flatten_key(k, parent_key, sep)
        column = renamed.get(new_key, new_key)
        if isinstance(v, collections.abc.MutableMapping) and level < max_level:
            items.extend(
                flatten_record(
                    v,
                    flatten_schema,
                    parent_key + [k],
                    sep=sep,
                    level=level + 1,
                    max_level=max_level,
                    native_columns=native_columns,
                    renamed=renamed,
                ).items()
            )
        elif column in native_columns:
            items.append((column, v))
        else:
            items.append(
                (
                    column,
                    dump_json_value(v)
                    # A missing value stays None (SQL NULL), not the JSON text
                    # 'null'; a null inside a value is still dumped as JSON.
                    if v is not None
                    and _should_json_dump_value(column, v, flatten_schema)
                    else v,
                )
            )
    return dict(items)


# A property with no usable type can hold any value, so it is stored as JSON
# text, which keeps every value intact.
UNTYPED_COLUMN: dict[str, list[str]] = {"type": ["null", "object"]}


# JSON Schema keywords that list the alternatives a value can take. A property
# declared with one of them (and no "type") gets its column from a branch.
_CHOICE_KEYWORDS = ("anyOf", "oneOf", "allOf")


def _non_null_types(branch: object) -> list[str]:
    """A branch's declared types other than null, in their declared order."""
    if not isinstance(branch, dict):
        return []
    types = branch.get("type") or []
    types = [types] if isinstance(types, str) else types
    return [t for t in types if t != "null"]


def _choice_column(prop: dict, keyword: str, *, nested: bool = False) -> dict:
    """The column for an anyOf/oneOf/allOf property: its first typed branch.

    Null-only and untyped branches (a ``$ref``, a nested choice, ``{}``) are
    skipped; with no typed branch left, the column is ``UNTYPED_COLUMN``. The
    column's type is null followed by the branch's types in their declared
    order, as for a plain type list, so it is the same in every process.
    """
    branches = prop[keyword] if isinstance(prop[keyword], list) else []
    branch = next((b for b in branches if _non_null_types(b)), None)
    if branch is None:
        return UNTYPED_COLUMN
    # With nested types, an object or array chosen from alternatives stays
    # JSON: its shape isn't the column's, since a value may take another branch.
    column = {
        key: value
        for key, value in branch.items()
        if not (nested and key in _SHAPE_KEYWORDS)
    }
    column["type"] = ["null", *_non_null_types(branch)]
    return _with_wrapper_hint(prop, column)


# The keywords that give an object's or an array's shape.
_SHAPE_KEYWORDS = ("properties", "items")


def _nested_column(prop: object) -> dict:
    """The column schema of a struct field or list element (``nested_types: native``).

    A field or element is typed as a top-level property would be, at any
    depth; an object or array of its own becomes a STRUCT or LIST again when
    its shape is declared.
    """
    if not isinstance(prop, dict):  # a boolean schema
        return UNTYPED_COLUMN
    return _leaf_column(
        {key: value for key, value in prop.items() if key != "description"},
        nested=True,
    )


def _native_shape(prop: dict) -> dict | None:
    """The STRUCT or LIST column of a plainly typed object or array, if it has one.

    An ``object`` with non-empty ``properties`` keeps them, and an ``array``
    whose ``items`` have a type keeps them, each normalised by
    ``_nested_column``. Several non-null types, an object without
    properties, an array without typed items: None, so the column is JSON.
    """
    types = _non_null_types(prop)
    if types == ["object"]:
        properties = prop.get("properties")
        if isinstance(properties, dict) and properties and _struct_names(properties):
            return {
                **prop,
                "type": ["null", "object"],
                "properties": {
                    name: _nested_column(child) for name, child in properties.items()
                },
            }
    if types == ["array"]:
        items = _nested_column(prop.get("items"))
        # Missing or untyped items ({}, a $ref) give UNTYPED_COLUMN itself.
        if items is not UNTYPED_COLUMN and _non_null_types(items):
            return {**prop, "type": ["null", "array"], "items": items}
    return None


def _struct_names(properties: dict) -> bool:
    """Whether a STRUCT can hold these names: none empty, none equal ignoring case.

    DuckDB matches struct field names case-insensitively and refuses an
    empty one; such an object stays JSON, as it loaded before nested types.
    """
    names = [str(name).lower() for name in properties]
    return all(names) and len(set(names)) == len(names)


def _leaf_column(prop: dict, *, nested: bool = False) -> dict:
    """The column for a property that is not flattened into further columns.

    With ``nested`` (``nested_types: native``), a plainly typed object or
    array with a declared shape keeps it (see ``_native_shape``).
    """
    if nested and "type" in prop and (shaped := _native_shape(prop)) is not None:
        return shaped
    if "type" in prop:
        # certain taps (e.g. postgres) sometimes return a list of types for JSON columns
        # we need to handle this specific case, if we don't the schema returns VARCHAR
        # may need to revisit this for other types
        if "array" in prop["type"]:
            return {"type": ["null", "array"]}
        if "object" in prop["type"]:
            return {"type": ["null", "object"]}
        return prop
    if keyword := next((kw for kw in _CHOICE_KEYWORDS if kw in prop), None):
        return _choice_column(prop, keyword, nested=nested)
    return UNTYPED_COLUMN  # no type information at all, e.g. {}


def _with_wrapper_hint(prop: dict, branch: dict) -> dict:
    """Carry a choice property's own x-sql-datatype hint into its chosen branch.

    The hint describes the column whichever branch is chosen; a hint on the
    branch itself wins, and an empty one counts as no hint. Only the hint is
    carried over, so no other key on the wrapper (such as ``format``) changes
    the column's type.
    """
    if sql_datatype_hint(prop) and not sql_datatype_hint(branch):
        return {**branch, "x-sql-datatype": prop["x-sql-datatype"]}
    return branch


# pylint: disable=dangerous-default-value,invalid-name
def flatten_schema(  # noqa: PLR0913
    d,
    parent_key=[],
    sep="__",
    level=0,
    max_level=0,
    auto_cast_timestamps=False,
    *,
    nested: bool = False,
):
    """This function is a bit of a blackbox. Adapted from DuckDB target and Meltano SDK.
    See note mentioned here in official Meltano SDK repo:
    https://github.com/meltano/sdk/blob/35b499d6163f91752fd674f2a3e46a7e22a1fb47/singer_sdk/helpers/_flattening.py#L342

    With ``nested`` (``nested_types: native``), an object below
    ``max_level`` with declared properties, and an array with typed items,
    keep their ``properties`` and ``items``, for STRUCT and LIST columns.
    """
    items = []

    if "properties" not in d:
        return {}

    for k, prop in d["properties"].items():
        new_key = flatten_key(k, parent_key, sep)
        if not isinstance(prop, dict):  # a boolean schema: true allows any value
            items.append((new_key, UNTYPED_COLUMN))
            continue
        # some schemas from airbyte sources have description field which is not needed and causes issues
        # (left out of a copy, so the caller's schema keeps it)
        v = {key: value for key, value in prop.items() if key != "description"}

        if (
            "type" in v
            and "object" in v["type"]
            and "properties" in v
            and level < max_level
        ):
            items.extend(
                flatten_schema(
                    v,
                    parent_key + [k],
                    sep=sep,
                    level=level + 1,
                    max_level=max_level,
                    auto_cast_timestamps=auto_cast_timestamps,
                    nested=nested,
                ).items()
            )
            continue
        column = _leaf_column(v, nested=nested)
        # Timestamp auto-casting applies to the column, however it was declared;
        # a format on a choice wrapper (not carried into the column) also stops it.
        if v.get("format") is None and _should_auto_cast_to_timestamp(
            new_key, column, auto_cast_timestamps
        ):
            column = _apply_timestamp_format(column)
        items.append((new_key, column))

    key_func = lambda item: item[0]
    sorted_items = sorted(items, key=key_func)
    for k, g in itertools.groupby(sorted_items, key=key_func):
        if len(list(g)) > 1:
            raise ValueError(f"Duplicate column name produced in schema: {k}")

    # A deep copy, so editing a column never reaches the caller's schema or
    # UNTYPED_COLUMN.
    return copy.deepcopy(dict(sorted_items))


def nullable_choices(schema: dict) -> dict:
    """Return a copy of a stream schema in which anyOf/oneOf properties accept null.

    Every DuckLake column this target creates is nullable, and record
    validation has always accepted null for most of these properties, so a tap
    that sends null where its choice list doesn't declare it still loads. Any
    property with anyOf or oneOf accepts null, whatever else it declares, at
    any depth of nested object properties. Only null is added; other values
    validate as before. The given schema is not changed.
    """
    if "properties" not in schema:
        return schema
    properties = {}
    for name, prop in schema["properties"].items():
        if isinstance(prop, dict) and ("anyOf" in prop or "oneOf" in prop):
            properties[name] = {"anyOf": [prop, {"type": "null"}]}
        elif isinstance(prop, dict):
            properties[name] = nullable_choices(prop)
        else:
            properties[name] = prop
    return {**schema, "properties": properties}


# The JSON Schema types JSON text can be decoded to, and their Python types.
_CONTAINERS: dict[str, type] = {"object": dict, "array": list}

# The kind of value a schema allows when it says nothing of its type: ``{}``,
# ``true``, or a ``$ref`` (not resolved here). It may be a string.
_ANY = "any"


@dataclass(frozen=True)
class JsonTextRule:
    """Where a value's strings are JSON text, from a property's declared schema.

    Built by ``json_text_rule`` and applied by ``decode_json_text``. Keyed
    by the record's own property names, so it holds whatever the columns
    are named once flattened, conformed or truncated.

    Attributes:
        decodes: The containers (``object``, ``array``) a string here is
            the JSON text of: those the property is declared, when it
            can't be a string. Empty when a string here is a string.
        properties: The rules of the object's properties that have one.
        items: The rule of the array's items, if they have one.
    """

    decodes: frozenset[str] = frozenset()
    properties: Mapping[str, "JsonTextRule"] = field(default_factory=dict)
    items: "JsonTextRule | None" = None


def _branches(schema: dict, keyword: str) -> list:
    """The branches of a schema's ``anyOf``, ``oneOf`` or ``allOf``."""
    branches = schema.get(keyword)
    return branches if isinstance(branches, list) else []


def _value_kinds(schema: object) -> set[str]:
    """The kinds of value a schema allows: its non-null types, or ``_ANY``.

    ``type`` decides when present. Otherwise ``anyOf`` and ``oneOf`` allow
    what any branch allows, and ``allOf`` what every branch that says
    something allows, at any depth. A schema with none of them allows
    anything. An ``enum`` or ``const`` with a string value allows a string,
    whatever ``type`` says.
    """
    if not isinstance(schema, dict):  # a boolean schema
        return {_ANY} if schema is True else set()
    if "type" in schema:
        kinds = set(_non_null_types(schema))
    else:
        choices = [
            _value_kinds(branch)
            for keyword in ("anyOf", "oneOf")
            for branch in _branches(schema, keyword)
        ]
        every = [
            kinds
            for branch in _branches(schema, "allOf")
            if (kinds := _value_kinds(branch)) != {_ANY}
        ]
        if choices:
            kinds = set().union(*choices)
        elif every:
            kinds = set.intersection(*every)
        else:
            kinds = {_ANY}
    enum = schema.get("enum")
    pinned = [*(enum if isinstance(enum, list) else []), schema.get("const")]
    if any(isinstance(value, str) for value in pinned):
        kinds.add("string")
    return kinds


def _property_rule(prop: object) -> JsonTextRule | None:
    """A property's ``JsonTextRule``, or None if it needs none at any depth."""
    if not isinstance(prop, dict):  # a boolean schema
        return None
    kinds = _value_kinds(prop)
    decodes = (
        frozenset()
        if kinds & {"string", _ANY}
        else frozenset(kinds & _CONTAINERS.keys())
    )
    children = prop.get("properties")
    properties = {
        name: rule
        for name, child in (children.items() if isinstance(children, dict) else ())
        if (rule := _property_rule(child)) is not None
    }
    items = _property_rule(prop.get("items"))
    if not decodes and not properties and items is None:
        return None
    return JsonTextRule(decodes=decodes, properties=properties, items=items)


def json_text_rule(schema: dict) -> JsonTextRule | None:
    """Where a stream's records hold JSON text, or None if nowhere.

    A string in a property declared an object or an array, that can't be a
    string, can only be the JSON text of its value, which some taps send.
    Its declared types are read through ``anyOf``/``oneOf``/``allOf`` at any
    depth; a property that also allows a string (a type, a branch, an
    ``enum`` or ``const`` value), or may be anything (no type, a ``$ref``),
    holds a string as a string. The rule reaches the properties of an object
    (``properties``) and the items of an array (``items``), at any depth,
    flattened or not.
    """
    # The record itself is an object, never text: only its properties count.
    root = _property_rule({"properties": schema.get("properties")})
    return root if root is not None and root.properties else None


def _decode_container(text: str, decodes: frozenset[str]) -> object:
    """The container of a kind in ``decodes`` that JSON text encodes, else the text.

    Parsed as the singer-sdk parses messages, a number with a fraction as a
    Decimal. Text that isn't JSON (NaN and Infinity aren't), or encodes
    another value (a string, number, boolean or null, or an object where
    only an array is declared), stays text: a JSON null would otherwise
    become SQL NULL, and the property is declared that container.
    """
    try:
        value = simplejson.loads(text, use_decimal=True)
    except (ValueError, RecursionError):
        return text
    if any(isinstance(value, _CONTAINERS[kind]) for kind in decodes):
        return value
    return text


def decode_json_text(value: object, rule: JsonTextRule) -> object:
    """Replace JSON text with the object or array it encodes, where ``rule`` says.

    Returns ``value`` with each string the rule decodes (see
    ``JsonTextRule``) that is the JSON text of its declared container
    replaced by that container, which then loads as one sent as such.
    Containers on the way are copied; ``value`` itself is not changed.
    """
    if isinstance(value, str):
        if not rule.decodes:
            return value
        decoded = _decode_container(value, rule.decodes)
        return value if decoded is value else decode_json_text(decoded, rule)
    if isinstance(value, dict) and rule.properties:
        copied = dict(value)
        for name, child in rule.properties.items():
            if name in copied:
                copied[name] = decode_json_text(copied[name], child)
        return copied
    if isinstance(value, list) and rule.items is not None:
        return [decode_json_text(item, rule.items) for item in value]
    return value


def truncate_column_names(
    schema: dict, max_length: int = 63
) -> tuple[dict, dict[str, str]]:
    """Truncate schema column names to max_length, resolving collisions.

    PostgreSQL truncates identifiers to 63 characters (NAMEDATALEN).
    This function applies the same truncation to avoid DuckLake binder errors
    when using a Postgres catalog.

    Returns (new_schema, old_to_new_name_mapping).
    The mapping only includes names that actually changed.

    Collision resolution: when multiple columns map to the same truncated name,
    each is shortened further and given a ``_{i}`` suffix (0-indexed).
    """
    if max_length <= 0:
        return schema, {}

    # First pass: truncate names that exceed max_length
    truncated: dict[str, str] = {}
    for name in schema:
        truncated[name] = name[:max_length] if len(name) > max_length else name

    # Detect collisions
    seen: dict[str, list[str]] = {}
    for original, trunc in truncated.items():
        seen.setdefault(trunc, []).append(original)

    # Resolve collisions with _{i} suffixes
    mapping: dict[str, str] = {}
    for trunc, originals in seen.items():
        if len(originals) == 1:
            original = originals[0]
            if trunc != original:
                mapping[original] = trunc
        else:
            for i, original in enumerate(originals):
                suffix = f"_{i}"
                new_name = trunc[: max_length - len(suffix)] + suffix
                mapping[original] = new_name

    # Build new schema preserving insertion order
    new_schema = {mapping.get(name, name): value for name, value in schema.items()}
    return new_schema, mapping


# pylint: disable=redefined-outer-name
def _should_json_dump_value(key, value, flatten_schema=None):
    """Adapted from here: https://github.com/jwills/target-duckdb/blob/36c8ce68a0b2584c4bbb07325482968b1edc0c40/target_duckdb/db_sync.py#L125
    """
    if isinstance(value, (dict, list)):
        return True

    if flatten_schema and key in flatten_schema and "type" in flatten_schema[key]:
        t = flatten_schema[key]["type"]
        t = [t] if isinstance(t, str) else t
        # If array OR object type, return True. We need to JSON dump these values.
        if "array" in t or "object" in t:
            return True

    return False
