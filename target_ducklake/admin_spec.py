"""The destination spec the xflow admin UI renders its form from (spec.json).

spec.json is generated from ``Targetducklake.config_jsonschema``, so a title,
description, enum, default or secret flag is written once, in target.py. What
only the form needs is kept here: which settings it offers and in what order,
which it requires, why every other setting is left to an engineer, and how
the load methods read as Airbyte sync modes. ``bin/render-spec.py`` writes
the file; ``tests/test_admin_ui.py`` fails while it is stale.
"""

from __future__ import annotations

import copy
import json
import typing as t

from target_ducklake.layout import LOAD_METHODS
from target_ducklake.target import Targetducklake

DOCUMENTATION_URL = "https://github.com/paulcaron16k/target-ducklake-definite#readme"

# The settings the form offers, in form order. Connection first, then where
# and how rows land.
EXPOSED: tuple[str, ...] = (
    "catalog_type",
    "catalog_url",
    "meta_schema",
    "data_path",
    "storage_type",
    "public_key",
    "secret_key",
    "region",
    "default_target_schema",
    "target_schema_prefix",
    "load_method",
    "batch_size_rows",
)

# The form refuses to submit without these (meltano.yml's
# settings_group_validation says the same).
REQUIRED: tuple[str, ...] = ("catalog_url", "data_path")

_PER_PIPELINE = (
    "decides how one pipeline's records become columns, so an engineer sets it "
    "with the pipeline (often per stream in stream_defaults or "
    "stream_overrides), not an admin once per destination"
)
_LAYOUT = (
    "a per-stream JSON document (or a path to one) an engineer writes for the "
    "pipeline; a form field can't edit it"
)
_TUNING = "tunes the loader process on its host; its default suits a destination"
_SDK = "the singer-sdk's own setting, set with the pipeline in Meltano"

# Every other setting, with why the form leaves it out. A new setting must be
# added to EXPOSED or here; the tests fail until it is.
UI_EXCLUDED: dict[str, str] = {
    "add_record_metadata": _PER_PIPELINE,
    "auto_cast_timestamps": _PER_PIPELINE,
    "conform_column_names": _PER_PIPELINE,
    "convert_tz_to_utc": _PER_PIPELINE,
    "date_time_type": _PER_PIPELINE,
    "delete_rows_with_deleted_at": _PER_PIPELINE,
    "flatten_max_level": _PER_PIPELINE,
    "max_column_length": _PER_PIPELINE,
    "nested_types": _PER_PIPELINE,
    "overwrite_if_no_pk": _PER_PIPELINE,
    "validate_records": _PER_PIPELINE,
    "value_type_checking": _PER_PIPELINE,
    "partition_fields": _LAYOUT,
    "stream_defaults": _LAYOUT,
    "stream_overrides": _LAYOUT,
    "table_config": _LAYOUT,
    "data_inlining_row_limit": _TUNING,
    "inlined_data_flush_rows": _TUNING,
    "max_record_age_minutes": _TUNING,
    "parallel_draining": _TUNING,
    "temp_file_dir": _TUNING,
    "max_batch_size": "deprecated in favour of batch_size_rows, which the form offers",
    "nats": (
        "writeCompleted events need a NATS deployment an engineer wires once, "
        "with its own credentials"
    ),
    "warehouse": "only names the lake in writeCompleted events (see nats)",
    "batch_config": _SDK,
    "faker_config": _SDK,
    "flattening_enabled": _SDK,
    "flattening_max_depth": _SDK,
    "hard_delete": _SDK,
    "process_activate_version_messages": _SDK,
    "stream_map_config": _SDK,
    "stream_maps": _SDK,
}

# What each load method does, in Airbyte's sync-mode words.
SYNC_MODES: dict[str, str] = {
    "append": "append",
    "merge": "append_dedup",
    "overwrite": "overwrite",
}

# The keywords copied from a setting's schema into its form field, in order.
_FIELD_KEYWORDS = (
    "type",
    "title",
    "description",
    "default",
    "enum",
    "minimum",
    "maximum",
    "exclusiveMinimum",
    "pattern",
)


def config_schema() -> dict[str, t.Any]:
    """The target's whole settings surface, the SDK's settings included.

    Returns:
        A deep copy of ``Targetducklake.config_jsonschema``, safe to change.
    """
    return copy.deepcopy(Targetducklake.config_jsonschema)


def form_schema(prop: dict[str, t.Any]) -> dict[str, t.Any]:
    """A setting's schema as a form field takes it: one type, no null.

    The loader accepts several settings as a string too (an environment
    variable can only be a string), as a ``oneOf`` of a plain string, the
    real type and null. A form asks for the real type.

    Args:
        prop: The setting's schema in ``config_jsonschema``.

    Returns:
        The schema with that ``oneOf`` resolved and ``null`` dropped.

    Raises:
        ValueError: If the setting doesn't come down to one type.
    """
    field = {key: value for key, value in prop.items() if key != "oneOf"}
    if "oneOf" in prop:
        real = [
            branch
            for branch in prop["oneOf"]
            if branch.get("type") not in ("string", ["string"], "null")
        ]
        if len(real) != 1:
            msg = f"expected one non-string, non-null type in {prop['oneOf']}"
            raise ValueError(msg)
        field = {**real[0], **field}
    types = field.get("type")
    if isinstance(types, list):
        types = [name for name in types if name != "null"]
        if len(types) != 1:
            msg = f"expected one non-null type, not {field.get('type')}"
            raise ValueError(msg)
        field["type"] = types[0]
    return field


def _form_field(prop: dict[str, t.Any], order: int) -> dict[str, t.Any]:
    """The spec's property for one setting, in the order Airbyte specs use."""
    schema = form_schema(prop)
    field: dict[str, t.Any] = {"order": order}
    field.update({key: schema[key] for key in _FIELD_KEYWORDS if key in schema})
    if prop.get("secret"):
        field["airbyte_secret"] = True
    return {"type": field.pop("type"), "title": field.pop("title"), **field}


def build_spec() -> dict[str, t.Any]:
    """The destination spec, as the admin UI reads it.

    Returns:
        The spec: its envelope and the ``connectionSpecification`` form.

    Raises:
        ValueError: If a load method has no sync mode.
    """
    properties = config_schema()["properties"]
    unmapped = sorted(set(LOAD_METHODS) - set(SYNC_MODES))
    if unmapped:
        msg = f"load methods without a sync mode: {unmapped}"
        raise ValueError(msg)
    return {
        "documentationUrl": DOCUMENTATION_URL,
        # Airbyte's dbt transformations; this target runs none.
        "supportsDbt": False,
        "supportedDestinationSyncModes": sorted(
            {SYNC_MODES[method] for method in properties["load_method"]["enum"]}
        ),
        "connectionSpecification": {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "title": "DuckLake Destination Spec",
            "type": "object",
            "required": list(REQUIRED),
            "additionalProperties": False,
            "properties": {
                name: _form_field(properties[name], order)
                for order, name in enumerate(EXPOSED)
            },
        },
    }


def render_spec() -> str:
    """spec.json's contents.

    Returns:
        The spec as indented JSON, ending in a newline.
    """
    return json.dumps(build_spec(), indent=2, ensure_ascii=False) + "\n"
