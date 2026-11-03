"""Per-stream load method and key properties (DUCKLKD-41)."""

from __future__ import annotations

import io
import json
from typing import TYPE_CHECKING, Any

import pytest

from target_ducklake.layout import (
    LayoutError,
    check_layout_config,
    resolve_key_properties,
    resolve_load_method,
)
from target_ducklake.target import Targetducklake

if TYPE_CHECKING:
    from tests.catalogs import Lake

PROPERTIES = {
    "id": {"type": "integer"},
    "v": {"type": ["null", "string"]},
    "_sdc_deleted_at": {"type": ["null", "string"], "format": "date-time"},
}


def _schema(stream: str, keys: list[str] | None) -> dict:
    message: dict[str, Any] = {
        "type": "SCHEMA",
        "stream": stream,
        "schema": {"type": "object", "properties": PROPERTIES},
    }
    if keys is not None:
        message["key_properties"] = keys
    return message


def _run(
    lake: Lake,
    streams: dict[str, list[dict]],
    *,
    keys: list[str] | None = None,
    **config: object,
) -> None:
    """Load each stream's records in one run; every stream declares ``keys``."""
    messages: list[dict] = []
    for stream, records in streams.items():
        messages.append(_schema(stream, keys))
        messages.extend(
            {"type": "RECORD", "stream": stream, "record": r} for r in records
        )
    target = Targetducklake(config={**lake.config(), **config})
    try:
        target.listen(
            file_input=io.StringIO("".join(json.dumps(m) + "\n" for m in messages))
        )
    finally:
        target.target_connector.close()


FIRST = [{"id": 1, "v": "a"}, {"id": 2, "v": "b"}]
SECOND = [{"id": 2, "v": "b2"}, {"id": 3, "v": "c"}]


def _rows(lake: Lake, table: str) -> list[tuple]:
    return lake.query(f"SELECT id, v FROM lake.s.{table} ORDER BY ALL")  # noqa: S608


class TestLoadMethodSetting:
    """load_method, and target-iceberg's write_mode, per stream."""

    @pytest.mark.parametrize(
        ("config", "expected"),
        [
            ({}, "merge"),
            ({"load_method": "append"}, "append"),
            (
                {
                    "load_method": "append",
                    "stream_defaults": {"load_method": "overwrite"},
                },
                "overwrite",
            ),
            (
                {
                    "load_method": "append",
                    "stream_defaults": {"load_method": "overwrite"},
                    "stream_overrides": {"t": {"load_method": "merge"}},
                },
                "merge",
            ),
            ({"stream_overrides": {"other": {"load_method": "append"}}}, "merge"),
            (
                {
                    "stream_overrides": {"t": {"load_method": None}},
                    "load_method": "append",
                },
                "append",
            ),
        ],
        ids=["default", "top-level", "defaults", "override", "other-stream", "null"],
    )
    def test_precedence(self, config: dict, expected: str):
        """Override, then stream_defaults, then the top level, then merge."""
        assert resolve_load_method(config, "t") == expected

    @pytest.mark.parametrize(
        ("mode", "expected"),
        [("append-only", "append"), ("upsert", "merge"), ("overwrite", "overwrite")],
    )
    def test_write_mode_names_a_load_method(self, mode: str, expected: str):
        """target-iceberg's values, in a shared config, are honoured."""
        config = {
            "load_method": "append",
            "stream_overrides": {"t": {"write_mode": mode}},
        }
        assert resolve_load_method(config, "t") == expected

    def test_an_override_write_mode_wins_over_a_default_load_method(self):
        """write_mode is load_method at its own level, so the override wins."""
        config = {
            "stream_defaults": {"load_method": "append"},
            "stream_overrides": {"t": {"write_mode": "upsert"}},
        }
        assert resolve_load_method(config, "t") == "merge"
        assert resolve_load_method(config, "u") == "append"

    def test_an_override_load_method_wins_over_a_default_write_mode(self):
        """And the other way round."""
        config = {
            "stream_defaults": {"write_mode": "upsert"},
            "stream_overrides": {"t": {"load_method": "append"}},
        }
        assert resolve_load_method(config, "t") == "append"
        assert resolve_load_method(config, "u") == "merge"

    def test_both_at_one_level_must_agree(self):
        """Agreeing is fine; disagreeing fails at startup, naming both."""
        agree = {
            "stream_overrides": {"t": {"write_mode": "upsert", "load_method": "merge"}}
        }
        assert resolve_load_method(agree, "t") == "merge"
        disagree = {
            "stream_defaults": {"write_mode": "append-only", "load_method": "merge"}
        }
        with pytest.raises(
            LayoutError, match="write_mode 'append-only' means load_method 'append'"
        ):
            check_layout_config(disagree)

    @pytest.mark.parametrize(
        ("layout", "message"),
        [
            ({"load_method": "upsert"}, "load_method must be one of"),
            ({"load_method": "APPEND"}, "load_method must be one of"),
            ({"write_mode": "merge"}, "write_mode must be one of"),
            ({"write_mode": "append"}, "target-iceberg's values"),
            ({"write_mode": 1}, "write_mode must be one of"),
        ],
        ids=[
            "sdk-value-as-load-method",
            "case",
            "ducklake-value-as-write-mode",
            "append",
            "not-a-string",
        ],
    )
    def test_a_bad_value_is_rejected_at_startup(self, layout: dict, message: str):
        """Each setting takes its own vocabulary; the message names the other."""
        with pytest.raises(LayoutError, match=message):
            check_layout_config({"stream_overrides": {"t": layout}})


class TestKeyPropertiesSetting:
    """key_properties per stream."""

    def test_unset_keeps_the_taps(self):
        """None means the tap's keys are used."""
        assert resolve_key_properties({}, "t") is None

    def test_override_then_defaults(self):
        """An override wins over stream_defaults."""
        config = {
            "stream_defaults": {"key_properties": ["id"]},
            "stream_overrides": {"t": {"key_properties": ["id", "v"]}},
        }
        assert resolve_key_properties(config, "t") == ["id", "v"]
        assert resolve_key_properties(config, "u") == ["id"]

    @pytest.mark.parametrize(
        "value",
        [[], "id", [1], ["id", "id"], [""]],
        ids=["empty", "string", "number", "duplicate", "blank"],
    )
    def test_a_bad_value_is_rejected_at_startup(self, value: object):
        """A non-empty list of distinct names."""
        config = {"stream_overrides": {"t": {"key_properties": value}}}
        with pytest.raises(LayoutError, match="non-empty list of distinct"):
            check_layout_config(config)


class TestThroughListen:
    """On a real catalog (SQLite, and Postgres when set)."""

    def test_one_tap_mixes_load_methods(self, lake: Lake):
        """Stream events appends, dims merges and snap overwrites, in the same runs."""
        overrides = {
            "events": {"load_method": "append"},
            "snap": {"write_mode": "overwrite"},
        }
        for records in (FIRST, SECOND):
            _run(
                lake,
                {"events": records, "dims": records, "snap": records},
                keys=["id"],
                stream_overrides=overrides,
            )
        assert _rows(lake, "events") == [(1, "a"), (2, "b"), (2, "b2"), (3, "c")]
        assert _rows(lake, "dims") == [(1, "a"), (2, "b2"), (3, "c")]
        assert _rows(lake, "snap") == [(2, "b2"), (3, "c")]

    def test_unspecified_streams_use_the_global_setting(self, lake: Lake):
        """A top-level append covers every stream without an override."""
        for records in (FIRST, SECOND):
            _run(
                lake,
                {"events": records, "dims": records},
                keys=["id"],
                load_method="append",
                stream_overrides={"dims": {"write_mode": "upsert"}},
            )
        assert _rows(lake, "events") == [(1, "a"), (2, "b"), (2, "b2"), (3, "c")]
        assert _rows(lake, "dims") == [(1, "a"), (2, "b2"), (3, "c")]

    def test_keys_for_a_tap_that_declares_none(self, lake: Lake):
        """Configured keys make a keyless stream merge instead of append."""
        config: dict[str, Any] = {"stream_overrides": {"t": {"key_properties": ["id"]}}}
        for records in (FIRST, SECOND):
            _run(lake, {"t": records}, keys=None, **config)
        assert _rows(lake, "t") == [(1, "a"), (2, "b2"), (3, "c")]

    def test_keys_replace_the_taps(self, lake: Lake):
        """The tap's key (v) is replaced: rows merge on id."""
        config: dict[str, Any] = {"stream_defaults": {"key_properties": ["id"]}}
        for records in (FIRST, SECOND):
            _run(lake, {"t": records}, keys=["v"], **config)
        assert _rows(lake, "t") == [(1, "a"), (2, "b2"), (3, "c")]

    def test_configured_keys_beat_overwrite_if_no_pk(self, lake: Lake):
        """overwrite_if_no_pk applies to streams without keys after the setting."""
        config: dict[str, Any] = {
            "overwrite_if_no_pk": True,
            "stream_overrides": {"t": {"key_properties": ["id"]}},
        }
        for records in (FIRST, SECOND):
            _run(lake, {"t": records}, keys=None, **config)
        assert _rows(lake, "t") == [(1, "a"), (2, "b2"), (3, "c")]

    @pytest.mark.parametrize(
        "layout",
        [{"load_method": "append"}, {"write_mode": "append-only"}],
        ids=["load-method", "write-mode"],
    )
    def test_a_stream_load_method_beats_overwrite_if_no_pk(
        self, lake: Lake, layout: dict
    ):
        """A keyless stream set to append keeps appending; others overwrite."""
        config: dict[str, Any] = {
            "overwrite_if_no_pk": True,
            "stream_overrides": {"events": layout},
        }
        for records in (FIRST, SECOND):
            _run(lake, {"events": records, "snap": records}, keys=None, **config)
        assert _rows(lake, "events") == [(1, "a"), (2, "b"), (2, "b2"), (3, "c")]
        assert _rows(lake, "snap") == [(2, "b2"), (3, "c")]

    def test_configured_keys_serve_deletes(self, lake: Lake):
        """delete_rows_with_deleted_at works on a keyless tap given keys."""
        config: dict[str, Any] = {
            "delete_rows_with_deleted_at": True,
            "stream_overrides": {"t": {"key_properties": ["id"]}},
        }
        _run(lake, {"t": FIRST}, keys=None, **config)
        deleted = [{"id": 1, "_sdc_deleted_at": "2024-01-01T00:00:00Z"}]
        _run(lake, {"t": deleted}, keys=None, **config)
        assert _rows(lake, "t") == [(2, "b")]

    def test_a_key_the_stream_lacks_fails_the_stream(self, lake: Lake):
        """Named, with the stream's properties."""
        config: dict[str, Any] = {
            "stream_overrides": {"t": {"key_properties": ["id", "nope"]}}
        }
        with pytest.raises(LayoutError, match=r"key_properties \['nope'\] are not"):
            _run(lake, {"t": FIRST}, keys=["id"], **config)
