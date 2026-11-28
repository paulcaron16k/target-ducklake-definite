"""Date-times the singer-sdk doesn't parse, on RECORD messages (DUCKLKD-165).

The singer-sdk parses a top-level date-time property only. A STRUCT field, a
LIST element and a flattened column arrive as text, which DuckDB casts; text
in an ISO 8601 form DuckDB refuses but Python's ``datetime.fromisoformat``
reads is written as the top-level value would be, so every column takes the
same forms. BATCH files are cast by DuckDB and are not covered here.
"""

from __future__ import annotations

import io
import json
from datetime import datetime
from typing import TYPE_CHECKING, Any

import pytest

from target_ducklake.record_lines import datetime_text
from target_ducklake.target import Targetducklake
from tests.test_record_lines import DATE_TIMES

if TYPE_CHECKING:
    from tests.catalogs import Lake

# The forms DuckDB refuses, from the issue.
PYTHON_ONLY = [
    "20240101T100000Z",
    "2024-W01-1T10:00:00Z",
    "2024-01-01T10:00:00,5Z",
    "2024-01-01T10:00Z",
    "2024-01-01T10:00+02:00",
    "2024-01-01T10:00:00 +05:30",
]
# Each form's instant in UTC, as the lake shows a TIMESTAMPTZ.
PYTHON_ONLY_UTC = [
    "2024-01-01 10:00:00+00",
    "2024-01-01 10:00:00+00",
    "2024-01-01 10:00:00.5+00",
    "2024-01-01 10:00:00+00",
    "2024-01-01 08:00:00+00",
    "2024-01-01 04:30:00+00",
]
AT = {"type": ["null", "string"], "format": "date-time"}
ID = {"id": {"type": "integer"}}
CONFIGS = pytest.mark.parametrize(
    "config",
    [
        {},
        {"date_time_type": "timestamp"},
        {"date_time_type": "timestamp", "convert_tz_to_utc": True},
    ],
    ids=["timestamptz", "timestamp", "timestamp-utc"],
)


def _load(lake: Lake, properties: dict, records: list[dict], **config: object) -> None:
    messages: list[dict[str, Any]] = [
        {
            "type": "SCHEMA",
            "stream": "t",
            "schema": {"type": "object", "properties": properties},
            "key_properties": ["id"],
        },
        *({"type": "RECORD", "stream": "t", "record": r} for r in records),
    ]
    target = Targetducklake(config={**lake.config(), **config})
    try:
        target.listen(
            file_input=io.StringIO("".join(json.dumps(m) + "\n" for m in messages))
        )
    finally:
        target.target_connector.close()


def _shapes(form: str) -> dict[str, Any]:
    """A STRUCT field and a LIST element holding ``form``."""
    return {"o": {"at": form}, "l": [form, None]}


NESTED = {
    "o": {"type": ["null", "object"], "properties": {"at": AT}},
    "l": {"type": ["null", "array"], "items": AT},
}
NESTED_N = {"on": NESTED["o"], "ln": NESTED["l"]}


class TestNested:
    """A STRUCT field and a LIST element (``nested_types: native``)."""

    @CONFIGS
    def test_each_form_loads_as_python_reads_it(self, lake: Lake, config: dict) -> None:
        """As the text of Python's parse loads, which DuckDB reads.

        ``on``/``ln`` hold ``datetime_text`` of Python's parse, the text a
        top-level column's line holds for a value the singer-sdk parsed.
        """
        _load(
            lake,
            {**ID, **NESTED, **NESTED_N},
            [
                {
                    "id": i,
                    **_shapes(form),
                    **{
                        f"{k}n": v
                        for k, v in _shapes(
                            datetime_text(datetime.fromisoformat(form))
                        ).items()
                    },
                }
                for i, form in enumerate(DATE_TIMES)
            ],
            **config,
        )
        rows = lake.query(
            "SELECT id, CAST(o AS VARCHAR), CAST(l AS VARCHAR), "
            'CAST("on" AS VARCHAR), CAST(ln AS VARCHAR) FROM lake.s.t ORDER BY id'
        )
        assert len(rows) == len(DATE_TIMES)
        assert [(i, o, items) for i, o, items, _, _ in rows] == [
            (i, o, items) for i, _, _, o, items in rows
        ]

    def test_the_issue_forms_are_their_instants(self, lake: Lake) -> None:
        """Each form DuckDB refuses, into a TIMESTAMPTZ field, is its UTC instant."""
        _load(
            lake,
            {**ID, **NESTED},
            [{"id": i, **_shapes(form)} for i, form in enumerate(PYTHON_ONLY)],
        )
        rows = lake.query(
            "SELECT CAST(o.at AS VARCHAR), CAST(l[1] AS VARCHAR) FROM lake.s.t "
            "ORDER BY id"
        )
        assert rows == [(utc, utc) for utc in PYTHON_ONLY_UTC]

    @pytest.mark.parametrize("value", ["2024-13-01T00:00:00Z", "yesterday"])
    def test_text_neither_reads_fails(self, lake: Lake, value: str) -> None:
        """Left to DuckDB, which refuses it, naming the field."""
        with pytest.raises(ValueError, match=r"o\.at"):
            _load(lake, {**ID, **NESTED}, [{"id": 1, "o": {"at": value}}])


class TestFlattened:
    """A column flattened from an object (``flatten_max_level``)."""

    @CONFIGS
    def test_each_form_loads_as_python_reads_it(self, lake: Lake, config: dict) -> None:
        """As the text of Python's parse loads."""
        obj = {"type": ["null", "object"], "properties": {"at": AT, "n": AT}}
        _load(
            lake,
            {**ID, "o": obj},
            [
                {
                    "id": i,
                    "o": {
                        "at": form,
                        "n": datetime_text(datetime.fromisoformat(form)),
                    },
                }
                for i, form in enumerate(DATE_TIMES)
            ],
            flatten_max_level=1,
            **config,
        )
        rows = lake.query(
            "SELECT CAST(o__at AS VARCHAR), CAST(o__n AS VARCHAR) FROM lake.s.t "
            "ORDER BY id"
        )
        assert len(rows) == len(DATE_TIMES)
        assert [at for at, _ in rows] == [n for _, n in rows]

    def test_the_issue_forms_are_their_instants(self, lake: Lake) -> None:
        """Each form DuckDB refuses is its UTC instant."""
        obj = {"type": ["null", "object"], "properties": {"at": AT}}
        _load(
            lake,
            {**ID, "o": obj},
            [{"id": i, "o": {"at": form}} for i, form in enumerate(PYTHON_ONLY)],
            flatten_max_level=1,
        )
        rows = lake.query("SELECT CAST(o__at AS VARCHAR) FROM lake.s.t ORDER BY id")
        assert rows == [(utc,) for utc in PYTHON_ONLY_UTC]

    def test_text_neither_reads_fails(self, lake: Lake) -> None:
        """Left to DuckDB, which refuses it, naming the column."""
        obj = {"type": ["null", "object"], "properties": {"at": AT}}
        with pytest.raises(ValueError, match="o__at"):
            _load(
                lake,
                {**ID, "o": obj},
                [{"id": 1, "o": {"at": "yesterday"}}],
                flatten_max_level=1,
            )
