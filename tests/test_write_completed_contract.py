"""target-ducklake's writeCompleted events against Zamboni's published contract.

Zamboni owns the writeCompleted event (docs/write-completed.md). tests/contracts
holds a pinned copy of its JSON Schema (bin/refresh-write-completed-contract.sh),
and these tests validate events from a real run against it, and check that the
copy is the contract version write_completed.py states. A refreshed copy that
moves a field, a type or the version fails here, naming what changed.
"""

from __future__ import annotations

import io
import json
from pathlib import Path
from typing import TYPE_CHECKING, Any

import jsonschema
import pytest

from target_ducklake import target
from target_ducklake.target import Targetducklake
from target_ducklake.write_completed import CONTRACT_VERSION, DATASCHEMA

if TYPE_CHECKING:
    from tests.catalogs import Lake

SPEC_PATH = Path(__file__).parent / "contracts" / "zamboni-write-completed_spec.json"


@pytest.fixture(scope="module")
def spec() -> dict[str, Any]:
    """The vendored spec."""
    return json.loads(SPEC_PATH.read_text(encoding="utf-8"))


@pytest.fixture
def events(lake: Lake, monkeypatch: pytest.MonkeyPatch) -> list[dict[str, Any]]:
    """The events of a real run: a merged table, and an overwritten one."""
    captured: list[dict[str, Any]] = []

    def capture(batch: list[dict], *_args: object, **_kwargs: object) -> bool:
        captured.extend(batch)
        return True

    monkeypatch.setattr(target, "publish", capture)
    schema = {"type": "object", "properties": {"id": {"type": "integer"}}}
    messages = [
        {"type": "SCHEMA", "stream": "m", "schema": schema, "key_properties": ["id"]},
        {"type": "RECORD", "stream": "m", "record": {"id": 1}},
        {"type": "SCHEMA", "stream": "o", "schema": schema},
        {"type": "RECORD", "stream": "o", "record": {"id": 1}},
    ]
    loader = Targetducklake(
        config={
            **lake.config(),
            "nats": {"servers": "nats://127.0.0.1:1", "subject": "x"},
            "stream_overrides": {"o": {"load_method": "overwrite"}},
        }
    )
    try:
        loader.listen(
            file_input=io.StringIO("".join(json.dumps(m) + "\n" for m in messages))
        )
    finally:
        loader.target_connector.close()
    assert len(captured) == 2  # noqa: PLR2004
    return captured


def test_the_vendored_spec_is_the_version_this_target_writes(spec: dict[str, Any]):
    """CONTRACT_VERSION, and the dataschema URL that states it."""
    assert spec["x-contract-version"] == CONTRACT_VERSION
    assert spec["$id"] == DATASCHEMA


def test_events_validate_against_the_spec(
    spec: dict[str, Any], events: list[dict[str, Any]]
):
    """Every required field, with the spec's types and formats."""
    validator = jsonschema.Draft202012Validator(
        spec, format_checker=jsonschema.Draft202012Validator.FORMAT_CHECKER
    )
    for event in events:
        validator.validate(event)


def test_every_field_is_in_the_contract(
    spec: dict[str, Any], events: list[dict[str, Any]]
):
    """The payload allows unknown fields, so an invented one would pass validation."""
    for event in events:
        assert set(event) <= set(spec["properties"])
        assert set(event["data"]) <= set(spec["properties"]["data"]["properties"])
