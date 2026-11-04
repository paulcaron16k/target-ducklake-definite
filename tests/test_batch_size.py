"""Batch size: the singer-sdk's batch_size_rows, and max_batch_size (DUCKLKD-92)."""

from __future__ import annotations

import io
import json
import logging
from typing import TYPE_CHECKING

import pytest
from singer_sdk.exceptions import ConfigValidationError

from target_ducklake.sinks import batch_size
from target_ducklake.target import Targetducklake

if TYPE_CHECKING:
    from collections.abc import Iterator

    from tests.catalogs import Lake

LIST_FILES = "SELECT data_file FROM ducklake_list_files('lake', 't', schema => 's')"


@pytest.fixture
def warnings(caplog: pytest.LogCaptureFixture) -> Iterator[pytest.LogCaptureFixture]:
    """The target's warnings, caught on its own logger.

    The singer-sdk configures logging when a target starts, which replaces
    the root handler caplog listens on.
    """
    logger = logging.getLogger("target-ducklake")
    logger.addHandler(caplog.handler)
    caplog.handler.setLevel(logging.WARNING)
    try:
        yield caplog
    finally:
        logger.removeHandler(caplog.handler)


def _target(lake: Lake, **config: object) -> Targetducklake:
    return Targetducklake(config={**lake.config(), **config})


def _run(lake: Lake, records: int, **config: object) -> None:
    """Append ``records`` rows to s.t in one run."""
    messages = [
        {
            "type": "SCHEMA",
            "stream": "t",
            "schema": {"type": "object", "properties": {"id": {"type": "integer"}}},
            "key_properties": [],
        },
        *(
            {"type": "RECORD", "stream": "t", "record": {"id": i}}
            for i in range(records)
        ),
    ]
    target = _target(lake, load_method="append", **config)
    try:
        target.listen(
            file_input=io.StringIO("".join(json.dumps(m) + "\n" for m in messages))
        )
    finally:
        target.target_connector.close()


class TestPrecedence:
    """batch_size_rows, else max_batch_size, else 10000."""

    @pytest.mark.parametrize(
        ("config", "expected"),
        [
            ({}, 10000),
            ({"max_batch_size": 500}, 500),
            ({"batch_size_rows": 700}, 700),
            ({"batch_size_rows": 700, "max_batch_size": 500}, 700),
            ({"batch_size_rows": None, "max_batch_size": 500}, 500),
        ],
        ids=["default", "max-batch-size", "batch-size-rows", "both", "null"],
    )
    def test_batch_size(self, config: dict, expected: int):
        """The SDK's setting wins; the older one still works alone."""
        assert batch_size(config) == expected

    def test_a_max_batch_size_string_is_read_as_a_number(self, lake: Lake):
        """As it always was, for configs written as strings."""
        target = _target(lake, max_batch_size="250")
        assert batch_size(target.config) == 250  # noqa: PLR2004

    def test_a_max_batch_size_that_is_not_a_number_names_the_key(self, lake: Lake):
        """Not a bare int() error."""
        with pytest.raises(ValueError, match="max_batch_size must be a whole number"):
            _target(lake, max_batch_size="1.5")

    def test_both_set_and_equal(self, lake: Lake, warnings: pytest.LogCaptureFixture):
        """The same size either way, and nothing to warn about but the name."""
        target = _target(lake, batch_size_rows=300, max_batch_size="300")
        assert batch_size(target.config) == 300  # noqa: PLR2004
        assert "both set" not in warnings.text


class TestStartup:
    """The checks and messages when the target starts."""

    def test_max_batch_size_alone_is_reported_deprecated(
        self, lake: Lake, warnings: pytest.LogCaptureFixture
    ):
        """It still applies, with a warning naming batch_size_rows."""
        _target(lake, max_batch_size=500)
        assert "max_batch_size is deprecated: set batch_size_rows" in warnings.text

    def test_both_differing_says_which_applies(
        self, lake: Lake, warnings: pytest.LogCaptureFixture
    ):
        """batch_size_rows wins, and the warning says so."""
        _target(lake, batch_size_rows=700, max_batch_size=500)
        assert "both set: using batch_size_rows" in warnings.text

    def test_batch_size_rows_alone_is_quiet(
        self, lake: Lake, warnings: pytest.LogCaptureFixture
    ):
        """No warning for the standard setting, or for the default."""
        _target(lake, batch_size_rows=700)
        _target(lake)
        assert "batch_size_rows" not in warnings.text
        assert "max_batch_size" not in warnings.text

    @pytest.mark.parametrize(
        ("key", "value"),
        [("max_batch_size", "0"), ("max_batch_size", "-1")],
    )
    def test_below_one_is_refused(self, lake: Lake, key: str, value: object):
        """A batch needs at least one record; a string is checked here."""
        with pytest.raises(ValueError, match=f"{key} must be at least 1"):
            _target(lake, **{key: value})

    @pytest.mark.parametrize("key", ["batch_size_rows", "max_batch_size"])
    def test_a_number_below_one_fails_the_config_schema(self, lake: Lake, key: str):
        """As an integer, the schema's minimum refuses it."""
        with pytest.raises(ConfigValidationError, match=key):
            _target(lake, **{key: 0})


class TestThroughListen:
    """Batches, as Parquet files written, on a real catalog."""

    @pytest.mark.parametrize(
        ("config", "files"),
        [
            ({"batch_size_rows": 2}, 3),
            ({"max_batch_size": 2}, 3),
            ({"batch_size_rows": 5, "max_batch_size": 2}, 1),
            ({}, 1),
        ],
        ids=["batch-size-rows", "max-batch-size", "both", "default"],
    )
    def test_each_setting_sizes_the_batches(self, lake: Lake, config: dict, files: int):
        """Five records: three batches of at most two, or one of five."""
        _run(lake, 5, **config)
        assert len(lake.query(LIST_FILES)) == files
        assert lake.query("SELECT count(*) FROM lake.s.t") == [(5,)]


class TestEnvironment:
    """Settings the singer-sdk reads from TARGET_DUCKLAKE_* variables."""

    def test_batch_size_rows_from_the_environment(
        self, lake: Lake, monkeypatch: pytest.MonkeyPatch
    ):
        """The SDK types env values from the schema, so it must be an integer."""
        monkeypatch.setenv("TARGET_DUCKLAKE_BATCH_SIZE_ROWS", "3")
        target = Targetducklake(config=lake.config(), parse_env_config=True)
        assert batch_size(target.config) == 3  # noqa: PLR2004
