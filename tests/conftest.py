"""Shared test fixtures."""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

import pytest

from target_ducklake.connector import DuckLakeConnector
from tests.catalogs import POSTGRES_ENV, Lake

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path


@pytest.fixture(autouse=True)
def _no_table_options_unless_marked(
    request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Read every table's inlining limit as 0, unless the test is marked.

    Setup reads each table's ``data_inlining_row_limit`` from DuckLake's
    ``options()`` (DUCKLKD-39). Many tests stand a plain DuckDB database or
    mocks in for the catalog, which have no options. Tests of inlining are
    marked ``ducklake_options`` and run the real query on a real catalog.
    """
    if request.node.get_closest_marker("ducklake_options") is None:
        monkeypatch.setattr(DuckLakeConnector, "table_inlining_limit", lambda *_args: 0)


@pytest.fixture(params=["sqlite", "postgres"])
def lake(request: pytest.FixtureRequest, tmp_path: Path) -> Iterator[Lake]:
    """Each catalog test on SQLite, and on Postgres when POSTGRES_ENV is set.

    A Postgres test's metadata schema is dropped afterwards.
    """
    if request.param == "postgres" and not os.environ.get(POSTGRES_ENV):
        pytest.skip(f"set {POSTGRES_ENV} to a libpq conninfo to test on Postgres")
    created = Lake(request.param, tmp_path)
    yield created
    created.drop()
