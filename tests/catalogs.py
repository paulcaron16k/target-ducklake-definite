"""DuckLake catalogs for tests that load through the target: SQLite or Postgres."""

from __future__ import annotations

import os
import uuid
from typing import TYPE_CHECKING

import duckdb

if TYPE_CHECKING:
    from pathlib import Path

# A libpq conninfo for a Postgres database to run the catalog tests on as well
# (the Postgres retest of DUCKLKD-85, 108 and 109). Each test gets a fresh
# metadata schema. Unset, those tests run on SQLite only.
POSTGRES_ENV = "TARGET_DUCKLAKE_TEST_POSTGRES"


class Lake:
    """A DuckLake catalog for one test: SQLite in tmp_path, or a Postgres schema."""

    def __init__(self, kind: str, tmp_path: Path) -> None:
        """Create the catalog's place: a fresh metadata schema on Postgres."""
        self.kind = kind
        self.tmp_path = tmp_path
        self.data_path = f"{tmp_path / 'data'}/"
        if kind == "postgres":
            self.conninfo = os.environ[POSTGRES_ENV]
            self.meta_schema = f"ducklkd_{uuid.uuid4().hex[:12]}"
            con = duckdb.connect()
            con.execute("INSTALL postgres; LOAD postgres;")
            con.execute(f"ATTACH '{self.conninfo}' AS pg (TYPE postgres)")
            con.execute(
                f"CALL postgres_execute('pg', 'CREATE SCHEMA {self.meta_schema}')"
            )
            con.close()

    def drop(self) -> None:
        """Drop the Postgres metadata schema, and the catalog with it."""
        if self.kind != "postgres":
            return
        con = duckdb.connect()
        con.execute("LOAD postgres;")
        con.execute(f"ATTACH '{self.conninfo}' AS pg (TYPE postgres)")
        con.execute(
            f"CALL postgres_execute('pg', 'DROP SCHEMA {self.meta_schema} CASCADE')"
        )
        con.close()

    def config(self) -> dict:
        """The target's catalog and storage settings."""
        catalog: dict[str, object]
        if self.kind == "postgres":
            catalog = {
                "catalog_type": "postgres",
                "catalog_url": self.conninfo,
                "meta_schema": self.meta_schema,
            }
        else:
            catalog = {
                "catalog_type": "sqlite",
                "catalog_url": str(self.tmp_path / "catalog.sqlite"),
            }
        return {
            **catalog,
            "data_path": self.data_path,
            "storage_type": "local",
            "default_target_schema": "s",
            "temp_file_dir": str(self.tmp_path / "staging") + "/",
        }

    def attach_sql(self) -> str:
        """ATTACH the catalog as ``lake``, without inlining (DUCKLKD-110)."""
        if self.kind == "postgres":
            return (
                f"LOAD postgres; ATTACH 'ducklake:postgres:{self.conninfo}' AS lake "
                f"(DATA_PATH '{self.data_path}', DATA_INLINING_ROW_LIMIT 0, "
                f"META_SCHEMA '{self.meta_schema}')"
            )
        return (
            f"ATTACH 'ducklake:sqlite:{self.tmp_path / 'catalog.sqlite'}' AS lake "
            f"(DATA_PATH '{self.data_path}', DATA_INLINING_ROW_LIMIT 0)"
        )

    def query(self, sql: str) -> list[tuple]:
        """Run a query on the catalog, in UTC."""
        con = duckdb.connect()
        con.execute("SET TimeZone = 'UTC'")
        con.execute(self.attach_sql())
        rows = con.execute(sql).fetchall()
        con.close()
        return rows
