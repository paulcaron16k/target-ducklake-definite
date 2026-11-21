"""Runs the target in this process, whose time zone the caller set with ``TZ``.

DuckDB reads the process's time zone once, when it is loaded, and makes it
every new database's default ``TimeZone``; setting ``TZ`` in a running test
process doesn't change it. So ``tests/test_session_time_zone.py`` starts this
module in a child process with ``TZ`` set:

    python -m tests.zoned_run < payload.json

The payload is ``{"runs": [{"config": {...}, "messages": [...]}, ...]}``;
each run is one target invocation, in order. A run that fails is reported,
and the next one runs. The last line written to stdout is a JSON object:

- ``default_zone``: the zone a new DuckDB database gets in this process;
- ``runs``: for each run, ``{"zone": ...}``, its target session's zone, or
  ``{"error": "Type: message"}``;
- ``reconnected``: for the first run's config, the session zone after the
  target closed its connection and a query opened a new one, and after its
  catalog was detached and attached again (the path of a locked SQLite
  catalog, DUCKLKD-82), and whether a query then found the catalog.
"""

from __future__ import annotations

import io
import json
import sys

import duckdb

from target_ducklake.target import Targetducklake


def _session_zone(target: Targetducklake) -> str:
    """The time zone of a query the target runs, as DuckDB reports it."""
    query = "SELECT current_setting('TimeZone')"
    return target.target_connector.execute(query).fetchone()[0]


def _reconnected(config: dict) -> dict:
    """The session zone after a reconnect, and after the catalog is re-attached."""
    target = Targetducklake(config=config)
    connector = target.target_connector
    try:
        _session_zone(target)
        connector.close()
        zones = [_session_zone(target)]
        connector._detach_catalog(duckdb.Error("database is locked"))  # noqa: SLF001
        # The next query attaches the catalog again first.
        zones.append(_session_zone(target))
        schemata = connector.execute(
            "SELECT count(*) FROM information_schema.schemata WHERE catalog_name = ?",
            [connector.catalog_name],
        ).fetchone()[0]
    finally:
        connector.close()
    return {"zones": zones, "attached": schemata > 0}


def _run(run: dict) -> dict:
    """Load one run's messages; its session's zone, or the error it raised."""
    target = Targetducklake(config=run["config"])
    lines = "".join(json.dumps(message) + "\n" for message in run["messages"])
    try:
        target.listen(file_input=io.StringIO(lines))
        return {"zone": _session_zone(target)}
    except Exception as error:  # noqa: BLE001 - reported, for its own test
        return {"error": f"{type(error).__name__}: {error}"}
    finally:
        target.target_connector.close()


def main() -> None:
    """Run each payload run, then report the zones."""
    payload = json.load(sys.stdin)
    default_zone = duckdb.connect().sql("SELECT current_setting('TimeZone')")
    report = {
        "default_zone": default_zone.fetchone()[0],
        "runs": [_run(run) for run in payload["runs"]],
        "reconnected": _reconnected(payload["runs"][0]["config"]),
    }
    sys.stdout.write("\n" + json.dumps(report) + "\n")


if __name__ == "__main__":
    main()
