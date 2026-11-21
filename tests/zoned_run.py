"""Runs the target in this process, whose time zone the caller set with ``TZ``.

DuckDB reads the process's time zone once, when it is loaded, and makes it
every new database's default ``TimeZone``; setting ``TZ`` in a running test
process doesn't change it. So ``tests/test_session_time_zone.py`` starts this
module in a child process with ``TZ`` set:

    python -m tests.zoned_run < payload.json

The payload is ``{"runs": [{"config": {...}, "messages": [...]}, ...]}``;
each run is one target invocation, in order. The last line written to stdout
is a JSON object: ``default_zone``, the zone a new DuckDB database gets in
this process, and ``session_zones``, the zone each run's target session had.
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


def main() -> None:
    """Run each payload run, then report the zones."""
    payload = json.load(sys.stdin)
    default_zone = duckdb.connect().sql("SELECT current_setting('TimeZone')")
    report = {"default_zone": default_zone.fetchone()[0], "session_zones": []}
    for run in payload["runs"]:
        target = Targetducklake(config=run["config"])
        lines = "".join(json.dumps(message) + "\n" for message in run["messages"])
        try:
            target.listen(file_input=io.StringIO(lines))
            report["session_zones"].append(_session_zone(target))
        finally:
            target.target_connector.close()
    sys.stdout.write("\n" + json.dumps(report) + "\n")


if __name__ == "__main__":
    main()
