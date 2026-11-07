"""Load provenance on DuckLake snapshots (DUCKLKD-90).

Each transaction that writes a table's rows sets DuckLake's commit message
(``CALL <catalog>.set_commit_message``), so the catalog's snapshot history
says which run, stream and load method wrote each snapshot, with how many
rows. ``snapshots()`` shows it as ``author``, ``commit_message`` and
``commit_extra_info``; the extra info is a JSON object with these keys:

- ``run_id``: the run that committed it (``DuckLakeConnector.tag_commits``).
  writeCompleted events count a table's snapshots by it.
- ``stream``: the stream the rows came from (a stream map's alias, when one
  renames the stream).
- ``load_method``: how the stream loads the table: ``append``, ``merge`` or
  ``overwrite`` (the method applied, so a merge without keys is an append).
- ``rows``: the staged rows the commit loaded, rows marked deleted included
  (with ``delete_rows_with_deleted_at``, those delete rows instead of loading).
  Left out when the commit loads none of its own (``operation`` says why) or
  when they couldn't be counted.
- ``operation``: only on commits that do more, or other, than load rows:
  ``full-refresh`` when an overwrite replaces the table's rows,
  ``activate-version`` when ACTIVATE_VERSION retires the rows of other table
  versions, and ``flush-inlined-data`` when inlined rows move to Parquet.
  A flush writes all of the table's inlined rows; its ``stream`` is the
  stream whose end triggered it.

The key names are kept once written: renaming one would orphan the
snapshots that already carry it.
"""

from __future__ import annotations

import json
from typing import Any, NamedTuple

# The commit's operation, when it isn't a plain load of rows.
FULL_REFRESH = "full-refresh"
ACTIVATE_VERSION = "activate-version"
FLUSH_INLINED_DATA = "flush-inlined-data"


class CommitProvenance(NamedTuple):
    """What one data commit is: its stream, load method, rows and operation.

    Attributes:
        stream: The stream whose rows the commit writes.
        load_method: The stream's applied load method.
        rows: The staged rows the commit loads; None when it loads none of
            its own, or they couldn't be counted.
        operation: ``FULL_REFRESH``, ``ACTIVATE_VERSION`` or
            ``FLUSH_INLINED_DATA``; None for a plain append or merge.
    """

    stream: str
    load_method: str
    rows: int | None = None
    operation: str | None = None

    def extra_info(self, run_id: str | None) -> str:
        """The commit's extra info: a compact JSON object, unset keys left out."""
        info: dict[str, Any] = {
            "run_id": run_id,
            "stream": self.stream,
            "load_method": self.load_method,
            "rows": self.rows,
            "operation": self.operation,
        }
        return json.dumps(
            {key: value for key, value in info.items() if value is not None},
            separators=(",", ":"),
        )

    def message(self) -> str:
        """The commit message: one line for a person reading the history.

        For example ``orders: merge, 120 rows`` or
        ``orders: overwrite (full-refresh), 3 rows``.
        """
        operation = f" ({self.operation})" if self.operation else ""
        rows = ""
        if self.rows is not None:
            rows = f", {self.rows} row{'' if self.rows == 1 else 's'}"
        return f"{self.stream}: {self.load_method}{operation}{rows}"
