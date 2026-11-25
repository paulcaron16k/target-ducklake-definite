"""Staging rows as a batch of records, as the sink does at drain (DUCKLKD-157).

For tests of what one column's values stage as: the rows are written as JSON
lines (``record_lines``) and staged by ``batch_files.stage_records``.
"""

from __future__ import annotations

import logging
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

import pyarrow as pa
import pyarrow.parquet as pq

from target_ducklake.batch_files import (
    BatchColumn,
    FoundValues,
    StagingContext,
    stage_records,
)
from target_ducklake.record_lines import LineColumn, RecordLines, value_rules

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence


@dataclass
class Staged:
    """A staged batch: its rows, the values its casts changed, and the warnings."""

    table: pa.Table
    found: list[FoundValues] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    def column(self, name: str) -> list:
        """The column's staged values, as Python values."""
        return self.table.column(name).to_pylist()

    @property
    def changes(self) -> str:
        """What the findings say, one per line: column, change and values."""
        return "\n".join(
            f"column {each.finding.column!r} {each.finding.change} "
            f"({each.count} value(s)): {each.sample}"
            for each in self.found
        )


def stage_rows(  # noqa: PLR0913 (the rows and their columns, then the rules)
    rows: Iterable[dict],
    columns: Sequence[tuple[str, str]],
    *,
    strict: bool = False,
    keys: Iterable[str] = (),
    utc: Iterable[str] = (),
    convert_tz: Iterable[str] = (),
    containers: Iterable[str] = (),
) -> Staged:
    """Stage ``rows`` (values as the singer-sdk parses them) at ``columns``.

    Args:
        rows: The records, each a top-level column's value by its name.
        columns: Each column's name and DuckDB type.
        strict: Fail on a value a cast changes (``value_type_checking``),
            with a ValueError saying what changed.
        keys: The key columns, whose last row of each key is kept.
        utc: The columns and nested leaf paths whose date-times stage as
            UTC text (``StagingContext.utc_paths``).
        convert_tz: Those whose date-times with an offset do
            (``convert_tz_to_utc``).
        containers: The columns whose property is declared an object or
            array, whose values are JSON text.

    Raises:
        ValueError: For a value the staging refuses, or under ``strict``.
    """
    declared = set(containers)
    staged = Staged(pa.table({}))
    logger = logging.Logger("tests.staging")  # noqa: LOG001 (this batch's own)
    logger.addHandler(_Recorded(staged.warnings))

    def report(found: list[FoundValues]) -> None:
        staged.found += found
        if strict:
            raise ValueError(staged.changes)

    with tempfile.TemporaryDirectory() as directory:
        lines = RecordLines(
            str(Path(directory) / "records.jsonl"),
            value_rules(
                LineColumn((name,), name, column_type, name in declared)
                for name, column_type in columns
            ),
            stream="t",
            strict=strict,
            logger=logger,
        )
        for row in rows:
            lines.write(dict(row))
        lines.close()
        path = Path(directory) / "staged.parquet"
        stage_records(
            lines.path,
            [
                BatchColumn(name, column_type, path=(name,))
                for name, column_type in columns
            ],
            str(path),
            stream="t",
            context=StagingContext(
                keys=frozenset(k.lower() for k in keys),
                utc_paths=frozenset(p.lower() for p in utc),
                convert_tz_paths=frozenset(p.lower() for p in convert_tz),
                record_lines=True,
            ),
            settings={},
            on_findings=report,
        )
        staged.table = pq.read_table(path)
    return staged


class _Recorded(logging.Handler):
    """Keeps each message logged, as text."""

    def __init__(self, messages: list[str]) -> None:
        super().__init__()
        self.messages = messages

    def emit(self, record: logging.LogRecord) -> None:
        self.messages.append(record.getMessage())
