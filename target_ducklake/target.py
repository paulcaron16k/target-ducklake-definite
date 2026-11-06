"""ducklake target class."""

from __future__ import annotations

import ast
import contextlib
import json
import logging
import re
import uuid
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING, cast
from urllib.parse import urlsplit

from singer_sdk import typing as th
from singer_sdk.helpers._classproperty import classproperty
from singer_sdk.helpers.capabilities import CapabilitiesEnum, PluginCapabilities
from singer_sdk.target_base import SQLTarget

from target_ducklake.batch_files import batch_encoding, batch_records
from target_ducklake.layout import (
    DATE_TIME_TYPES,
    NESTED_TYPES,
    LayoutError,
    check_layout_config,
)
from target_ducklake.nats_publish import (
    JETSTREAM_MODES,
    check_nats_config,
    publish,
    redact,
)
from target_ducklake.parquet_utils import PERMISSIVE, VALUE_TYPE_CHECKING_MODES
from target_ducklake.sinks import (
    BATCH_SIZE_DEFAULT,
    BATCH_SIZE_KEY,
    LEGACY_BATCH_SIZE_KEY,
    batch_size,
    ducklakeSink,
)
from target_ducklake.type_source import check_type_source_config
from target_ducklake.write_completed import WriteLedger, build_event

if TYPE_CHECKING:
    from collections.abc import Iterable

    from singer_sdk.sinks import Sink

    from target_ducklake.connector import DuckLakeConnector


# One stream's table layout (target-iceberg's shape). Values are checked by
# layout.resolve_layout, which names the fix; the schema stays permissive so a
# truncate transform or an unknown key gets that message, not a schema error.
_LAYOUT_SCHEMA: dict = {
    "type": "object",
    "properties": {
        "partition": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "column": {"type": "string"},
                    "transform": {"type": "string"},
                    "num_buckets": {"type": "integer"},
                    "width": {"type": "integer"},
                    "name": {"type": "string"},
                },
                "required": ["column"],
            },
        },
        "sort_order": {"type": "array", "items": {"type": "string"}},
        "load_method": {"type": ["string", "null"]},
        "write_mode": {"type": ["string", "null"]},
        "key_properties": {"type": ["array", "null"], "items": {"type": "string"}},
        "type_source": {"type": ["object", "null"]},
        "on_error": {"type": ["string", "null"]},
    },
}


def _parse_layout_settings(config: dict) -> None:
    """Parse table layout settings given as JSON strings (env vars), in place."""
    for key in ("stream_defaults", "stream_overrides"):
        if isinstance(config.get(key), str):
            try:
                config[key] = json.loads(config[key])
            except json.JSONDecodeError as error:
                msg = f"{key} is not valid JSON: {error}"
                raise LayoutError(msg) from None


class Targetducklake(SQLTarget):
    name = "target-ducklake"

    @classproperty
    def capabilities(self) -> list[CapabilitiesEnum]:
        """The SQL target's capabilities, and BATCH messages (DUCKLKD-40)."""
        return [*SQLTarget.capabilities, PluginCapabilities.BATCH]

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.max_parallelism = 10 if self._drains_in_parallel() else 1
        # Fail at startup on a malformed layout for any stream, including one
        # whose records never arrive; columns are checked per stream later.
        check_layout_config(self.config, self.logger)
        check_type_source_config(self.config, self.logger)
        self._check_batch_size()
        self._MAX_RECORD_AGE_IN_MINUTES = float(
            self.config.get("max_record_age_minutes", 10.0)
        )
        # writeCompleted events (DUCKLKD-86): on with nats.servers and
        # nats.subject. The sinks report their commits to the ledger, and the
        # connector tags them as this run's.
        self._nats = check_nats_config(self.config, self.logger)
        self.write_ledger = WriteLedger() if self._nats is not None else None
        if self._nats is not None:
            self._connector().tag_commits(uuid.uuid4().hex)
            if not self.config.get("warehouse"):
                self.logger.info(
                    "warehouse is not set: writeCompleted events name the "
                    "warehouse by data_path, %s.",
                    self._warehouse(),
                )

    def _connector(self) -> DuckLakeConnector:
        """The target's connector, as a DuckLakeConnector."""
        return cast("DuckLakeConnector", self.target_connector)

    def _warehouse(self) -> str:
        """The warehouse writeCompleted events name: the setting, else data_path.

        DuckLake has no warehouse name; its data path names the lake.
        """
        return self.config.get("warehouse") or self.config["data_path"].rstrip("/")

    def _secrets(self) -> list[str]:
        """Every credential the config holds, for redacting warnings.

        The settings the config schema marks secret, the NATS credentials,
        and the password inside the catalog URL (a libpq ``password=`` or a
        URL's userinfo), which an error message may echo on its own.
        """
        properties = self.config_jsonschema["properties"]
        found = [
            str(value)
            for key, value in self.config.items()
            if value and properties.get(key, {}).get("secret")
        ]
        if self._nats is not None:
            found += self._nats.secrets()
        catalog_url = str(self.config.get("catalog_url") or "")
        found += re.findall(r"password\s*=\s*'?([^'\s]+)", catalog_url)
        if "://" in catalog_url:
            found.append(urlsplit(catalog_url).password or "")
        return [secret for secret in found if secret]

    def process_endofpipe(self) -> None:
        """Drain and clean up every sink, then publish writeCompleted events.

        The singer-sdk calls this once the whole input is read, and a failed
        commit raises out of the drain, so events are only published for a
        run that completed: one per table, after its last commit (DUCKLKD-86).
        """
        super().process_endofpipe()
        if self._nats is not None:
            publish(self.write_completed_events(), self._nats, self.logger)

    def write_completed_events(self) -> list[dict]:
        """The writeCompleted event of each table the run committed to.

        Advisory: a table whose snapshots can't be read gets no event, with a
        warning (redacted), and the run still succeeds.
        """
        if self.write_ledger is None:
            return []
        connector = self._connector()
        events = []
        for write in self.write_ledger.tables():
            try:
                snapshots = connector.table_snapshots(write.schema, write.table)
            except Exception as error:  # noqa: BLE001 (advisory: never fails the run)
                self.logger.warning(
                    "No writeCompleted event for %s.%s: reading its snapshots "
                    "failed (%s). The run's data is committed and unaffected.",
                    write.schema,
                    write.table,
                    redact(f"{type(error).__name__}: {error}", self._secrets()),
                )
                continue
            if snapshots is None:
                # Its commits changed nothing (an empty table emptied again),
                # or it was dropped since: nothing to report.
                self.logger.info(
                    "No writeCompleted event for %s.%s: the run committed no "
                    "snapshot of it.",
                    write.schema,
                    write.table,
                )
                continue
            events.append(build_event(write, snapshots, warehouse=self._warehouse()))
        return events

    def _check_batch_size(self) -> None:
        """Check the batch size settings, and report the deprecated one (DUCKLKD-92).

        ``batch_size_rows`` is the singer-sdk's setting, which target-iceberg
        reads. ``max_batch_size``, this target's older one, applies only
        without it; see ``batch_size``.

        Raises:
            ValueError: If either is below 1.
        """
        rows = self.config.get(BATCH_SIZE_KEY)
        legacy = self.config.get(LEGACY_BATCH_SIZE_KEY)
        for key, value in ((BATCH_SIZE_KEY, rows), (LEGACY_BATCH_SIZE_KEY, legacy)):
            if value is not None and value < 1:
                msg = f"{key} must be at least 1, not {value}."
                raise ValueError(msg)
        if legacy is None:
            return
        if rows is None:
            self.logger.warning(
                "max_batch_size is deprecated: set batch_size_rows, the "
                "singer-sdk's setting (as in target-iceberg), instead. Using %d.",
                legacy,
            )
        elif rows != legacy:
            self.logger.warning(
                "batch_size_rows (%d) and the deprecated max_batch_size (%d) are "
                "both set: using batch_size_rows.",
                rows,
                legacy,
            )

    def _process_batch_message(self, message_dict: dict) -> None:
        """Load a BATCH message's files as the RECORD messages they hold.

        The singer-sdk's own handling gives a file's records to the sink's
        ``process_batch`` as they are, skipping stream maps, metadata columns,
        validation and flattening (a flattened column loaded NULL), and reads
        each file whole. Here each record takes the RECORD path instead, so
        both kinds of message load alike, and a sink drains whenever it is
        full while a file is read, so memory stays bounded by the batch size.
        Files are opened from the URLs in the manifest, with
        ``batch_config.storage.params`` (fsspec options such as credentials)
        when set. The message's ``version`` and ``time_extracted``, if any, go
        with each record, as a RECORD message's do.
        """
        self._assert_line_requires(
            message_dict, requires={"stream", "encoding", "manifest"}
        )
        stream_name = message_dict["stream"]
        # As for a RECORD message; a stream map may alias the stream's sink.
        if stream_name not in self.mapper.stream_maps:
            self._assert_sink_exists(stream_name)
        encoding = batch_encoding(message_dict["encoding"])
        storage = (self.config.get("batch_config") or {}).get("storage") or {}
        chunk_rows = batch_size(self.config)
        # A RECORD message's other keys, which _sdc metadata reads.
        extra = {
            key: message_dict[key]
            for key in ("version", "time_extracted")
            if key in message_dict
        }
        for url in message_dict["manifest"]:
            records = batch_records(
                url,
                encoding,
                storage_params=storage.get("params"),
                chunk_rows=chunk_rows,
            )
            # Closed, with its file, if a record fails mid-file.
            with contextlib.closing(records):
                for record in records:
                    self._process_record_message(
                        {
                            "type": "RECORD",
                            "stream": stream_name,
                            "record": record,
                            **extra,
                        }
                    )

    def _drains_in_parallel(self) -> bool:
        """Whether sinks are drained in parallel: parallel_draining, except on SQLite.

        A SQLite catalog allows one writer at a time, so two sinks committing
        at once fail with 'database is locked'. Its sinks are always drained
        one at a time.
        """
        parallel = bool(self.config.get("parallel_draining", True))
        if parallel and self.config.get("catalog_type") == "sqlite":
            self.logger.info(
                "catalog_type is sqlite, which allows one writer at a time: "
                "sinks are drained one at a time (parallel_draining is ignored)."
            )
            return False
        return parallel

    def _drain_all(self, sink_list: Iterable[Sink], parallelism: int) -> None:
        """Drain sinks in parallel threads, and wait for all of them before raising.

        The SDK's parallel drain raises the first failure without waiting for
        the other drains, which keep running in background threads. The
        interpreter then shuts down under them: a thread that returns from
        native code (here Polars' ``from_arrow``) during shutdown is ended by
        CPython with ``pthread_exit``, which can't unwind through Rust or C++
        frames, so the process aborted (exit 134) instead of failing with the
        error. Here every drain finishes first; each failure is logged and the
        first one is raised, so the run exits with it. Sinks that drain
        successfully commit their batch, as on the serial path. Nothing
        changes when drains succeed.
        """
        if parallelism == 1:
            super()._drain_all(sink_list, parallelism)
            return
        with ThreadPoolExecutor(max_workers=parallelism) as pool:
            futures = [pool.submit(self.drain_one, sink) for sink in sink_list]
        # Leaving the block waited for every drain.
        errors = [error for future in futures if (error := future.exception())]
        for error in errors[1:]:
            self.logger.error("Another sink also failed to drain: %s", error)
        if errors:
            raise errors[0]

    def _write_state_message(self, state: dict) -> None:
        """Emit STATE, unless an overwrite hasn't committed its replace yet.

        A mid-stream drain (a full batch, or max_record_age_minutes) only
        stages an overwrite's batches; its rows reach the table when the
        stream ends. STATE emitted before that would let a resumable tap skip
        rows that were never committed if the run then fails. So STATE is held
        until every overwrite has committed. The SDK writes the latest STATE
        again after the final drain and clean-up.
        """
        sinks = [*self._sinks_to_clear, *self._sinks_active.values()]
        pending = [
            sink.stream_name
            for sink in sinks
            if getattr(sink, "has_uncommitted_overwrite", False)
        ]
        if pending:
            self.logger.info(
                "Holding STATE until the overwrite of %s commits.", ", ".join(pending)
            )
            return
        super()._write_state_message(state)

    def _process_activate_version_message(self, message_dict: dict) -> None:
        """Drain the stream's sink, then let it activate the version.

        Draining first writes any buffered records of the version, so rows
        the sink is about to keep are in the table before rows of other
        versions are deleted. The SDK neither drains nor checks whether the
        version can be honoured; the sink decides (``activate_version``).
        """
        stream_name = message_dict["stream"]
        for stream_map in self.mapper.stream_maps[stream_name]:
            sink = self._sinks_active.get(stream_map.stream_alias)
            if sink is None:
                self.logger.info(
                    "ACTIVATE_VERSION for %s before its SCHEMA; ignored.", stream_name
                )
                continue
            if not sink.process_activate_version_messages:
                self.logger.info(
                    "process_activate_version_messages is off; ignoring "
                    "ACTIVATE_VERSION for %s.",
                    stream_name,
                )
                continue
            self.drain_one(sink)
            sink.activate_version(message_dict["version"])

    @property
    def config(self):
        """Get config dict with JSON string parsing for complex types."""
        config = dict(super().config)

        # Convert certain config values from strings to their respective types
        # This is required for certain config values that are passed in via env vars into Kubernetes pods
        # Kubernetes pods only support string values for env vars (at least in GCP)
        if "partition_fields" in config and isinstance(config["partition_fields"], str):
            partition_fields_str = config["partition_fields"]
            parsed_partition_fields = ast.literal_eval(partition_fields_str)
            config["partition_fields"] = parsed_partition_fields
            logging.info(
                f"Successfully parsed partition_fields from string to object: {parsed_partition_fields}"
            )
        _parse_layout_settings(config)
        if config.get(LEGACY_BATCH_SIZE_KEY) is not None:
            try:
                config[LEGACY_BATCH_SIZE_KEY] = int(config[LEGACY_BATCH_SIZE_KEY])
            except ValueError:
                msg = (
                    f"{LEGACY_BATCH_SIZE_KEY} must be a whole number, not "
                    f"{config[LEGACY_BATCH_SIZE_KEY]!r}."
                )
                raise ValueError(msg) from None
        if "max_record_age_minutes" in config:
            config["max_record_age_minutes"] = float(config["max_record_age_minutes"])
        if "flatten_max_level" in config:
            config["flatten_max_level"] = int(config.get("flatten_max_level", 0))
        if "max_column_length" in config:
            config["max_column_length"] = int(config.get("max_column_length", 63))
        if "validate_records" in config and isinstance(config["validate_records"], str):
            config["validate_records"] = config["validate_records"].lower() == "true"
        if "overwrite_if_no_pk" in config and isinstance(
            config["overwrite_if_no_pk"], str
        ):
            config["overwrite_if_no_pk"] = (
                config["overwrite_if_no_pk"].lower() == "true"
            )
        if "parallel_draining" in config and isinstance(
            config["parallel_draining"], str
        ):
            config["parallel_draining"] = (
                config["parallel_draining"].lower() == "true"
            )
        if "auto_cast_timestamps" in config and isinstance(
            config["auto_cast_timestamps"], str
        ):
            config["auto_cast_timestamps"] = (
                config["auto_cast_timestamps"].lower() == "true"
            )
        return config

    config_jsonschema = th.PropertiesList(
        th.Property(
            "catalog_url",
            th.StringType(nullable=False),
            secret=True,  # Flag config as protected.
            title="Catalog URL",
            description=(
                "Connection string for the catalog database, as DuckLake's ATTACH "
                "takes it: libpq keywords (host=... dbname=... user=... "
                "password=...) for postgres and mysql, a file path for sqlite and "
                "duckdb. Secret, since it can hold the catalog password."
            ),
        ),
        th.Property(
            "catalog_type",
            th.StringType(
                allowed_values=["postgres", "sqlite", "mysql", "duckdb"],
                nullable=False,
            ),
            default="postgres",
            title="Catalog Type",
            description="Type of catalog database: postgres, sqlite, mysql, or duckdb",
        ),
            th.Property(
                "meta_schema",
                th.StringType(nullable=True),
                title="Meta Schema",
                description="Schema name in the catalog database to use for DuckLake metadata tables",
            ),
        th.Property(
            "data_path",
            th.StringType(nullable=False),
            title="Data Path",
            description="GCS, S3, or local folder path for data storage",
        ),
        th.Property(
            "storage_type",
            th.StringType(
                allowed_values=["GCS", "S3", "local"],
                nullable=False,
            ),
            default="local",
            title="Storage Type",
            description="Type of storage: GCS, S3, or local",
        ),
        th.Property(
            "public_key",
            th.StringType(nullable=True),
            secret=True,  # An access key id: half of a credential.
            title="Public Key",
            description=(
                "Key id for GCS or S3: the S3 access key id, or the GCS HMAC "
                "key id. Set with secret_key, or neither to use the host's own "
                "credentials (instance role, service account)."
            ),
        ),
        th.Property(
            "secret_key",
            th.StringType(nullable=True),
            secret=True,  # Flag config as protected.
            title="Secret Key",
            description=(
                "Secret for public_key: the S3 secret access key, or the GCS "
                "HMAC secret."
            ),
        ),
        th.Property(
            "region",
            th.StringType(nullable=True),
            title="Region",
            description="AWS region for S3 storage type (required when using S3 with explicit credentials)",
        ),
        th.Property(
            "default_target_schema",
            th.StringType(nullable=True),
            title="Default Target Schema Name",
            description=(
                "Default database schema where data should be written. "
                "If not provided schema will attempt to be derived from the stream name "
                "(e.g. database taps provide schema name in the stream name)."
            ),
        ),
        th.Property(
            "target_schema_prefix",
            th.StringType(nullable=True),
            title="Target Schema Prefix",
            description=(
                "Prefix to add to the target schema name. "
                "If not provided, no prefix will be added. "
                "May be useful if target schema name is inferred from the stream name "
                "and you want to add a prefix to the schema name."
            ),
        ),
        th.Property(
            "add_record_metadata",
            th.BooleanType(),
            default=False,
            title="Add Singer Record Metadata",
            description=(
                "When True, automatically adds Singer Data Capture (SDC) metadata columns to target tables: "
            ),
        ),
        th.Property(
            "load_method",
            th.StringType(allowed_values=["append", "merge", "overwrite"]),
            title="Load Method",
            description="Method to use for loading data into the target table: append, merge, or overwrite. An overwrite stages every batch and replaces the table's rows in one transaction at the end of the stream. Default merge; a stream without key properties appends.",
        ),
        th.Property(
            "flatten_max_level",
            th.CustomType(
                {"oneOf": [{"type": "string"}, {"type": "integer", "minimum": 0}]}
            ),
            default=0,
            title="Flattening Max Level",
            description="Maximum depth for flattening nested fields. Set to 0 to disable flattening.",
        ),
        th.Property(
            "temp_file_dir",
            th.StringType(),
            default="temp_files/",
            title="Temporary File Directory",
            description="Directory path for storing temporary parquet files",
        ),
        th.Property(
            "max_batch_size",
            th.CustomType(
                {
                    "oneOf": [
                        {
                            "type": "string",
                            "description": "String representation of max batch size",
                        },
                        {"type": "integer", "minimum": 1},
                    ]
                }
            ),
            title="Max Batch Size",
            description=(
                "Deprecated: use batch_size_rows. Records per batch, used only "
                "when batch_size_rows is not set."
            ),
        ),
        th.Property(
            BATCH_SIZE_KEY,
            # A plain integer, as in the singer-sdk and target-iceberg: the
            # SDK types TARGET_DUCKLAKE_BATCH_SIZE_ROWS from it, and can't
            # from a oneOf.
            th.CustomType({"type": "integer", "minimum": 1}),
            title="Batch Size Rows",
            description=(
                "Records per batch: each batch is staged as one Parquet file and "
                f"loaded in one transaction. Default {BATCH_SIZE_DEFAULT}. The "
                "singer-sdk's setting, as in target-iceberg; wins over the "
                "deprecated max_batch_size."
            ),
        ),
        th.Property(
            "partition_fields",
            th.CustomType(
                {
                    "oneOf": [
                        {
                            "type": "string",
                            "description": "JSON string representation of partition fields object",
                        },
                        {
                            "type": "object",
                            "additionalProperties": {
                                "type": "array",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        "column_name": {"type": "string"},
                                        "type": {
                                            "type": "string",
                                            "enum": ["timestamp", "identifier"],
                                        },
                                        "granularity": {
                                            "type": "array",
                                            "items": {
                                                "type": "string",
                                                "enum": [
                                                    "year",
                                                    "month",
                                                    "day",
                                                    "hour",
                                                ],
                                            },
                                        },
                                    },
                                    "required": ["column_name", "type"],
                                },
                            },
                        },
                        {"type": "null"},
                    ]
                }
            ),
            nullable=True,
            title="Partition Fields",
            description=(
                "Object mapping stream names to arrays of partition column definitions. "
                "Each stream key maps directly to an array of column definitions. "
                "Can be provided as a JSON string or object."
            ),
        ),
        th.Property(
            "stream_defaults",
            th.CustomType(
                {"oneOf": [{"type": "string"}, _LAYOUT_SCHEMA, {"type": "null"}]}
            ),
            nullable=True,
            title="Stream Defaults",
            description=(
                "Table layout for every stream, as in target-iceberg: partition "
                "([{column, transform, num_buckets}], transforms identity, year, "
                "month, day, hour, bucket) and sort_order (column names, optionally "
                "with ASC/DESC and NULLS FIRST/LAST). Also load_method (or "
                "target-iceberg's write_mode), key_properties, and type_source "
                "(column -> type, for an all-string tap) with on_error (raise, "
                "null or string), among others. Can be a JSON string."
            ),
        ),
        th.Property(
            "stream_overrides",
            th.CustomType(
                {
                    "oneOf": [
                        {"type": "string"},
                        {"type": "object", "additionalProperties": _LAYOUT_SCHEMA},
                        {"type": "null"},
                    ]
                }
            ),
            nullable=True,
            title="Stream Overrides",
            description=(
                "Per-stream table layout, keyed by stream name, merged over "
                "stream_defaults key by key; same shape. partition replaces "
                "partition_fields for that stream. Can be a JSON string."
            ),
        ),
        th.Property(
            "table_config",
            th.StringType,
            title="Table Config Path",
            description=(
                "Path to a Zamboni table-config.json (version 2), the file "
                "target-iceberg reads. Its per-table partition, and an ordering in "
                "sort mode, set the table layout; Iceberg-only and maintenance "
                "sections are ignored. A stream's own partition and sort_order win."
            ),
        ),
        th.Property(
            "max_column_length",
            th.CustomType(
                {"oneOf": [{"type": "string"}, {"type": "integer", "minimum": 0}]}
            ),
            default=63,
            title="Max Column Name Length",
            description=(
                "Maximum length for column names. Only applied when catalog_type is 'postgres'. "
                "Names exceeding this limit are truncated to avoid PostgreSQL's 63-character "
                "identifier limit (NAMEDATALEN). Colliding truncated names get a _{i} suffix. "
                "Set to 0 to disable truncation."
            ),
        ),
        th.Property(
            "auto_cast_timestamps",
            th.CustomType(
                {
                    "oneOf": [
                        {
                            "type": "string",
                            "description": "String representation of auto cast timestamps flag",
                        },
                        {"type": "boolean"},
                    ]
                }
            ),
            default=False,
            title="Auto Cast Timestamps",
            description=(
                "When True, automatically attempts to cast timestamp-like fields to timestamp types in ducklake."
            ),
        ),
        th.Property(
            "convert_tz_to_utc",
            th.BooleanType(),
            default=False,
            title="Convert TZ to UTC",
            description=(
                "When True, converts timezone-aware date-time values to UTC before "
                "loading, so a timestamp column without time zone stores the UTC "
                "time. When False, such a column stores each value's local "
                "wall-clock time (its offset is dropped). A timestamptz column "
                "stores the correct instant either way."
            ),
        ),
        th.Property(
            "validate_records",
            th.CustomType(
                {
                    "oneOf": [
                        {
                            "type": "string",
                            "description": "String representation of validate records",
                        },
                        {"type": "boolean"},
                    ]
                }
            ),
            default=False,
            title="Validate Records",
            description="Whether to validate the schema of the incoming streams.",
        ),
        th.Property(
            "value_type_checking",
            th.StringType(allowed_values=list(VALUE_TYPE_CHECKING_MODES)),
            default=PERMISSIVE,
            title="Value Type Checking",
            description=(
                "What to do with a number its column would round: more fraction "
                "digits than a DECIMAL column holds, or a fraction for an integer "
                "column. permissive (default) rounds it as DuckDB's cast of a DECIMAL"
                " does, half away from zero, and warns how many values in each column"
                " were rounded, saying so when the column is a key (records whose "
                "keys round alike are merged). strict fails the batch, naming the "
                "column and the value: use it while onboarding a source. A value a "
                "column can't hold at all, such as one with too many integer digits, "
                "fails either way, as DuckDB's cast would."
            ),
        ),
        th.Property(
            "date_time_type",
            th.StringType(allowed_values=list(DATE_TIME_TYPES)),
            default=DATE_TIME_TYPES[0],
            title="Date-Time Type",
            description=(
                "The column type of a format date-time property without an "
                "x-sql-datatype hint. timestamptz (default) loads instants, as "
                "target-iceberg does: a value with an offset is stored as its UTC "
                "instant, and a value without one is read as UTC. timestamp loads "
                "wall-clock times, as before (see convert_tz_to_utc). Applies to new "
                "tables and new columns; an existing TIMESTAMP column keeps its "
                "type. Can be set per stream in stream_defaults and stream_overrides."
            ),
        ),
        th.Property(
            "nested_types",
            th.StringType(allowed_values=list(NESTED_TYPES)),
            default=NESTED_TYPES[0],
            title="Nested Types",
            description=(
                "How an object with declared properties and an array with typed "
                "items are loaded. native (default) creates STRUCT and LIST "
                "columns, as target-iceberg does, each leaf typed as a top-level "
                "property would be; json stores them as JSON text, as before. "
                "Objects without properties, arrays without items, anyOf/oneOf "
                "and multi-type properties are JSON either way. Applies to new "
                "tables and new columns; an existing JSON column stays JSON. Can "
                "be set per stream in stream_defaults and stream_overrides."
            ),
        ),
        th.Property(
            "delete_rows_with_deleted_at",
            th.BooleanType(),
            default=False,
            title="Delete Rows With _sdc_deleted_at",
            description=(
                "When True, a record whose _sdc_deleted_at is set (a log-based "
                "CDC tap's delete) deletes its key's row from the table, in the "
                "same transaction as its batch, instead of being loaded as a live "
                "row. Needs key properties, except with load_method overwrite. "
                "Unlike hard_delete, which only decides what ACTIVATE_VERSION does. "
                "Can be set per stream in stream_defaults and stream_overrides."
            ),
        ),
        th.Property(
            "conform_column_names",
            th.BooleanType(),
            default=False,
            title="Conform Column Names",
            description=(
                "When True, column names are conformed as the singer-sdk's SQL "
                "targets and target-iceberg conform them: lowercased, '.', '-' "
                "and spaces made '_', characters other than ASCII letters, digits "
                "and '_' dropped, and a leading digit made a letter. Two names "
                "that conform to the same column fail the stream. Key properties, "
                "partition and sort columns may name either form. Off by default: "
                "for an existing table, a renamed key column fails the stream "
                "(a merge would load every row again) and other renamed columns "
                "are added beside the old ones (see the README). Can be set per "
                "stream in stream_defaults and stream_overrides."
            ),
        ),
        th.Property(
            "data_inlining_row_limit",
            th.IntegerType(),
            default=0,
            title="Data Inlining Row Limit",
            description=(
                "An insert of at most this many rows is stored in the catalog "
                "(DuckLake data inlining) instead of a new Parquet file, so small "
                "incremental runs don't create many tiny files. 0 (default) turns "
                "inlining off. Kept off for a table with a STRUCT or LIST column, "
                "which DuckLake 1.5.2 can't always read back inlined. Set on each "
                "table as a DuckLake option. Can be set per stream in "
                "stream_defaults and stream_overrides."
            ),
        ),
        th.Property(
            "inlined_data_flush_rows",
            th.IntegerType(),
            default=10000,
            title="Inlined Data Flush Rows",
            description=(
                "With data inlining on, a stream ends by writing its table's "
                "inlined rows to Parquet (ducklake_flush_inlined_data) once they "
                "reach this many. 0 never flushes, leaving it to maintenance. "
                "Can be set per stream in stream_defaults and stream_overrides."
            ),
        ),
        th.Property(
            "overwrite_if_no_pk",
            th.CustomType(
                {
                    "oneOf": [
                        {
                            "type": "string",
                            "description": "String representation of overwrite if no primary key, overrides load_method.",
                        },
                        {"type": "boolean"},
                    ]
                }
            ),
            default=False,
            title="Overwrite If No Primary Key",
            description="When True, a stream without primary keys replaces the target table's rows (as load_method overwrite does, in one transaction at the end of the stream). Overrides load_method, except append set in the stream's own stream_overrides entry.",
        ),
        th.Property(
            "parallel_draining",
            th.CustomType(
                {
                    "oneOf": [
                        {
                            "type": "string",
                            "description": "String representation of parallel draining flag",
                        },
                        {"type": "boolean"},
                    ]
                }
            ),
            default=True,
            title="Parallel Draining",
            description=(
                "When True, enables parallel draining with max_parallelism=10 "
                "(Meltano default). When False, sets max_parallelism=1 to "
                "disable parallel draining. Ignored for a sqlite catalog, "
                "which allows one writer at a time: its sinks are always "
                "drained one at a time."
            ),
        ),
        th.Property(
            "max_record_age_minutes",
            th.CustomType(
                {
                    "oneOf": [
                        {
                            "type": "string",
                            "description": "String representation of max record age in minutes",
                        },
                        {"type": "number", "exclusiveMinimum": 0},
                    ]
                }
            ),
            default=10.0,
            title="Max Record Age (Minutes)",
            description=(
                "Maximum age in minutes that records can sit in a sink before a drain "
                "is forced. Larger values batch more aggressively; smaller values flush "
                "more often."
            ),
        ),
        th.Property(
            "warehouse",
            th.StringType,
            title="Warehouse",
            description=(
                "The warehouse name writeCompleted events carry, which a table "
                "maintainer matches on (target-iceberg's setting of that name). "
                "Defaults to data_path, without a trailing slash."
            ),
        ),
        th.Property(
            "nats",
            th.ObjectType(
                th.Property(
                    "servers",
                    th.StringType,
                    title="NATS Servers",
                    description=(
                        "NATS server URLs, comma-separated (nats://host:4222). With "
                        "nats.subject, turns writeCompleted events on."
                    ),
                ),
                th.Property(
                    "subject",
                    th.StringType,
                    title="NATS Subject",
                    description=(
                        "The subject writeCompleted events are published on. With "
                        "nats.servers, turns them on."
                    ),
                ),
                th.Property(
                    "jetstream",
                    th.StringType,
                    allowed_values=list(JETSTREAM_MODES),
                    default="auto",
                    title="NATS JetStream",
                    description=(
                        "auto publishes through JetStream when a stream covers the "
                        "subject (deduplicated on the event id), else over core NATS "
                        "(at most once); true or false forces one."
                    ),
                ),
                th.Property(
                    "username",
                    th.StringType,
                    title="NATS Username",
                    description="For user/password authentication.",
                ),
                th.Property(
                    "password",
                    th.StringType,
                    secret=True,
                    title="NATS Password",
                    description="For user/password authentication.",
                ),
                th.Property(
                    "token",
                    th.StringType,
                    secret=True,
                    title="NATS Token",
                    description="Token authentication; wins over user/password.",
                ),
                th.Property(
                    "creds_file",
                    th.StringType,
                    title="NATS Credentials File",
                    description=(
                        "Path to a NATS .creds file; wins over token and user/password."
                    ),
                ),
                th.Property(
                    "tls",
                    th.BooleanType,
                    default=False,
                    title="NATS TLS",
                    description=(
                        "Connect over TLS only, trusting the system's CAs; a server "
                        "that doesn't require TLS is refused, never used in plain text."
                    ),
                ),
            ),
            title="writeCompleted Events (NATS)",
            description=(
                "Publish one writeCompleted event (Zamboni's contract, as "
                "target-iceberg does) per table at the end of a completed run. "
                "Off unless nats.servers and nats.subject are set; needs the "
                "nats extra. A failure to publish is a warning."
            ),
        ),
    ).to_dict()

    default_sink_class = ducklakeSink


# The singer-sdk adds its own settings (batch_config, stream_maps, hard_delete,
# ...) to config_jsonschema in place, but only once a target is built or
# --about runs. Adding them here, once, makes the class attribute the whole
# settings surface from import, which spec.json and the tests read.
Targetducklake.append_builtin_config(Targetducklake.config_jsonschema)


if __name__ == "__main__":
    Targetducklake.cli()
