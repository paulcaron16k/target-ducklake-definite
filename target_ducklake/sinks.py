"""ducklake target sink class, which handles writing streams."""

from __future__ import annotations

import contextlib
import copy
import os
import shutil
import uuid
from collections.abc import Sequence
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

import duckdb
import polars as pl
import psutil
import pyarrow as pa
import pyarrow.parquet as pq
from singer_sdk import Target
from singer_sdk.sinks import SQLSink
from singer_sdk.sinks.core import JSONSchemaValidator

from target_ducklake.conform import (
    conform_name,
    conform_names,
    describe_renames,
    fold_case,
    is_lossy,
)
from target_ducklake.connector import (
    DELETED_AT_COLUMN,
    SINGER_DECIMAL_FORMAT,
    VERSION_COLUMN,
    DuckLakeConnector,
    DuckLakeConnectorError,
)
from target_ducklake.flatten import (
    flatten_record,
    flatten_schema,
    nullable_choices,
    truncate_column_names,
)
from target_ducklake.layout import (
    LayoutError,
    conformed_name_hint,
    overrides_append,
    resolve_conform_column_names,
    resolve_date_time_type,
    resolve_delete_marked,
    resolve_inlining,
    resolve_key_properties,
    resolve_layout,
    resolve_load_method,
    resolve_nested_types,
)
from target_ducklake.parquet_utils import (
    STRICT,
    StagingRules,
    concat_tables,
    is_nested_type,
    nested_leaf_paths,
    staging_schema,
    staging_type,
)
from target_ducklake.type_hints import declares_base64_binary
from target_ducklake.type_source import (
    Coercer,
    TypeSource,
    declared_schema,
    resolve_type_source,
)

if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping

    from target_ducklake.write_completed import WriteLedger

# Records per batch (DUCKLKD-92): the singer-sdk's batch_size_rows, as in
# target-iceberg, else this target's older max_batch_size, else the default.
BATCH_SIZE_KEY = "batch_size_rows"
LEGACY_BATCH_SIZE_KEY = "max_batch_size"
BATCH_SIZE_DEFAULT = 10000


def batch_size(config: Mapping[str, Any]) -> int:
    """Records per batch: ``batch_size_rows``, else ``max_batch_size``, else 10000.

    The target checks both at startup and reports ``max_batch_size``, which is
    deprecated.
    """
    for key in (BATCH_SIZE_KEY, LEGACY_BATCH_SIZE_KEY):
        if config.get(key) is not None:
            return int(config[key])
    return BATCH_SIZE_DEFAULT


def _stream_keys(
    target: Target,
    stream_name: str,
    schema: dict,
    declared: Sequence[str] | None,
) -> Sequence[str] | None:
    """The stream's key properties: configured ones, else those its tap declares.

    ``key_properties`` in ``stream_overrides``/``stream_defaults``
    (DUCKLKD-41) replaces the tap's, for a tap that declares none or the
    wrong ones. They are taken before the sink exists, so everything keyed
    (merge, dedup, deletes) uses them. With ``conform_column_names`` on, a
    configured key may name a property by its conformed name too (a config
    shared with target-iceberg does); it is taken as the property's own
    name, which records carry.

    Raises:
        LayoutError: If a configured key isn't a property of the stream.
    """
    configured = resolve_key_properties(target.config, stream_name)
    if configured is None:
        return declared
    properties = _property_names(target, stream_name, schema)
    if missing := [key for key in configured if key not in properties]:
        msg = (
            f"stream {stream_name!r}: key_properties {missing} are not properties "
            f"of the stream (it has {sorted(set(properties.values()))})."
            f"{conformed_name_hint(missing, properties)}"
        )
        raise LayoutError(msg)
    if len({properties[key] for key in configured}) != len(configured):
        msg = (
            f"stream {stream_name!r}: key_properties {configured} name a property "
            "twice, by its own and its conformed name."
        )
        raise LayoutError(msg)
    configured = [properties[key] for key in configured]
    if list(declared or ()) != configured:
        target.logger.info(
            "Stream %r: key properties %s, as configured, instead of %s from the tap.",
            stream_name,
            configured,
            list(declared or ()),
        )
    return configured


def _property_names(target: Target, stream_name: str, schema: dict) -> dict[str, str]:
    """The names a config may give the stream's properties, mapped to theirs.

    A property's own name, and with ``conform_column_names`` on its
    conformed name too, as a config shared with target-iceberg names it.
    """
    properties = {name: name for name in schema.get("properties") or {}}
    if resolve_conform_column_names(target.config, stream_name):
        for name in list(properties):
            properties.setdefault(conform_name(name), name)
    return properties


def _stream_type_source(
    target: Target, stream_name: str, schema: dict
) -> TypeSource | None:
    """The stream's ``type_source``, its columns named by the stream's properties.

    With ``conform_column_names`` on, a column may be given by its conformed
    name too, as for ``key_properties``; names the stream doesn't have are
    kept, for the sink to report.

    Raises:
        LayoutError: If two entries name one property, by its own and its
            conformed name.
    """
    type_source = resolve_type_source(target.config, stream_name)
    if type_source is None:
        return None
    properties = _property_names(target, stream_name, schema)
    columns = {
        properties.get(name, name): kind for name, kind in type_source.columns.items()
    }
    if len(columns) != len(type_source.columns):
        msg = (
            f"stream {stream_name!r}: type_source {sorted(type_source.columns)} "
            "names a property twice, by its own and its conformed name."
        )
        raise LayoutError(msg)
    return TypeSource(columns, type_source.on_error)


def _is_timestamptz(column_type: str) -> bool:
    """Whether a DuckDB column type is TIMESTAMPTZ, by either of its names."""
    name = " ".join(column_type.upper().split())
    return name in {"TIMESTAMPTZ", "TIMESTAMP WITH TIME ZONE"}


def _is_fractional(staged: pa.DataType) -> bool:
    """Whether a staging type holds fractions: a decimal or a float."""
    return pa.types.is_decimal(staged) or pa.types.is_floating(staged)


class ducklakeSink(SQLSink):
    """ducklake target sink class."""

    connector_class = DuckLakeConnector

    # Set when the tap's own SCHEMA declares _sdc_deleted_at. A class-level
    # default: the SDK strips metadata columns inside Sink.__init__, before
    # this class's __init__ body runs.
    _tap_declares_deletions: bool = False

    def _remove_sdc_metadata_from_schema(self) -> None:
        """Strip the SDK's metadata columns, but keep a tap's ``_sdc_deleted_at``.

        Without ``add_record_metadata`` the SDK removes every ``_sdc_*``
        column. The others are its own bookkeeping, but a log-based CDC tap
        declares ``_sdc_deleted_at`` itself: it is how the tap reports a
        delete. Removing it lost every delete without a trace, so it is kept,
        as target-iceberg keeps it (DUCKLKD-23); ``delete_rows_with_deleted_at``
        then deletes the rows it marks.
        """
        properties = self.schema.get("properties") or {}
        declared = properties.get(DELETED_AT_COLUMN)
        super()._remove_sdc_metadata_from_schema()
        if declared is not None:
            properties[DELETED_AT_COLUMN] = declared
            self._tap_declares_deletions = True

    def _remove_sdc_metadata_from_record(self, record: dict) -> None:
        """Strip the SDK's metadata values, keeping a tap-declared ``_sdc_deleted_at``.

        A record without it is a live row: the value is None.
        """
        deleted_at = record.get(DELETED_AT_COLUMN)
        super()._remove_sdc_metadata_from_record(record)
        if self._tap_declares_deletions:
            record[DELETED_AT_COLUMN] = deleted_at

    def __init__(
        self,
        target: Target,
        stream_name: str,
        schema: dict,
        key_properties: Sequence[str] | None,
        connector: DuckLakeConnector | None = None,
    ) -> None:
        # type_source (DUCKLKD-43) declares its columns' types before the SDK
        # builds its validator from the schema, and before it is flattened.
        type_source = _stream_type_source(target, stream_name, schema)
        typed = schema if type_source is None else declared_schema(schema, type_source)
        super().__init__(
            target,
            stream_name,
            typed,
            _stream_keys(target, stream_name, schema, key_properties),
            connector=connector,
        )
        # The tap's own schema, which the SDK compares a repeated SCHEMA
        # message with to decide whether the stream needs a new sink.
        self.original_schema = copy.deepcopy(schema)
        self._coercer = self._type_source_coercer(type_source, schema)

        self.base_temp_file_dir = self.config.get("temp_file_dir", "temp_files/")
        self.temp_file_dir = os.path.join(
            self.base_temp_file_dir, f"{self.stream_name}_{uuid.uuid4()}"
        )
        self.files_saved = 0
        # Overwrite: each batch's parquet file, loaded together at clean_up.
        self._staged_files: list[str] = []
        # ACTIVATE_VERSION: the table versions of the records received, and
        # the versions already announced by an earlier ACTIVATE_VERSION.
        self._versions_received: set[int] = set()
        self._versions_announced: set[int] = set()
        self.convert_tz_to_utc = self.config.get("convert_tz_to_utc", False)

        # NOTE: we probably don't need all this logging, but useful for debugging while target is in development
        # Log original schema for debugging
        self.logger.info(f"Original schema for stream '{stream_name}': {self.schema}")

        # Create pyarrow and Ducklake schemas
        self.flatten_max_level = self.config.get("flatten_max_level", 0)
        self.auto_cast_timestamps = self.config.get("auto_cast_timestamps", False)
        # STRUCT and LIST columns for shaped objects and arrays, or JSON text.
        self.nested_types = resolve_nested_types(self.config, self.stream_name)
        self.flatten_schema = flatten_schema(
            self.schema,
            max_level=self.flatten_max_level,
            auto_cast_timestamps=self.auto_cast_timestamps,
            nested=self.nested_types == "native",
        )
        # Log flattened schema for debugging
        self.logger.info(
            f"Flattened schema for stream '{stream_name}': {self.flatten_schema}"
        )

        # Flattened names mapped to the column names they are loaded as, for
        # the names conformed or truncated (see _rename_columns).
        self._column_name_mapping: dict[str, str] = {}
        # Other names a config may use for a renamed column: conformed names
        # that were then truncated.
        self._column_aliases: dict[str, str] = {}
        # The columns whose names were truncated (see flatten_record).
        self._truncated_columns: frozenset[str] = frozenset()
        # Conformed columns mapped to the names they have with the setting off.
        self._unconformed_columns: dict[str, str] = {}
        self._rename_columns()

        # The column type of unhinted date-time properties, for this stream.
        self.date_time_type = resolve_date_time_type(self.config, self.stream_name)
        self.ducklake_schema = self.connector.json_to_ducklake_schema(
            self.flatten_schema, self.date_time_type, self.nested_types
        )
        # Staged at the columns' own types, so every value they hold stages.
        self.pyarrow_schema = staging_schema(self.ducklake_schema)
        # Log pyarrow and ducklake schemas for debugging
        self.logger.info(
            f"PyArrow schema for stream '{stream_name}': {self.pyarrow_schema}"
        )
        self.logger.info(
            f"DuckLake schema for stream '{stream_name}': {self.ducklake_schema}"
        )

        # Partition keys and sort order (stream_defaults / stream_overrides,
        # table_config, or the older partition_fields), checked against the
        # stream's columns.
        self.layout = resolve_layout(
            self.config,
            self.stream_name,
            [column["name"] for column in self.ducklake_schema],
            self.logger,
            # Only table_config needs the target table; deriving it raises for
            # a stream without a schema name, which may not be needed yet.
            table=(
                (self.target_schema, self.target_table)
                if self.config.get("table_config")
                else None
            ),
            # A shared config names columns by the tap's names or the
            # conformed ones, in full; Postgres catalogs truncate.
            aliases={**self._column_aliases, **self._column_name_mapping},
        )

        # stream_overrides, stream_defaults, the top-level setting, or merge.
        self.load_method = resolve_load_method(self.config, self.stream_name)

        # Determine if table should be overwritten. overwrite_if_no_pk
        # overrides load_method, except an append set in the stream's own
        # stream_overrides entry.
        if (
            not self.key_properties
            and self.config.get("overwrite_if_no_pk", False)
            and not overrides_append(self.config, self.stream_name)
        ):
            self.logger.info(
                f"Load method is overwrite for {self.stream_name}: no key properties and overwrite_if_no_pk is True"
            )
            self.should_overwrite_table = True
        elif self.load_method == "overwrite":
            self.logger.info(f"Load method is overwrite for {self.stream_name}")
            self.should_overwrite_table = True
        else:
            self.logger.info(
                f"Load method is {self.load_method} for {self.stream_name}"
            )
            self.should_overwrite_table = False
        self._overwrite_committed = False
        self.delete_marked = self._resolve_delete_marked()
        # The STRUCT and LIST columns, whose dicts and lists flatten_record
        # keeps; set in setup, once the table's own column types are known.
        self._native_columns: frozenset[str] = frozenset()
        # Data inlining (DUCKLKD-39): as configured, and as applied to the
        # table in setup (kept off for a table with nested columns).
        self.inlining = resolve_inlining(self.config, self.stream_name)
        self._inlining_limit = 0
        # The run's record of commits, for writeCompleted events (DUCKLKD-86);
        # None when they are off.
        self._write_ledger: WriteLedger | None = getattr(target, "write_ledger", None)

    def _type_source_coercer(
        self, type_source: TypeSource | None, schema: dict
    ) -> Coercer | None:
        """The stream's type_source converter, or None when nothing is typed.

        Typed columns the stream doesn't have are reported and left out: a
        type_source in stream_defaults names the columns of several streams.
        """
        if type_source is None:
            return None
        properties = schema.get("properties") or {}
        if absent := sorted(set(type_source.columns) - set(properties)):
            self.logger.warning(
                "Stream %r has no properties %s, which its type_source types.",
                self.stream_name,
                absent,
            )
        present = {
            name: declared
            for name, declared in type_source.columns.items()
            if name in properties
        }
        if not present:
            return None
        return Coercer(
            self.stream_name,
            TypeSource(present, type_source.on_error),
            strict=self.config.get("value_type_checking") == STRICT,
            logger=self.logger,
            # The records' own names, which the coercer sees.
            key_columns=frozenset(key.lower() for key in self._key_properties),
        )

    def _validate_and_parse(self, record: dict) -> dict:
        """Convert the type_source columns, then validate and parse as the SDK does.

        The SDK validates a record, and parses its date-times, before any
        other hook sees it, against the schema whose typed columns were
        declared their types; so values are converted here, in place.

        Raises:
            CoercionError: For a value that isn't its declared type, when the
                stream's on_error is raise.
        """
        if self._coercer is not None:
            self._coercer.coerce(record)
        return super()._validate_and_parse(record)

    def _resolve_delete_marked(self) -> bool:
        """Whether records with ``_sdc_deleted_at`` set delete their key's row.

        From ``delete_rows_with_deleted_at`` (DUCKLKD-23). A stream that
        doesn't have the column (the tap doesn't declare it and
        ``add_record_metadata`` is off) has nothing marked: it warns and
        loads as before. A stream without key properties can't say which row
        a delete is for, so it is refused, unless it overwrites the table,
        where a marked record is simply left out.

        Raises:
            ValueError: If the setting is on for a stream without key
                properties that doesn't overwrite.
        """
        if not resolve_delete_marked(self.config, self.stream_name):
            if self._tap_declares_deletions:
                self.logger.info(
                    "Stream %r declares %s: deleted records are loaded as rows "
                    "with it set, not deleted. Set delete_rows_with_deleted_at to "
                    "delete them.",
                    self.stream_name,
                    DELETED_AT_COLUMN,
                )
            return False
        if DELETED_AT_COLUMN not in self.flatten_schema:
            self.logger.warning(
                "delete_rows_with_deleted_at is on for stream %r, but it has no "
                "%s column (the tap doesn't declare it, and add_record_metadata "
                "is off), so no record is marked deleted.",
                self.stream_name,
                DELETED_AT_COLUMN,
            )
            return False
        if not self.key_properties and not self.should_overwrite_table:
            msg = (
                f"stream {self.stream_name!r}: delete_rows_with_deleted_at needs "
                "key properties to tell which row a deleted record is for. Declare "
                "key properties, or turn the setting off for this stream."
            )
            raise ValueError(msg)
        return True

    def get_validator(self) -> JSONSchemaValidator | None:
        """Validate records against the schema with anyOf/oneOf made nullable.

        See ``nullable_choices``. The SDK's default validates against
        ``self.schema`` itself, which stays exactly as the tap sent it.
        """
        if not self.validate_schema:
            return None
        return JSONSchemaValidator(
            nullable_choices(self.schema),
            validate_formats=self.validate_field_string_format,
        )

    @property
    def target_schema(self) -> str:
        """Get the target schema."""
        if self.config.get("default_target_schema"):
            # if default target schema is provided, use it
            self.logger.info(
                f"Using provided default target schema {self.config.get('default_target_schema')}"
            )
            return self.config.get("default_target_schema")  # type: ignore
        # if no default target schema is provided, try to derive it from the stream name
        # only works for database extractors (eg public-users becomes public.users)
        stream_name_dict = stream_name_to_dict(self.stream_name)
        if stream_name_dict.get("schema_name"):
            if self.config.get("target_schema_prefix"):
                target_schema = f"{self.config.get('target_schema_prefix')}_{stream_name_dict.get('schema_name')}"
            else:
                target_schema = stream_name_dict.get("schema_name")
            self.logger.info(
                f"Using derived target schema {target_schema} from stream name {self.stream_name}"
            )
            return target_schema
        raise ValueError(
            f"No schema name found for stream {self.stream_name} and no default target schema provided"
        )

    @property
    def target_table(self) -> str:
        """Get the target table."""
        stream_name_dict = stream_name_to_dict(self.stream_name)
        return stream_name_dict.get("table_name")

    @property
    def max_size(self) -> int:
        """Records to batch before ``is_full``: see ``batch_size``."""
        return batch_size(self.config)

    @property
    def key_properties(self) -> Sequence[str]:
        """The key columns: each key property by its column's name.

        The singer-sdk's ``SQLSink`` conforms its keys whether or not the
        columns are, so a key named ``Order ID`` was looked for as
        ``order_id`` in a table whose column is ``Order ID``. Here a key is
        renamed exactly as its column is (``_rename_columns``), and the
        records' own names stay in ``_key_properties``, which the singer-sdk
        checks every record against.
        """
        return [self._column_name_mapping.get(key, key) for key in self._key_properties]

    def _rename_columns(self) -> None:
        """Conform, then truncate, the flattened column names.

        Conformed with ``conform_column_names`` (DUCKLKD-87), as the
        singer-sdk's SQL targets and target-iceberg name columns; a name
        changed in a way that loses information is warned about. Truncated
        for a Postgres catalog, which caps identifiers at 63 characters
        (NAMEDATALEN) and otherwise causes DuckLake binder errors; see
        https://github.com/duckdb/ducklake/issues/619. Records are renamed
        to match by ``flatten_record``.

        Raises:
            ConformedNameClashException: If two names conform to one column.
            ValueError: If a name conforms to the empty name, or two columns
                end up with names DuckDB can't tell apart.
        """
        flattened = list(self.flatten_schema)
        conformed: dict[str, str] = {}
        if resolve_conform_column_names(self.config, self.stream_name):
            conformed = conform_names(self.flatten_schema)
            if lossy := {old: new for old, new in conformed.items() if is_lossy(old)}:
                self.logger.warning(
                    "Stream %r: conforming these column names dropped characters "
                    "outside ASCII letters, digits, '_', '-', '.' and spaces, or "
                    "made a leading digit a letter: %s",
                    self.stream_name,
                    describe_renames(lossy),
                )
            if conformed:
                self.logger.info(
                    "Stream %r: conformed %d column name(s): %s",
                    self.stream_name,
                    len(conformed),
                    describe_renames(conformed),
                )
            self.flatten_schema = {
                conformed.get(name, name): prop
                for name, prop in self.flatten_schema.items()
            }

        self.flatten_schema, truncated = self._truncate_column_names(
            self.flatten_schema
        )
        self._truncated_columns = frozenset(truncated.values())
        if truncated:
            self.logger.info(
                "Truncated %d column name(s) to max length %d: %s",
                len(truncated),
                self.config.get("max_column_length", 63),
                truncated,
            )

        # The names the columns had with the setting off, for the check of
        # an existing table in setup.
        _, unconformed = self._truncate_column_names({n: {} for n in flattened})
        for name in flattened:
            new = conformed.get(name, name)
            column = truncated.get(new, new)
            if column != name:
                self._column_name_mapping[name] = column
            # A conformed name that was then truncated.
            if new not in (column, name):
                self._column_aliases[new] = column
            if name in conformed:
                self._unconformed_columns[column] = unconformed.get(name, name)
        self._check_distinct_columns()

    def _truncate_column_names(
        self, schema: dict[str, dict]
    ) -> tuple[dict[str, dict], dict[str, str]]:
        """``truncate_column_names`` on a Postgres catalog, else nothing changed."""
        max_length = self.config.get("max_column_length", 63)
        if self.config.get("catalog_type", "postgres") != "postgres" or max_length <= 0:
            return schema, {}
        return truncate_column_names(schema, max_length=max_length)

    def _check_distinct_columns(self) -> None:
        """Refuse two columns whose final names DuckDB can't tell apart.

        DuckDB folds the case of ASCII letters in identifiers, so ``Id`` and
        ``id`` are one column (``É`` and ``é`` are two); truncation's
        suffixes can also meet another name.

        Raises:
            ValueError: Naming the stream's properties that share a column.
        """
        sources = {new: old for old, new in self._column_name_mapping.items()}
        by_column: dict[str, list[str]] = {}
        for name in self.flatten_schema:
            by_column.setdefault(fold_case(name), []).append(sources.get(name, name))
        if clashes := {col: old for col, old in by_column.items() if len(old) > 1}:
            msg = (
                f"stream {self.stream_name!r}: properties that would share a "
                f"column (DuckDB column names ignore ASCII case): {clashes}. Rename "
                "one in the tap (or with a stream map)."
            )
            raise ValueError(msg)

    def setup(self) -> None:
        # create the target schema if it doesn't exist
        self.connector.prepare_target_schema(target_schema_name=self.target_schema)
        # The table's columns before this run (empty for a new table), read
        # once for the checks and for prepare_table.
        existing = self.connector.existing_column_types(
            self.target_schema, self.target_table
        )
        self._check_partition_types(existing)
        self._check_conformed_columns(existing)
        self._check_nested_columns(existing)

        # prepare the table
        table_types = self.connector.prepare_table(
            target_schema_name=self.target_schema,
            table_name=self.target_table,
            columns=self.ducklake_schema,
            layout=self.layout,
            existing_column_types=existing,
        )
        self._table_columns = list(table_types)
        # DuckDB matches identifiers case-insensitively, and the singer-sdk
        # lowercases key_properties while columns keep the property's case.
        self._table_types = {name.lower(): typ for name, typ in table_types.items()}
        self._check_blob_columns()
        self._check_type_source_columns()
        self._apply_inlining(table_types)
        self.pyarrow_schema = staging_schema(
            self._staged_columns(
                self.ducklake_schema, self._table_types, self.flatten_schema
            )
        )
        self._native_columns = frozenset(
            field.name
            for field in self.pyarrow_schema
            if pa.types.is_struct(field.type) or pa.types.is_list(field.type)
        )

    def _check_type_source_columns(self) -> None:
        """Fit the type_source columns to the types the table already has.

        - Under ``on_error: string`` the columns are text, so a raw value
          always fits. A table that already has one as another type can't
          hold such a value: the stream is refused before anything loads.
        - Otherwise, a column the table already has as VARCHAR isn't retyped
          (in place, the target only widens integer and floating-point
          columns), with a warning. Its values are still converted, so
          ``on_error`` applies, and stored as the text ``on_error: string``
          stores, so the column has one text form.

        Raises:
            LayoutError: Under ``on_error: string``, for a typed column the
                table has as a type other than text.
        """
        if self._coercer is None:
            return
        for name, declared in self._coercer.type_source.columns.items():
            column = self._column_name_mapping.get(name, name)
            table_type = self._table_types.get(column.lower())
            if table_type is None or declared.kind == "string":
                continue
            is_text = pa.types.is_string(staging_type(table_type))
            if self._coercer.keeps_text and not is_text:
                msg = (
                    f"stream {self.stream_name!r}: on_error is string, so column "
                    f"{column!r} must be text to keep a value that isn't "
                    f"{declared.kind}, but table {self.target_table} has it as "
                    f"{table_type}. Set on_error to raise or null for this stream, "
                    "or recreate the column as VARCHAR."
                )
                raise LayoutError(msg)
            if is_text and not self._coercer.keeps_text:
                self.logger.warning(
                    "Stream %r: type_source declares column %r %s, but table %s "
                    "already has it as %s, which isn't retyped in place. Its values "
                    "are converted, then stored as text. Recreate the column to "
                    "type it.",
                    self.stream_name,
                    column,
                    declared.kind,
                    self.target_table,
                    table_type,
                )
                self._store_as_text(name, column)

    def _store_as_text(self, name: str, column: str) -> None:
        """Load a typed property as text, into the table's VARCHAR column.

        Declared a string for validation and staging alike, and its converted
        values written as text by the coercer.
        """
        text = {"type": ["null", "string"]}
        self.schema["properties"][name] = dict(text)
        self.flatten_schema[column] = dict(text)
        self._validator = self.get_validator()
        if self._coercer is not None:
            self._coercer.store_as_text(name)

    def _apply_inlining(self, table_types: dict[str, str]) -> None:
        """Give the table the configured ``data_inlining_row_limit``.

        Kept at 0 for a table with a STRUCT or LIST column: DuckLake 1.5.2
        fails to read back inlined rows holding a TIMESTAMP, TIMESTAMPTZ,
        INTERVAL or UUID inside a STRUCT, or a JSON list (DUCKLKD-110). When
        inlining is turned off for a table, its inlined rows are flushed to
        Parquet first. The table's option is changed only when it differs.
        """
        wanted = self.inlining.row_limit
        nested = [name for name, typ in table_types.items() if is_nested_type(typ)]
        if wanted and nested:
            self.logger.warning(
                "Keeping data inlining off for table %s: its STRUCT or LIST "
                "column(s) %s can't always be read back inlined by DuckLake "
                "1.5.2 (DUCKLKD-110).",
                self.target_table,
                nested,
            )
            wanted = 0
        current = self.connector.table_inlining_limit(
            self.target_schema, self.target_table
        )
        if current != wanted:
            # The option first, so no writer inlines rows after the flush.
            self.connector.set_table_inlining_limit(
                self.target_schema, self.target_table, wanted
            )
            if not wanted:
                self._flush_turning_off(current)
        self._inlining_limit = wanted

    def _flush_turning_off(self, previous: int) -> None:
        """Flush the rows the table inlined while inlining was on.

        If the flush fails, the table's option goes back to ``previous`` before
        the error is raised, so the next run finds inlining still on and
        flushes again, rather than leaving the rows inlined for good.
        """
        try:
            if self.connector.inlined_row_count(self.target_schema, self.target_table):
                self.connector.flush_inlined_data(self.target_schema, self.target_table)
        except (duckdb.Error, DuckLakeConnectorError):
            self.connector.set_table_inlining_limit(
                self.target_schema, self.target_table, previous
            )
            raise

    def _flush_inlined_data_if_due(self) -> None:
        """Write the table's inlined rows to Parquet once there are enough.

        Run at the end of the stream, when inlining is on and
        ``inlined_data_flush_rows`` isn't 0, so inlined rows don't pile up in
        the catalog between maintenance runs. The stream's rows are committed
        by then, so a failed flush only warns: failing the run would hold
        back its STATE, and the next run would load the rows again. The rows
        stay inlined until a later flush.
        """
        threshold = self.inlining.flush_rows
        if not self._inlining_limit or not threshold:
            return
        try:
            inlined = self.connector.inlined_row_count(
                self.target_schema, self.target_table
            )
            if inlined >= threshold:
                self.connector.flush_inlined_data(self.target_schema, self.target_table)
        except (duckdb.Error, DuckLakeConnectorError) as error:
            self.logger.warning(
                "Couldn't flush the inlined rows of table %s to Parquet; they "
                "stay inlined until a later flush: %s",
                self.target_table,
                error,
            )

    def _check_conformed_columns(self, existing_types: Mapping[str, str]) -> None:
        """Refuse to conform a key column the table has by its old name.

        With ``conform_column_names`` turned on for an existing table, a
        renamed property is loaded into a new column. For a key that means
        a merge matches on the new, empty column: no old row is replaced, and
        every row is loaded again beside the old ones. A renamed non-key
        column only starts a new column, which is warned about.

        Raises:
            ValueError: For a key column the table has only by its old name.
        """
        if not self._unconformed_columns:
            return
        existing = {fold_case(name): name for name in existing_types}
        renamed = {
            existing[fold_case(old)]: new
            for new, old in self._unconformed_columns.items()
            if fold_case(old) in existing and fold_case(new) not in existing
        }
        if not renamed:
            return
        keys = {fold_case(key) for key in self.key_properties}
        if renamed_keys := {
            old: new for old, new in renamed.items() if fold_case(new) in keys
        }:
            msg = (
                f"stream {self.stream_name!r}: conform_column_names renames key "
                f"column(s) of the existing table {self.target_table!r}: "
                f"{describe_renames(renamed_keys)}. A merge would match on the "
                "new, empty column and load every row again beside the old ones. Turn "
                "conform_column_names off for this stream, or migrate the table "
                "first (rename the columns in the catalog)."
            )
            raise ValueError(msg)
        self.logger.warning(
            "Stream %r: conform_column_names loads these columns of the existing "
            "table %r into new columns; the old columns are NULL in the rows "
            "loaded from now on: %s",
            self.stream_name,
            self.target_table,
            describe_renames(renamed),
        )

    def _check_nested_columns(self, existing: Mapping[str, str]) -> None:
        """Refuse a key, a partition or a sort on a STRUCT or LIST column.

        A key must compare as one value, and DuckLake refuses to sort by a
        struct, while an identity partition on one would write a directory per
        distinct value. The table's own type decides for a column it has, the
        stream's for a new one, as in ``_check_partition_types``;
        ``existing`` is the table's columns before this run.

        Raises:
            ValueError: For a key on a nested column.
            LayoutError: For a partition or sort on one.
        """
        types = {c["name"].lower(): c["type"] for c in self.ducklake_schema}
        nested = {name for name, typ in types.items() if is_nested_type(typ)}
        if not nested:
            return
        types.update({name.lower(): typ for name, typ in existing.items()})
        nested = {name for name, typ in types.items() if is_nested_type(typ)}
        for key in self.key_properties or ():
            if key.lower() in nested:
                msg = (
                    f"stream {self.stream_name!r}: key property {key!r} is a STRUCT "
                    "or LIST column, which can't be a key. Key on scalar "
                    "properties, or set nested_types: json for this stream."
                )
                raise ValueError(msg)
        keys = [("partition", k.column) for k in self.layout.partition or ()] + [
            ("sort", k.column) for k in self.layout.sort or ()
        ]
        for kind, column in keys:
            if column.lower() in nested:
                msg = (
                    f"stream {self.stream_name!r}: {kind} on column {column!r}, "
                    "a STRUCT or LIST column. Partition and sort on scalar "
                    "columns, or set nested_types: json for this stream."
                )
                raise LayoutError(msg)

    def _check_partition_types(self, existing: Mapping[str, str]) -> None:
        """Refuse an identity partition on a BLOB column.

        DuckLake stores identity partition values as text and fails on bytes
        that aren't UTF-8 ("Invalid unicode (byte sequence mismatch)"), so the
        batch would fail at insert for most binary values. A bucket works.

        The table's own type decides for a column it has (one created as
        VARCHAR keeps its text, whatever the stream now declares), the
        stream's for a new one. Run before ``prepare_table`` applies the
        layout. ``existing`` is the table's columns before this run.
        """
        identity = [k for k in self.layout.partition or () if k.transform == "identity"]
        if not identity:
            return
        types = {c["name"].lower(): c["type"] for c in self.ducklake_schema}
        types.update({name.lower(): typ for name, typ in existing.items()})
        for key in identity:
            column_type = types.get(key.column.lower(), "")
            if pa.types.is_binary(staging_type(column_type)):
                msg = (
                    f"stream {self.stream_name!r}: identity partition on BLOB "
                    f"column {key.column!r}. DuckLake stores identity partition "
                    "values as text and fails on bytes that aren't UTF-8; use "
                    "transform: bucket (with num_buckets) instead."
                )
                raise LayoutError(msg)

    def _check_blob_columns(self) -> None:
        r"""Refuse to load text into a BLOB column without its encoding.

        A table's BLOB column holds bytes; a property that doesn't declare
        ``contentEncoding: base64`` doesn't say how its text stands for them.
        DuckDB's VARCHAR to BLOB cast would store the text's own bytes
        ('hello'), read its backslash escapes ('a\x41' as 'aA'), or fail
        on other text without naming the property.
        """
        for column in self.ducklake_schema:
            name = column["name"]
            table_type = self._table_types.get(name.lower())
            if table_type is None or not pa.types.is_binary(staging_type(table_type)):
                continue
            if declares_base64_binary(self.flatten_schema.get(name, {})):
                continue
            msg = (
                f"Column {name!r} is BLOB, but property {name!r} doesn't declare "
                "contentEncoding: base64, so how its text stands for bytes is "
                "unknown. Declare contentEncoding: base64, as the column was "
                "loaded, or load the text into another column."
            )
            raise ValueError(msg)

    @staticmethod
    def _staged_columns(
        columns: list[dict[str, str]],
        table_types: dict[str, str],
        flattened: dict[str, dict],
    ) -> list[dict[str, str]]:
        """The columns to stage: each at its DDL type, or an existing column's.

        An existing column's own type is used where staging at the stream's
        type would change values before the insert casts them:
        - a number column the table has as DECIMAL, REAL or DOUBLE (staging at
          another scale rounded silently: a DECIMAL(38, 30) column staged at
          the default (38, 9) lost 21 fraction digits, and a DOUBLE column
          staged as a decimal stored 1.5e-12 as 0);
        - a number column the table has as an integer, so a value out of its
          range fails with a clear error and a fraction is rounded with a
          warning, or fails under strict ``value_type_checking``: staged as
          DECIMAL(38, 9), DuckDB's cast rounded 1.5 and 2.4 to 2 silently, and
          a key column ended up with two rows for key 2;
        - a string property the table has as a text column, whose text is then
          kept as sent, whatever its hint (a ``numeric`` hint would otherwise
          store '1.5' as '1.500000000');
        - an ``x-singer.decimal`` string the table has as DECIMAL, such as an
          unsized one in a table created as DECIMAL(18, 3) before it became
          VARCHAR: staged as text, DuckDB's cast rounded it silently, and a
          value too large failed with a bare conversion error;
        - a STRUCT or LIST column the table has with another type: an
          existing JSON column stays JSON, and its values are staged as JSON
          text; an existing STRUCT or LIST column is staged at its own type,
          so every field it has is staged.

        ``table_types`` is keyed by lowercased name; ``flattened`` is the
        flattened stream schema the columns were built from.
        """
        staged = []
        for column in columns:
            table_type = table_types.get(column["name"].lower())
            if table_type is None:
                staged.append(column)
                continue
            ours, theirs = staging_type(column["type"]), staging_type(table_type)
            prop = flattened.get(column["name"], {})
            types = prop.get("type") or []
            types = [types] if isinstance(types, str) else types
            use_table_type = (
                is_nested_type(column["type"])
                or is_nested_type(table_type)
                or (_is_fractional(ours) and _is_fractional(theirs))
                or (_is_fractional(ours) and pa.types.is_integer(theirs))
                or (
                    pa.types.is_string(theirs)
                    and [t for t in types if t != "null"] == ["string"]
                )
                or (
                    pa.types.is_decimal(theirs)
                    and prop.get("format") == SINGER_DECIMAL_FORMAT
                )
            )
            staged.append({**column, "type": table_type} if use_table_type else column)
        return staged

    @property
    def _staging_rules(self) -> StagingRules:
        """What staging does with a value its column would round.

        From ``value_type_checking``; key columns are named so a rounded key
        is reported as one (it can merge records), or fails under strict.
        With ``date_time_type`` timestamptz, the table's TIMESTAMPTZ columns
        are named so a date-time without a zone is read as UTC.
        """
        utc_columns: frozenset[str] = frozenset()
        if self.date_time_type == "timestamptz":
            utc_columns = frozenset(
                name
                for name, column_type in self._table_types.items()
                if _is_timestamptz(column_type)
            ) | frozenset(
                path.lower()
                for name, column_type in self._table_types.items()
                for path, leaf_type in nested_leaf_paths(name, column_type)
                if _is_timestamptz(leaf_type)
            )
        return StagingRules(
            strict=self.config.get("value_type_checking") == STRICT,
            # The records' own names, which the coercer sees.
            key_columns=frozenset(key.lower() for key in self._key_properties),
            utc_columns=utc_columns,
        )

    def _key_column_types(self) -> dict[str, str]:
        """Map each key property to its table column's actual type.

        Merge keys are cast to these types. They come from the table, read in
        setup, because an existing table's types can differ from this stream's
        declared types.
        """
        return {
            key: self._table_types[key.lower()]
            for key in self.key_properties
            if key.lower() in self._table_types
        }

    def process_record(self, record: dict, context: dict) -> None:
        """Process the record and flatten if necessary."""
        # Present when add_record_metadata is on: the record's table version.
        if (version := record.get(VERSION_COLUMN)) is not None:
            self._versions_received.add(version)
        record_flatten = flatten_record(
            record,
            flatten_schema=self.flatten_schema,
            max_level=self.flatten_max_level,
            native_columns=self._native_columns,
            renamed=self._column_name_mapping,
            truncated=self._truncated_columns,
        )
        super().process_record(record_flatten, context)

    def _remove_temp_table_duplicates(self, pyarrow_df):
        """Remove duplicates within PyArrow table based on key properties."""
        if not self.key_properties or pyarrow_df is None:
            return pyarrow_df

        # Matched as DuckDB matches column names (see fold_case).
        pyarrow_df = pyarrow_df.rename_columns(
            [fold_case(col) for col in pyarrow_df.column_names]
        )
        keys = [fold_case(key) for key in self.key_properties]

        # Check that all key properties exist in the pyarrow table
        available_columns = set(pyarrow_df.column_names)
        for key_property in keys:
            if key_property not in available_columns:
                raise ValueError(
                    f"Key property {key_property} not found in pyarrow temp file for {self.target_table}. Available columns: {list(available_columns)}"
                )

        original_row_count = len(pyarrow_df)
        # Convert to polars, drop duplicates, then back to pyarrow.
        # Keep the last occurrence: records arrive in stream order, so the last one
        # is the newest version of the row. Polars' default (keep="any") may keep
        # an older version and overwrite the newer one in the target table.
        polars_df = pl.from_arrow(pyarrow_df)
        polars_df = polars_df.unique(subset=keys, keep="last", maintain_order=True)
        pyarrow_df = polars_df.to_arrow()

        new_row_count = len(pyarrow_df)
        duplicates_removed = original_row_count - new_row_count
        if duplicates_removed > 0:
            self.logger.info(
                f"Removed {duplicates_removed} duplicate rows based on key properties: {self.key_properties}"
            )

        return pyarrow_df

    def _get_memory_usage_mb(self) -> float:
        """Get current process memory usage in MB."""
        process = psutil.Process()
        return process.memory_info().rss / (1024 * 1024)

    def write_temp_file(self, context: dict) -> str:
        """Write the current batch to a temporary parquet file."""
        # Track memory before writing temp file
        memory_before_mb = self._get_memory_usage_mb()
        self.logger.info(
            f"[Memory] Before write_temp_file for {self.stream_name}: {memory_before_mb:.2f} MB"
        )

        self.logger.info(
            f"Writing batch for {self.stream_name} with {len(context['records'])} records to parquet file."
        )
        memory_before_concat = self._get_memory_usage_mb()
        pyarrow_df = None
        pyarrow_df = concat_tables(
            records=context.get("records", []),
            pyarrow_table=pyarrow_df,
            pyarrow_schema=self.pyarrow_schema,  # type: ignore
            flattened_schema=self.flatten_schema,
            convert_tz_to_utc=self.convert_tz_to_utc,
            rules=self._staging_rules,
        )
        memory_after_concat = self._get_memory_usage_mb()
        memory_diff_concat = memory_after_concat - memory_before_concat
        self.logger.info(
            f"[Memory] After concatenating tables: {memory_after_concat:.2f} MB (was {memory_before_concat:.2f} MB before)"
            f"(delta: {memory_diff_concat:+.2f} MB, {memory_diff_concat / memory_before_concat * 100:.2f}%)"
        )

        # Drop duplicates based on key properties if they exist in temp file
        # TODO: we may be able to skip this step. Deduplication handled when merging into DuckLake.
        if self.key_properties and pyarrow_df is not None:
            memory_before_dedup = self._get_memory_usage_mb()
            pyarrow_df = self._remove_temp_table_duplicates(pyarrow_df)

            memory_after_dedup = self._get_memory_usage_mb()
            memory_diff_dedup = memory_after_dedup - memory_before_dedup
            self.logger.info(
                f"[Memory] After deduplicating temp file: {memory_after_dedup:.2f} MB (was {memory_before_dedup:.2f} MB before)"
                f"(delta: {memory_diff_dedup:+.2f} MB, {memory_diff_dedup / memory_before_dedup * 100:.2f}%)"
            )

        self.logger.info(
            f"Batch pyarrow table has ({len(pyarrow_df)} rows)"  # type: ignore
        )
        del context["records"]

        # Ensure temp directory exists
        os.makedirs(self.temp_file_dir, exist_ok=True)

        # Create a more descriptive filename with timestamp
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        file_name = f"{self.stream_name}_{timestamp}_{self.files_saved:05d}.parquet"
        full_temp_file_path = os.path.join(self.temp_file_dir, file_name)

        try:
            self.logger.info(f"Writing batch to temp file: {full_temp_file_path}")
            pq.write_table(pyarrow_df, full_temp_file_path)
            self.files_saved += 1
            row_count = len(pyarrow_df) if pyarrow_df is not None else 0
            self.logger.info(f"Successfully wrote {row_count} rows to {file_name}")

            # Track memory after writing temp file
            memory_after_mb = self._get_memory_usage_mb()
            memory_diff_mb = memory_after_mb - memory_before_mb
            self.logger.info(
                f"[Memory] After write_temp_file for {self.stream_name}: {memory_after_mb:.2f} MB (was {memory_before_mb:.2f} MB at start)"
                f"(delta: {memory_diff_mb:+.2f} MB, {memory_diff_mb / memory_before_mb * 100:.2f}%)"
            )

            return full_temp_file_path
        except Exception as e:
            self.logger.error(
                f"Failed to write parquet file {full_temp_file_path}: {e}"
            )
            raise

    def process_batch(self, context: dict) -> None:
        """Process a batch of records."""
        # first write each batch to a parquet file
        temp_file_path = self.write_temp_file(context)
        self.logger.info(f"Temp file path: {temp_file_path}")

        if self.should_overwrite_table:
            # Kept, and loaded with every other batch in one transaction at
            # clean_up, so readers see the old table until the new one is whole.
            self._staged_files.append(temp_file_path)
            return

        # get the columns from the file and the table
        file_columns = [col["name"] for col in self.ducklake_schema]
        table_columns = self._table_columns

        with self._committing([temp_file_path]):
            # Without key properties, or with load method append, insert.
            if self.applied_load_method == "append":
                self.connector.insert_into_table(
                    file_location=temp_file_path,
                    target_schema_name=self.target_schema,
                    table_name=self.target_table,
                    file_columns=file_columns,
                    target_table_columns=table_columns,
                    delete_marked_keys=(
                        list(self.key_properties) if self.delete_marked else None
                    ),
                    key_column_types=self._key_column_types(),
                )
            else:
                self.connector.merge_into_table(
                    file_location=temp_file_path,
                    target_schema_name=self.target_schema,
                    table_name=self.target_table,
                    file_columns=file_columns,
                    target_table_columns=table_columns,
                    key_properties=list(self.key_properties),
                    key_column_types=self._key_column_types(),
                    delete_marked=self.delete_marked,
                )

        # delete file after insert
        os.remove(temp_file_path)
        self.logger.info(
            f"[Memory] After merging temp file: {self._get_memory_usage_mb():.2f} MB"
        )

    def clean_up(self) -> None:
        """Perform per-stream cleanup.

        For an overwrite, replace the table's rows with every staged batch in
        one transaction first. The SDK cleans up after the final drain and
        before it writes STATE, so STATE follows the commit, and a failed
        replace fails the run with the old rows intact.
        """
        try:
            if self.has_uncommitted_overwrite:
                with self._committing(self._staged_files):
                    self.connector.replace_table_contents(
                        file_locations=self._staged_files,
                        target_schema_name=self.target_schema,
                        table_name=self.target_table,
                        file_columns=[col["name"] for col in self.ducklake_schema],
                        target_table_columns=self._table_columns,
                        delete_marked=self.delete_marked,
                        key_properties=list(self.key_properties or []),
                    )
                self._overwrite_committed = True
            self._flush_inlined_data_if_due()
            if self._coercer is not None:
                self._coercer.report()
            super().clean_up()
        finally:
            self._remove_temp_file_dir()

    @property
    def applied_load_method(self) -> str:
        """How the sink loads: overwrite, append, or merge.

        Overwrite when the table is replaced (``should_overwrite_table``);
        append without key properties, as a merge needs them; else
        ``load_method``.
        """
        if self.should_overwrite_table:
            return "overwrite"
        if not self.key_properties or self.load_method == "append":
            return "append"
        return "merge"

    @contextlib.contextmanager
    def _committing(self, staged_files: list[str]) -> Iterator[None]:
        """Report the commit made in the block to the run's ledger, if it succeeds.

        Accounting never fails the run: rows that can't be counted are
        reported as unknown, with a warning, and the commit is still recorded.

        Args:
            staged_files: The parquet files the commit loads; none for a
                delete. Their rows are the commit's rows.
        """
        ledger = self._write_ledger
        if ledger is None:
            yield
            return
        rows: int | None
        try:
            rows = sum(pq.read_metadata(path).num_rows for path in staged_files)
        except Exception as error:  # noqa: BLE001 (advisory: never fails the run)
            self.logger.warning(
                "Couldn't count the rows of a batch of %s for its writeCompleted "
                "event, which leaves rows out: %s",
                self.target_table,
                error,
            )
            rows = None
        started_at = datetime.now(timezone.utc)
        yield
        ledger.record(
            self.target_schema,
            self.target_table,
            self.stream_name,
            rows=rows,
            load_method=self.applied_load_method,
            started_at=started_at,
            completed_at=datetime.now(timezone.utc),
        )

    @property
    def has_uncommitted_overwrite(self) -> bool:
        """Whether this is an overwrite whose replace hasn't been committed yet.

        Until it is, STATE must not be emitted (see
        ``Targetducklake._write_state_message``).
        """
        return self.should_overwrite_table and not self._overwrite_committed

    def _remove_temp_file_dir(self) -> None:
        """Remove this stream's dedicated temp directory."""
        if hasattr(self, "temp_file_dir"):
            self.logger.info(
                f"Cleaning up temp directory for stream {self.stream_name}: {self.temp_file_dir}"
            )
            try:
                if os.path.exists(self.temp_file_dir):
                    shutil.rmtree(self.temp_file_dir)
            except Exception as e:
                self.logger.warning(
                    f"Non-fatal error during temp directory cleanup for {self.stream_name}: {e}"
                )

    def activate_version(self, new_version: int) -> None:
        """Handle ACTIVATE_VERSION: retire rows that are not in the activated version.

        A full-table tap sends its records with a table version, then
        ACTIVATE_VERSION for it; rows of any other version (or none) are gone
        at the source. As in the SDK, they are marked deleted
        (``_sdc_deleted_at`` set) by default, and removed with ``hard_delete``. The target drains this sink first (see
        ``Targetducklake._process_activate_version_message``), so no record
        of the version is still buffered.

        Nothing is deleted when:
        - records carry no version (``add_record_metadata`` is off);
        - no record of this version has been received and the version
          wasn't announced before, as when a tap announces the version
          before sending its records, which would otherwise empty the table
          for the whole sync (a second ACTIVATE_VERSION with no records
          between means the source is empty, and every other row goes);
        - the stream is an overwrite, which replaces the whole table at
          clean_up anyway, and must not empty it early.
        """
        if not self.include_sdc_metadata_properties:
            self.logger.warning(
                f"Ignoring ACTIVATE_VERSION {new_version} for {self.stream_name}: "
                "records carry their table version only with add_record_metadata, "
                "so rows of older versions can't be told apart."
            )
            return
        if self.should_overwrite_table:
            self.logger.info(
                f"ACTIVATE_VERSION {new_version} for {self.stream_name}: the "
                "overwrite replaces the whole table at the end of the stream."
            )
            return
        if (
            new_version not in self._versions_received
            and new_version not in self._versions_announced
        ):
            # A second ACTIVATE_VERSION for the version, even with no records
            # between, ends its sync: the source is then empty.
            self._versions_announced.add(new_version)
            self.logger.info(
                f"ACTIVATE_VERSION {new_version} for {self.stream_name} arrived "
                "before any of its records; nothing to remove yet."
            )
            return
        with self._committing([]):
            if self.config.get("hard_delete", False):
                self.connector.delete_other_versions(
                    self.target_schema, self.target_table, new_version
                )
                return
            # As the SDK does by default: mark them deleted, at the current
            # UTC time, and keep them. A row the source sends again is revived
            # when it is next loaded (its own _sdc_deleted_at is empty).
            deleted_at_type = self._table_types.get(DELETED_AT_COLUMN, "")
            self.connector.mark_other_versions_deleted(
                self.target_schema,
                self.target_table,
                new_version,
                deleted_at_is_timestamptz=_is_timestamptz(deleted_at_type),
            )


def stream_name_to_dict(stream_name, separator="-"):
    catalog_name = None
    schema_name = None
    table_name = stream_name

    # Schema and table name can be derived from stream if it's in <schema_nama>-<table_name> format
    s = stream_name.split(separator)
    if len(s) == 2:
        schema_name = s[0]
        table_name = s[1]
    if len(s) > 2:
        catalog_name = s[0]
        schema_name = s[1]
        table_name = "_".join(s[2:])

    return {
        "catalog_name": catalog_name,
        "schema_name": schema_name,
        "table_name": table_name,
    }
