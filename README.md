# target-ducklake

`target-ducklake` is a Singer target for ducklake. This target is in development and may not be stable. I built this with GCS in mind, but it should work with S3 and local storage, though this has not been tested extensively.

Supports append, merge, and overwrite load methods (default is merge). The load method can be configured using the `load_method` setting, for every stream or per stream (see [Load method and keys per stream](#load-method-and-keys-per-stream)). If no key properties are provided and `overwrite_if_no_pk` is true, data will be overwritten. Otherwise, the configured load method determines the behavior.

## Configuration

### Accepted Config Options

| Setting | Type | Required | Default | Description |
|---------|------|----------|---------|-------------|
| `catalog_url` | string | ✅ | - | URL connection string to your catalog database |
| `data_path` | string | ✅ | - | GCS, S3, or local folder path for data storage |
| `storage_type` | string | ✅ | `"local"` | Type of storage: GCS, S3, or local |
| `catalog_type` | string | ✅ | `"postgres"` | Type of catalog database: postgres, sqlite, mysql, or duckdb. See <https://ducklake.select/docs/stable/duckdb/usage/choosing_a_catalog_database> |
| `meta_schema` | string | ❌ | - | Schema name in the catalog database to use for Ducklake metadata tables. If not provided will use the default schema for the catalog database (eg `public` for Postgres) |
| `public_key` | string | ❌ | - | Public key for private GCS and S3 storage authentication (optional) |
| `secret_key` | string | ❌ | - | Secret key for private GCS and S3 storage authentication (optional) |
| `region` | string | ❌ | - | AWS region for S3 storage type (required when using S3 with explicit credentials) |
| `default_target_schema` | string | ❌ | - | Default database schema where data should be written. If not provided schema will attempt to be inferred from the stream name (inferring schema only works for database extractors ) |
| `target_schema_prefix` | string | ❌ | - | Prefix to add to the target schema name. If not provided, no prefix will be added |
| `add_record_metadata` | boolean | ❌ | `false` | When True, automatically adds Singer Data Capture (SDC) metadata columns to target tables |
| `hard_delete` | boolean | ❌ | `false` | When True, `ACTIVATE_VERSION` deletes rows absent from the activated version; when False, it sets their `_sdc_deleted_at`. See [Overwrite and ACTIVATE_VERSION](#overwrite-and-activate_version). |
| `delete_rows_with_deleted_at` | boolean | ❌ | `false` | When True, a record whose `_sdc_deleted_at` is set (a log-based CDC tap's delete) deletes its key's row, in the same transaction as its batch, instead of being loaded as a live row. Needs key properties, except with `load_method: overwrite`. Also settable per stream in `stream_defaults`/`stream_overrides`. See [Deletes from CDC taps](#deletes-from-cdc-taps) |
| `process_activate_version_messages` | boolean | ❌ | `true` | When False, `ACTIVATE_VERSION` messages are ignored. |
| `load_method` | string | ❌ | `"merge"` | Method to use for loading data into the target table: append, merge, or overwrite. Also settable per stream in `stream_defaults`/`stream_overrides`, as can the stream's `key_properties`. See [Load method and keys per stream](#load-method-and-keys-per-stream) and [Overwrite and ACTIVATE_VERSION](#overwrite-and-activate_version). |
| `flatten_max_level` | integer | ❌ | `0` | Maximum depth for flattening nested fields. Set to 0 to disable flattening. Objects deeper than this are STRUCT columns (see `nested_types`) |
| `nested_types` | string | ❌ | `native` | How an object with declared `properties` and an array with typed `items` are loaded: `native` creates STRUCT and LIST columns, as target-iceberg does; `json` stores them as JSON text, as before. Applies to new tables and new columns. Also settable per stream in `stream_defaults`/`stream_overrides`. See [Nested columns](#nested-columns) |
| `temp_file_dir` | string | ❌ | `"temp_files/"` | Directory path for storing temporary parquet files |
| `max_batch_size` | integer | ❌ | `10000` | Maximum number of records to process in a single batch |
| `partition_fields` | object | ❌ | - | Object mapping stream names to arrays of partition column definitions. Each stream key maps directly to an array of column definitions |
| `auto_cast_timestamps` | boolean | ❌ | `false` | When True, automatically attempts to cast timestamp-like fields to timestamp types in ducklake |
| `convert_tz_to_utc` | boolean | ❌ | `false` | When True, converts timezone-aware date-time values to UTC before loading, so a timestamp column without time zone stores the UTC time. When False, such a column stores each value's local wall-clock time (its offset is dropped). With the default, a timestamp column fed values in different offsets holds local times that aren't directly comparable; turn this on, or declare the column `timestamptz` (via `x-sql-datatype`), to compare instants. Either way, a `timestamptz` column stores the correct instant, and values with any offset, such as `+05:30`, load. Values without a time zone are loaded as given into a `timestamp` column; a `timestamptz` column reads them as UTC with `date_time_type: timestamptz` (the default), and in the DuckDB session's time zone with `timestamp`. |
| `date_time_type` | string | ❌ | `timestamptz` | The column type of a `format: date-time` property without an `x-sql-datatype` hint. `timestamptz` loads instants, as target-iceberg does: a value with an offset is stored as its UTC instant, and one without an offset is read as UTC. `timestamp` loads wall-clock times (see `convert_tz_to_utc`). Also settable per stream in `stream_defaults`/`stream_overrides`. See [Date-time columns](#date-time-columns) |
| `data_inlining_row_limit` | integer | ❌ | `0` | An insert of at most this many rows is stored in the catalog (DuckLake data inlining) instead of a new Parquet file. `0` turns inlining off. Also settable per stream in `stream_defaults`/`stream_overrides`. See [Data inlining](#data-inlining) |
| `inlined_data_flush_rows` | integer | ❌ | `10000` | With inlining on, a stream ends by writing its table's inlined rows to Parquet once they reach this many; `0` never flushes. Also settable per stream. See [Data inlining](#data-inlining) |
| `validate_records` | boolean | ❌ | `false` | Whether to validate the schema of the incoming streams |
| `value_type_checking` | string | ❌ | `permissive` | What to do with a number its column would round: `permissive` rounds it as DuckDB's cast of a DECIMAL does, with a warning; `strict` fails the batch naming the column and the value. See [Values a column would round](#values-a-column-would-round) |
| `overwrite_if_no_pk` | boolean | ❌ | `false` | When True, a stream without primary keys replaces the target table's rows, as `load_method: overwrite` does. Overrides load_method. |
| `max_column_length` | integer | ❌ | `63` | Maximum length for column names. Only applied when `catalog_type` is `postgres`. Names exceeding this limit are truncated. Colliding truncated names get a `_{i}` suffix. Set to `0` to disable. See [ducklake#619](https://github.com/duckdb/ducklake/issues/619). |
| `parallel_draining` | boolean | ❌ | `true` | When True, enables parallel draining with `max_parallelism=10` (Meltano default). When False, sets `max_parallelism=1` to disable parallel draining. Ignored when `catalog_type` is `sqlite`: SQLite allows one writer at a time, so its sinks are always drained one at a time (a log line says so). |
| `max_record_age_minutes` | number | ❌ | `10.0` | Maximum age in minutes that records can sit in a sink before a drain is forced. Tune lower for more frequent flushes, higher for larger batches. |

### Example Meltano YAML Configuration

```yaml
plugins:
    # ... other plugins ...
    loaders:
    - name: target-ducklake
      namespace: target_ducklake
      pip_url: https://github.com/definite-app/target-ducklake.git
      config:
        catalog_url: postgres://test:test@localhost:5432/test_db
        data_path: gs://test-bucket/test-path
        storage_type: GCS # Optional (default local)
        public_key: GOOG1234567890 # Optional (required for private GCS and S3 storage)
        secret_key: GOOG1234567890 # Optional (required for private GCS and S3 storage)
        default_target_schema: my_schema # Optional (default None)
        target_schema_prefix: my_prefix # Optional
        max_batch_size: 10000 # Optional (default 10000)
        add_record_metadata: true # Optional
        load_method: merge # Optional (default merge, options: append, merge, overwrite)
        auto_cast_timestamps: true # Optional
        overwrite_if_no_pk: true # Optional (default false)
        partition_fields: {"my_stream": [{"column_name": "created_at", "type": "timestamp", "granularity": ["year", "month"]}]} # Optional
```

### Binary columns

A `string` property with `contentEncoding: base64` is a BLOB column, as in target-iceberg, with or without a binary `x-sql-datatype` hint. A `format`, or a supported hint other than a binary or text type (`uuid`, a timestamp), decides the column first.

- **Decoded strictly** (`base64.b64decode(..., validate=True)`), always, whatever `value_type_checking` says: text outside the standard base64 alphabet, bad or missing padding, a line break, or the URL-safe alphabet (`-`, `_`) fails the batch, naming the column, rather than storing the wrong bytes.
- **An existing VARCHAR column** keeps the text as sent.
- **An existing BLOB column** fed by a property without `contentEncoding: base64` fails the stream at startup, naming the column: DuckDB's cast would store the text's own bytes, read its `\x` escapes, or fail without naming it.
- **Partitioning:** an identity partition on a BLOB column is refused (DuckLake stores identity partition values as text and fails on bytes that aren't UTF-8); a `bucket` partition works.

### Nested columns

By default (`nested_types: native`), an `object` with non-empty `properties` is a STRUCT column and an `array` with typed `items` is a list column (`T[]`), at any depth, as in target-iceberg. Consumers query fields directly (`o.price`, `o.items[1].n`) instead of parsing JSON.

- **Each field and element is typed as a top-level property would be**: `x-sql-datatype` hints, `x-singer.decimal`, `date_time_type` (a zone-less date-time in a TIMESTAMPTZ field is read as UTC), `contentEncoding: base64` (BLOB), `convert_tz_to_utc`, and the rounding rules of `value_type_checking`. Errors and warnings name the field by its path, e.g. `Column 'o.items[].price'`. Field names are quoted in the DDL, so reserved words such as `at` work.
- **Stays JSON:** an object without `properties`, an array without typed `items`, an object or array declared through `anyOf`/`oneOf`/`allOf` or alongside other types (`["object", "string"]`), an object whose property names are empty or equal ignoring case (a STRUCT can't hold them), and one with a `json`/`jsonb` hint. Inside a STRUCT such a field is a JSON field. A field with several scalar types is VARCHAR, as a top-level property is. With `nested_types: json`, every object and array is JSON, as before.
- **`flatten_max_level` flattens first**: objects above that depth become `parent__child` columns, and deeper objects become STRUCT columns.
- **Keys a STRUCT doesn't declare** are dropped with a warning counting them per field (DuckLake would drop them silently), or fail the batch under strict `value_type_checking`. A missing field is NULL. NULL objects, NULL lists, empty lists and NULL elements are stored as such.
- **Refused at startup**, naming the column: a key property that is a STRUCT or LIST column, and a partition or sort on one. Use scalar columns, or `nested_types: json` for the stream.
- **Existing tables:** a column that already exists as JSON stays JSON and keeps receiving JSON text. A new object or array property is added as a STRUCT or LIST column. See [Nested schema changes](#nested-schema-changes).

The default changed from JSON to `native` for new tables and new columns. Set `nested_types: json` to keep creating JSON columns.

#### Nested schema changes

When a stream's schema changes, an existing STRUCT or LIST column follows it where DuckDB can change the table in place, as metadata only:

- **A new sub-property** of a STRUCT, or of the STRUCT in a list (`items[].f`), at any depth, is added to the column (`ALTER TABLE ... ADD COLUMN "o"."items"."element"."f" ...`) and logged. Rows already loaded have NULL in it.
- **A sub-property the stream no longer declares** stays in the table and gets NULL in new rows.
- **A changed field type** is staged at the table's type where the scalar rules keep the value's meaning. Numbers load into any numeric field (an integer into DECIMAL(10, 2), rounded as at the top level), anything loads into a VARCHAR or JSON field, and text loads into a date-time, date or UUID field. Any other change fails the stream at startup, naming the field and both types: text into a number, boolean or BLOB field, for example, or a field that changed between scalar, STRUCT and list. DuckDB can't change a nested field's type in place, and changing the whole column's type would drop the fields the stream no longer declares.
- **A STRUCT or LIST column whose stream no longer declares its shape** (an object without properties, another type, or `nested_types: json`) fails the stream at startup, naming the column. An existing JSON column stays JSON.

The checks run before anything changes, so a refused stream leaves the table as it was. Field names match case-insensitively, as DuckDB's do. On a Postgres catalog, nested field names aren't bound by Postgres's 63-character limit (they aren't identifiers in the catalog), so only top-level names are truncated (`max_column_length`).

### Date-time columns

A Singer `date-time` is an instant: sources send UTC, and a value with another offset stands for its UTC instant. So by default (`date_time_type: timestamptz`) a `format: date-time` property without an `x-sql-datatype` hint is a TIMESTAMPTZ column, as in target-iceberg:

- A value with an offset (`2024-01-01T21:00:00+09:00`) is stored as its instant (`12:00 UTC`).
- A value without one (`2024-01-01T12:00:00`) is read as UTC, in every TIMESTAMPTZ column of the stream, including nested and auto-cast values that arrive as text, and merge keys. DuckDB alone would read it in its session time zone, the machine's local zone.
- **Existing tables keep their columns.** A column already created as TIMESTAMP stays TIMESTAMP, and its values load as before; new tables and new columns get TIMESTAMPTZ.
- `_sdc_deleted_at`, set when ACTIVATE_VERSION retires rows, is the current instant in either column type.
- An `x-sql-datatype` hint still decides: `timestamp` gives TIMESTAMP, `timestamptz` TIMESTAMPTZ.

`date_time_type: timestamp` gives the earlier behaviour: TIMESTAMP columns holding wall-clock times (each value's own, or UTC with `convert_tz_to_utc`), and hinted TIMESTAMPTZ columns that read a value without an offset in the session time zone. It can be set for all streams, in `stream_defaults`, or per stream in `stream_overrides`.

### Load method and keys per stream

`load_method` and `key_properties` can be set per stream in `stream_defaults` and `stream_overrides` (see [Partitioning and sort order](#partitioning-and-sort-order) for how the two merge). One tap can then append its events and merge its dimensions in one pipeline:

```yaml
load_method: merge          # every stream without its own
stream_overrides:
  page_views:
    load_method: append
  daily_snapshot:
    load_method: overwrite
  legacy_accounts:          # the tap declares no keys
    key_properties: [account_id]
```

- **`load_method`:** `merge`, `append` or `overwrite`. It is taken from the stream's override, else `stream_defaults`, else the top-level `load_method`, else `merge`.
- **`write_mode`** (target-iceberg's name for the same setting) is accepted with target-iceberg's values: `append-only`, `upsert` and `overwrite` mean `append`, `merge` and `overwrite`. A level that sets both must agree. Unlike target-iceberg, which declares `write_mode` but ignores it, target-ducklake applies it.
- **`key_properties`:** a list of the stream's property names. It replaces the keys the tap declares, for a tap that declares none or the wrong ones, and is used for everything that needs keys: merge, dedup within a batch, `delete_rows_with_deleted_at` and `overwrite_if_no_pk` (a stream given keys is no longer keyless). A name the stream doesn't have fails the stream when its schema arrives.
- A stream without keys can't merge, so `merge` appends, as before.

### Partitioning and sort order

Each stream's table layout is set with `stream_defaults` (every stream) and `stream_overrides` (keyed by stream name, merged over the defaults key by key). This is the same shape as target-iceberg, so both targets can be configured alike:

```yaml
stream_defaults:
  sort_order: [updated_at]
stream_overrides:
  orders:
    partition:
      - {column: occurred_at, transform: day}
      - {column: tenant_id, transform: bucket, num_buckets: 16}
    sort_order: [customer_id, "occurred_at DESC NULLS LAST"]
```

- **`partition`:** a list of `{column, transform}` entries. Transforms are `identity` (the default), `year`, `month`, `day`, `hour` and `bucket`, in lowercase as in target-iceberg and Zamboni (`Day` is refused); `bucket` requires `num_buckets`.
- **`sort_order`:** column names, each optionally followed by `ASC`/`DESC` and `NULLS FIRST`/`NULLS LAST`. DuckLake writes each new file's rows in this order.
- **Column names** are the table's (flattened names use `__`), matched case-insensitively. An unknown key, transform or column fails the stream with a message naming it, rather than being ignored.
- **When applied:** a new table gets the layout at creation. On an existing table, a changed layout is applied as metadata, and files written afterwards follow it (unlike target-iceberg, where partitioning is fixed at creation). An unchanged layout is left alone. A stream with no `partition` or `sort_order` keeps whatever layout its table already has; an empty list (`partition: []`, `sort_order: []`) removes the table's keys.
- **`table_config`:** the path to a Zamboni `table-config.json`, the file target-iceberg reads. Tables are matched case-insensitively, then by target-iceberg's conformed name (`Orders Daily` → `orders_daily`); a table with no block takes `defaults` (logged), so a `defaults.partition` must name columns every stream has, as in target-iceberg. The file must be version 2 (`namespaces.<ns>.tables.<table>`), the format Zamboni reads; the namespace is the target schema. The version 1 shape (top-level `tables` keyed `namespace.table`) and any other `version` are refused, since Zamboni refuses them too. A table's `partition` sets its partition keys, and an `ordering` with `mode: sort` (`[{column, direction, nulls}]`) sets its sort order (`mode: none`, the default, removes it); an ordering Zamboni would refuse is refused too: a `sort` list or `zorder` block without the matching `mode`, both at once, an empty `sort`, an unknown key in a sort entry, or a direction or nulls value other than lowercase `asc`/`desc` and `first`/`last`. A missing `warehouse` is not checked, since target-ducklake doesn't use it. Maintenance-only keys (`retention`, `partition_evolution`, …) are ignored, and keys target-ducklake doesn't know are logged, not rejected. The format is pinned to Zamboni's published spec: `tests/contracts/` holds a copy from a Zamboni tag, refreshed by `bin/refresh-table-config-contract.sh`, and `tests/test_table_config_contract.py` fails if the reader and the spec disagree on a key, transform, ordering mode or version. A key in the table's block wins over `defaults`, and lists replace whole, as in Zamboni. A stream's own `partition` and `sort_order` win over the file, and the file wins over `partition_fields`.

**Configs shared with target-iceberg.** A config written for target-iceberg works here. What only configures Iceberg is accepted and ignored with a log line at startup:
- `delete_strategy` and `merge_on_read_maintenance_acknowledged` (DuckLake deletes always write delete files, i.e. merge-on-read; compaction is maintenance);
- `on_error` without `type_source`, which it alone applies to (target-iceberg injects `on_error: raise` into `stream_defaults`);
- a partition `name`;
- the `truncate` and `void` transforms (that entry is skipped);
- `ordering` with `mode: zorder`;
- the table config's maintenance sections (`partition_evolution`, `retention`, sizing, …), `warehouse` and `version`.

If skipping `truncate`/`void` leaves no partition key, the partition counts as not configured, so an existing table's partitioning is not reset by accident.

`write_mode` and `key_properties` are applied (see [Load method and keys per stream](#load-method-and-keys-per-stream)). A setting that would change what data is loaded but isn't implemented is **rejected**, never ignored: `type_source`, with any `on_error` (DUCKLKD-43). Unknown keys, unknown transforms and columns the stream lacks are rejected too. Column names may be given in full where a Postgres catalog truncates them to 63 characters.

- **Checked at startup:** the `stream_defaults`/`stream_overrides` settings of every stream, and every table's layout sections in `table_config`, are checked when the target starts (a missing or invalid file fails here). Column names, and the older `partition_fields`, are checked when each stream's schema arrives.
- **Both settings can be given as JSON strings** (for environment variables).
- **`date_time_type`** can be set in `stream_defaults` and `stream_overrides` too, over the top-level setting (see [Date-time columns](#date-time-columns)), and so can **`nested_types`** (see [Nested columns](#nested-columns)).

The older `partition_fields` setting still works for a stream without a `partition`:

```json
{
    "stream_name": [
        {"column_name": "created_at", "type": "timestamp", "granularity": ["year", "month"]},
        {"column_name": "status", "type": "identifier"}
    ]
}
```

A full list of supported settings and capabilities for this
target is available by running:

```bash
target-ducklake --about
```

### Data inlining

DuckLake can store a small insert's rows in the catalog database instead of a new Parquet file ("data inlining"), so frequent small incremental runs don't leave many tiny files. It is off by default (`data_inlining_row_limit: 0`), as it always was, and every catalog type is attached with it off. A `duckdb` catalog used DuckLake's default (on) before.

- **`data_inlining_row_limit: N`** turns it on: each insert of at most N rows (one batch of a stream) is inlined. It is set on the table as a DuckLake option (`set_option('data_inlining_row_limit', N, table_name => ...)`), kept in the catalog, so other writers of the table inline too. The target writes the table option only when the limit that applies to the table differs (a table option wins over a schema one, which wins over a global one). So a table already following a schema or global option of N gets no table option of its own.
- **Flushing:** a stream ends by writing its table's inlined rows to one Parquet file (`ducklake_flush_inlined_data`) once they reach `inlined_data_flush_rows` (10000 by default), so they don't pile up in the catalog. `0` never flushes, leaving it to maintenance.
- **Turning it off** (back to `0`) sets the table's option to 0, then flushes what it has inlined. If that flush fails, the option goes back and the run fails, so the next run tries again.
- **Kept off for a table with a STRUCT or LIST column**, with a warning naming the table. DuckLake 1.5.2 can't read back inlined rows holding a TIMESTAMP, TIMESTAMPTZ, INTERVAL or UUID inside a STRUCT, or a JSON list (DUCKLKD-110): the read fails on SQLite and invalidates the database on Postgres. A nested column added later turns inlining off for the table, flushing first. A test pins the upstream bug, so an upgrade that fixes it shows.

### Column types

Each property's JSON Schema type and format give its column type: `integer` is BIGINT (INTEGER with 32-bit bounds), `number` is DECIMAL(38, 9) (or its `precision`/`scale`), `boolean` is BOOLEAN, `object` and `array` are STRUCT and list columns where their shape is declared and JSON otherwise (see [Nested columns](#nested-columns)), and `string` is VARCHAR, or DATE, TIME, TIMESTAMPTZ (`date-time`; see [Date-time columns](#date-time-columns)) or UUID by its `format`. A string with format `x-singer.decimal` (the singer-sdk's `use_singer_decimal`) is DECIMAL(precision, scale) when both `precision` and `scale` are integers (precision at least 1, scale from 0 to precision), with a precision above 38 capped as for a number. Otherwise, including a precision without a scale, it is VARCHAR, keeping the decimal's text digit for digit (with a warning naming the property), as target-iceberg keeps it as a string: no DuckDB DECIMAL holds an unsized decimal, a bare DECIMAL would be DECIMAL(18, 3), rounding to 3 places, and DECIMAL(p) is scale 0, rounding every fraction. A bare `decimal`/`numeric` `x-sql-datatype` doesn't size it; a sized `decimal(p,s)` or a non-decimal hint such as `double` decides the column. A property declaring several non-null types, such as `["integer", "string"]`, is VARCHAR, and each value is stored as its text (`5`, `1.50`, `true`). Values are staged at their column's own type, so any value a column can hold loads, and one it can't hold fails the batch instead of being changed. One it would round is rounded with a warning, or fails under strict `value_type_checking` (see [Values a column would round](#values-a-column-would-round)).

An `x-sql-datatype` annotation (as database taps emit, e.g. `smallint`, `numeric(12,4)`, `timestamptz`, `uuid`, `jsonb`) sets the column type instead, when the property's values can be loaded into it:

- a `string` property takes any supported hint, so `"12.5"` loads into a `numeric(12,4)` column;
- an `integer`, `number` or `boolean` property takes numeric hints and `json`/`jsonb`;
- an `object` or `array` property takes only `json`/`jsonb`.

Any other hint, such as `uuid` on an integer, is ignored with a warning, and the JSON type decides the column. Unsupported hints are ignored with the SDK's warning.

Supported hints (case and spacing ignored):

| Hint | Column type |
| --- | --- |
| `smallint`, `int2` / `integer`, `int`, `int4`, `mediumint` / `bigint`, `int8` | SMALLINT / INTEGER / BIGINT |
| MySQL `int(11)`, `bigint(20)` (display width ignored) | as without the width |
| `smallint unsigned`, `mediumint unsigned` / `int unsigned` / `bigint unsigned` | INTEGER / BIGINT / DECIMAL(20, 0) |
| `real`, `float4`, `float(p≤24)` / `float`, `float8`, `double`, `double precision`, `float(p>24)` (MySQL's `unsigned` ignored) | REAL / DOUBLE |
| `decimal(p,s)`, `numeric(p,s)`; `decimal(p)` (MySQL's `unsigned` ignored) | DECIMAL(p, s), capped at 38 digits; DECIMAL(p, 0) |
| `decimal`, `numeric` (no size) | DECIMAL(38, 9), or the property's `precision`/`scale` (so at most 29 integer digits) |
| `timestamp`, `timestamp without time zone`, `timestamp_ntz`, `datetime2`, `smalldatetime` | TIMESTAMP |
| `timestamptz`, `timestamp with time zone`, `timestamp_tz`, `timestamp_ltz`, `datetimeoffset` | TIMESTAMPTZ |
| `date` / `time`, `time without time zone` | DATE / TIME |
| any of the timestamp and time hints with a precision, e.g. `timestamp(3) with time zone` | as without it: microseconds are kept, so `datetime2(7)`'s 100 ns digit is dropped |
| `uuid`, `uniqueidentifier` / `json`, `jsonb` | UUID / JSON |

Hints naming the type a property gets anyway are accepted without a warning: a text type (`text`, `varchar(n)`, `varchar(max)`, `char(n)`, `nvarchar(n)`, `citext`) on a `string` property, and `boolean`/`bool` on a `boolean` property. On another JSON type they warn as unsupported. Deliberately unsupported, so they keep the JSON type's column: `tinyint` (MySQL uses `tinyint(1)` for booleans), `datetime` (MySQL's meaning is the opposite of ISO's), `timetz` and `time with time zone` (DuckLake's Parquet files drop the offset without converting it), and `bit`. A binary hint (`binary`, `varbinary(n)`, `blob`, `bytea`, `bytes`) alone keeps the text, with a warning naming the property: JSON carries no bytes, so only `contentEncoding: base64` says how to read them (see [Binary columns](#binary-columns)).

Existing tables keep their column types, apart from safe integer and float widenings. Values are staged at an existing column's own type where the stream's type would change them: a number column into an existing DECIMAL, REAL or DOUBLE column, a number into an existing integer column, a string property into an existing text column, whose text is kept as sent whatever its hint, and an `x-singer.decimal` string into an existing DECIMAL column. A table created before unbounded `x-singer.decimal` strings became VARCHAR has them as DECIMAL(18, 3), which DuckLake can't change to VARCHAR: extra fraction digits are rounded with a warning, and a value with more than 15 integer digits fails naming the column. Recreate the column to keep every digit. So a `double` hint on a column that already exists as DECIMAL(38, 9) still stores DECIMAL(38, 9) values: `1.5e-12` is stored as 0, and a warning reports how many values were rounded. Recreate the column to store doubles.

### Values a column would round

A number with more fraction digits than its DECIMAL column holds (1.231 in DECIMAL(10, 2), or 10 fraction digits in a new table's DECIMAL(38, 9)), or with a fraction for an integer column (1.5 in BIGINT), can only be stored rounded. `value_type_checking` decides what happens:

- **`permissive`** (the default) rounds it as DuckDB's cast of a DECIMAL would, half away from zero (1.225 to 1.23, 2.5 to 3, -2.5 to -3), and logs a warning counting the rounded values in each column of each batch. For a key column the warning says so: records whose keys round to the same value are merged into one row, by the merge and by the in-batch dedup every load method runs: the later record is kept and the earlier one is lost. If a source's keys may carry more fraction digits than the key column, keep it on `strict`. Values are rounded as the exact decimals the JSON held, whatever the property's hint: a `double`-hinted 2.5 for an integer column is 3, where DuckDB's cast of a DOUBLE, which rounds half to even, gave 2.
- **`strict`** fails the batch on the first such value, naming the column, the value and what it would become, and, for a key column, that records would merge. A float key with binary noise (`0.30000000000000004` for a DECIMAL(10, 2) column) fails too: send keys exact at the column's scale.

A number a numeric column can't hold at all fails the batch in both modes, naming the column: one with more integer digits than the DECIMAL holds, one outside BIGINT's range, NaN or infinity for a DECIMAL or integer column, or text that isn't a number. DuckDB's cast fails on these too; no rounding stores them, and the target won't invent a value such as NULL. Other values (booleans, dates and times) are left to pyarrow and DuckDB, whose errors on a bad one are their own. Narrowing DuckDB does silently, such as cutting a timestamp to microseconds or a DOUBLE to a REAL's precision, isn't inspected and isn't covered by the setting. `validate_records` (the singer-sdk's JSON Schema validation) is a separate setting and is off by default.

One way to use it: run `strict` while onboarding a source, until syncs run clean, then switch to `permissive` and watch the warnings. To load such values exactly instead, give the column enough fraction digits (recreate an existing one, or declare the property's `precision` and `scale` for a new table), or a DECIMAL type instead of an integer.

Why the target looks at values at all, rather than leaving them to DuckDB: values are staged in a Parquet file at the column's own type (staging at the stream's type and casting afterwards rounded twice: a DECIMAL(38, 30) column staged at the default (38, 9) lost 21 fraction digits). pyarrow, which writes that file, refuses a decimal with too many fraction digits ("Rescaling Decimal128 value would cause data loss") and truncates a fraction for an integer (1.5 to 1). So the target rounds those values itself, the way DuckDB's cast of a DECIMAL would, to let them load, and says how many it rounded, which DuckDB's cast doesn't. For a value DuckDB would refuse anyway, it fails with the column's name, which DuckDB's error omits. Strict is the only place it fails a value DuckDB would have loaded.

### Deletes from CDC taps

A log-based CDC tap reports a deleted row as an ordinary record carrying its key and `_sdc_deleted_at` set to when it was deleted. The tap declares `_sdc_deleted_at` in its schema, so the column is kept, as target-iceberg keeps it, even without `add_record_metadata`. The singer-sdk removes its own `_sdc_*` columns when `add_record_metadata` is off, and this one used to be removed too, losing every delete. A record without the column is a live row, so loading it clears an earlier mark.

A record is marked when its `_sdc_deleted_at` holds any non-null value. By default (`delete_rows_with_deleted_at: false`) a marked record is loaded like any other: the row stays, with `_sdc_deleted_at` set, and the target logs once per stream that deletes are recorded, not applied. An existing table gains the column, with NULL in the rows already loaded. With `delete_rows_with_deleted_at: true` the row is deleted instead, in the same transaction as the rest of its batch:

- **merge:** each record's key replaces its row as before, and a marked record's row isn't inserted again. Single and composite keys work.
- **append:** every row with a marked record's key is deleted, and the other records are appended.
- **overwrite:** marked records are left out of the new table. With key properties, each key's last record across all of the stream's batches decides, so a delete in a later batch removes a row an earlier batch loaded. No key is needed, but without one the target can't tell which row a delete is for: only the marked record itself is left out, and an earlier live record of the same row stays. Declare key properties to have deletes cancel earlier records.
- **Within a batch** the last record of a key decides: an update followed by a delete deletes the row, and a delete followed by a re-insert keeps the new row.
- **Refused at startup** for a merge or append stream without key properties, since it can't tell which row a delete is for.
- **A stream without the column** (not declared, and `add_record_metadata` off) has nothing marked: a warning says so, and it loads as before.

This is separate from `hard_delete`, which only decides what `ACTIVATE_VERSION` does with rows missing from a full-table sync (see below).

### Overwrite and ACTIVATE_VERSION

**Overwrite** (`load_method: overwrite`, or `overwrite_if_no_pk` for a stream without primary keys) replaces the table's rows atomically. Each batch is staged as a Parquet file in `temp_file_dir`, and at the end of the stream one transaction deletes the old rows and inserts every staged batch. Readers see the old table until that commit. A run that fails or is interrupted in the target leaves the old rows in place. Some limits:
- **STATE is held** while any overwrite stream hasn't committed: mid-run drains don't emit it, so a resumable tap can't skip rows that were never loaded. It is emitted after the commit. STATE is one message for all streams, so other streams' checkpoints wait too. After a crash during a long overwrite, they resume from their last emitted STATE.
- **The swap happens at the end of the input.** If the tap dies but its output ends cleanly, the target swaps in what it received.
- **`temp_file_dir` needs room for the whole stream,** not one batch.
- **A stream with no records still empties the table,** as before.
- **A schema change mid-stream** (a new SCHEMA for the stream) fails the run at the end, with the old rows intact.

**ACTIVATE_VERSION**, as full-table taps send it, retires rows that are gone at the source. The tap tags each record with a table version, then sends `ACTIVATE_VERSION` for that version. The target first loads any buffered records, then handles every row whose `_sdc_table_version` is not the activated version, including rows loaded without a version:
- **By default,** as in the Singer SDK, it sets their `_sdc_deleted_at` to the current UTC time and keeps them. A row the source sends again is revived when it is next loaded.
- **With `hard_delete: true`,** it deletes them.

Some cases retire nothing:
- **Without `add_record_metadata`:** records carry no version, so the message is ignored with a warning.
- **A version announced before any of its records** (as taps do at the start of a sync) is a no-op, so the table never empties mid-sync. A second `ACTIVATE_VERSION` for the same version, even with no records between, ends the sync: the source is empty, and every row is retired.
- **An overwrite stream** ignores versions, since it replaces the whole table anyway.

Between a sync's first batch and its `ACTIVATE_VERSION`, readers see the new rows next to rows of the old version. Set `process_activate_version_messages: false` to ignore the message entirely.

### Ducklake Schema Migration From 0.1 to 0.2
Solution to this issue: https://github.com/duckdb/ducklake/issues/240
The SQL to manually migrate your catalog schema (assuming Postgres) can be found in the `schema_migration.sql` file.

### Configure using environment variables

This Singer target will automatically import any environment variables within the working directory's
`.env` if the `--config=ENV` is provided, such that config values will be considered if a matching
environment variable is set either in the terminal context or in the `.env` file.

### Authentication and Authorization

<!--
Developer TODO: If your target requires special access on the destination system, or any special authentication requirements, provide those here.
-->

## Usage

You can easily run `target-ducklake` by itself or in a pipeline using [Meltano](https://meltano.com/).

### Executing the Target Directly

```bash
target-ducklake --version
target-ducklake --help
# Test using the "Smoke Test" tap:
tap-smoke-test | target-ducklake --config /path/to/target-ducklake-config.json
```

## Developer Resources

Follow these instructions to contribute to this project.

### Initialize your Development Environment

Prerequisites:

- Python 3.9+
- [uv](https://docs.astral.sh/uv/)

```bash
uv sync
```

### Create and Run Tests

Create tests within the `tests` subfolder and
then run:

```bash
# Run all unit tests (no database required)
uv run pytest tests/ -v
```

You can also test the `target-ducklake` CLI interface directly using `uv run`:

```bash
uv run target-ducklake --help
```

### Testing with [Meltano](https://meltano.com/)

_**Note:** This target will work in any Singer environment and does not require Meltano.
Examples here are for convenience and to streamline end-to-end orchestration scenarios._

<!--
Developer TODO:
Your project comes with a custom `meltano.yml` project file already created. Open the `meltano.yml` and follow any "TODO" items listed in
the file.
-->

Next, install Meltano (if you haven't already) and any needed plugins:

```bash
# Install meltano
pipx install meltano
# Initialize meltano within this directory
cd target-ducklake
meltano install
```

Now you can test and orchestrate using Meltano:

```bash
# Test invocation:
meltano invoke target-ducklake --version

# OR run a test ELT pipeline with the Smoke Test sample tap:
meltano run tap-smoke-test target-ducklake
```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the Meltano Singer SDK to
develop your own Singer taps and targets.
