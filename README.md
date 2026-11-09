# target-ducklake

`target-ducklake` is a Singer target for ducklake. This target is in development and may not be stable. I built this with GCS in mind, but it should work with S3 and local storage, though this has not been tested extensively.

Supports append, merge, and overwrite load methods (default is merge). The load method can be configured using the `load_method` setting, for every stream or per stream (see [Load method and keys per stream](#load-method-and-keys-per-stream)). If no key properties are provided and `overwrite_if_no_pk` is true, data will be overwritten. Otherwise, the configured load method determines the behavior.

## Configuration

### Accepted Config Options

| Setting | Type | Required | Default | Description |
|---------|------|----------|---------|-------------|
| `catalog_url` | string | ✅ | - | Connection string for the catalog database, as DuckLake's `ATTACH` takes it: libpq keywords for postgres (`dbname=... host=... user=... password=...`), the [MySQL extension's](https://duckdb.org/docs/stable/core_extensions/mysql) keywords for mysql (`database=... host=... user=... password=...`), a file path for sqlite and duckdb. Secret, since it can hold the catalog password. |
| `data_path` | string | ✅ | - | GCS, S3, or local folder path for data storage |
| `storage_type` | string | ✅ | `"local"` | Type of storage: GCS, S3, or local |
| `catalog_type` | string | ✅ | `"postgres"` | Type of catalog database: postgres, sqlite, mysql, or duckdb. See <https://ducklake.select/docs/stable/duckdb/usage/choosing_a_catalog_database> |
| `meta_schema` | string | ❌ | - | Schema name in the catalog database to use for Ducklake metadata tables. If not provided will use the default schema for the catalog database (eg `public` for Postgres) |
| `public_key` | string | ❌ | - | Access key ID for GCS or S3: the S3 access key id, or the GCS HMAC key id. Set with `secret_key`, or neither to use the host's own credentials. Secret. |
| `secret_key` | string | ❌ | - | Secret for `public_key`: the S3 secret access key, or the GCS HMAC secret. Secret. |
| `region` | string | ❌ | - | AWS region for S3 storage type (required when using S3 with explicit credentials) |
| `default_target_schema` | string | ❌ | - | Default database schema where data should be written. If not provided schema will attempt to be inferred from the stream name (inferring schema only works for database extractors ) |
| `target_schema_prefix` | string | ❌ | - | Prefix to add to the target schema name. If not provided, no prefix will be added |
| `add_record_metadata` | boolean | ❌ | `false` | When True, automatically adds Singer Data Capture (SDC) metadata columns to target tables |
| `hard_delete` | boolean | ❌ | `false` | When True, `ACTIVATE_VERSION` deletes rows absent from the activated version; when False, it sets their `_sdc_deleted_at`. Needs `add_record_metadata`: without it, `ACTIVATE_VERSION` is ignored with a warning. See [Overwrite and ACTIVATE_VERSION](#overwrite-and-activate_version). |
| `delete_rows_with_deleted_at` | boolean | ❌ | `false` | When True, a record whose `_sdc_deleted_at` is set (a log-based CDC tap's delete) deletes its key's row, in the same transaction as its batch, instead of being loaded as a live row. Needs key properties, except with `load_method: overwrite`. Also settable per stream in `stream_defaults`/`stream_overrides`. See [Deletes from CDC taps](#deletes-from-cdc-taps) |
| `process_activate_version_messages` | boolean | ❌ | `true` | When False, `ACTIVATE_VERSION` messages are ignored. When True they retire rows of other table versions, which needs `add_record_metadata` (without it they are ignored with a warning). |
| `load_method` | string | ❌ | `"merge"` | Method to use for loading data into the target table: append, merge, or overwrite. Also settable per stream in `stream_defaults`/`stream_overrides`, as can the stream's `key_properties`. See [Load method and keys per stream](#load-method-and-keys-per-stream) and [Overwrite and ACTIVATE_VERSION](#overwrite-and-activate_version). |
| `flatten_max_level` | integer | ❌ | `0` | Maximum depth for flattening nested fields. Set to 0 to disable flattening. Objects deeper than this are STRUCT columns (see `nested_types`) |
| `nested_types` | string | ❌ | `native` | How an object with declared `properties` and an array with typed `items` are loaded: `native` creates STRUCT and LIST columns, as target-iceberg does; `json` stores them as JSON text, as before. Applies to new tables and new columns. Also settable per stream in `stream_defaults`/`stream_overrides`. See [Nested columns](#nested-columns) |
| `temp_file_dir` | string | ❌ | `"temp_files/"` | Directory path for storing temporary parquet files |
| `batch_config` | object | ❌ | — | The singer-sdk's BATCH settings. The target reads `storage.params` (fsspec options such as credentials) to open BATCH files. See [BATCH messages](#batch-messages) |
| `batch_size_rows` | integer | ❌ | `10000` | Records per batch: each batch is staged as one Parquet file and loaded in one transaction. The singer-sdk's standard setting, as in target-iceberg. |
| `max_batch_size` | integer | ❌ | — | **Deprecated:** use `batch_size_rows`. Used only when `batch_size_rows` is not set (a warning says so); when both are set, `batch_size_rows` wins and a warning names both. Set it in the config, not as `TARGET_DUCKLAKE_MAX_BATCH_SIZE`: the singer-sdk's environment parsing can't type it (use `TARGET_DUCKLAKE_BATCH_SIZE_ROWS`). |
| `partition_fields` | object | ❌ | - | Object mapping stream names to arrays of partition column definitions. Each stream key maps directly to an array of column definitions |
| `auto_cast_timestamps` | boolean | ❌ | `false` | When True, automatically attempts to cast timestamp-like fields to timestamp types in ducklake |
| `convert_tz_to_utc` | boolean | ❌ | `false` | When True, converts timezone-aware date-time values to UTC before loading, so a timestamp column without time zone stores the UTC time. When False, such a column stores each value's local wall-clock time (its offset is dropped). With the default, a timestamp column fed values in different offsets holds local times that aren't directly comparable; turn this on, or declare the column `timestamptz` (via `x-sql-datatype`), to compare instants. Either way, a `timestamptz` column stores the correct instant, and values with any offset, such as `+05:30`, load. Values without a time zone are loaded as given into a `timestamp` column; a `timestamptz` column reads them as UTC with `date_time_type: timestamptz` (the default), and in the DuckDB session's time zone with `timestamp`. |
| `date_time_type` | string | ❌ | `timestamptz` | The column type of a `format: date-time` property without an `x-sql-datatype` hint. `timestamptz` loads instants, as target-iceberg does: a value with an offset is stored as its UTC instant, and one without an offset is read as UTC. `timestamp` loads wall-clock times (see `convert_tz_to_utc`). Also settable per stream in `stream_defaults`/`stream_overrides`. See [Date-time columns](#date-time-columns) |
| `data_inlining_row_limit` | integer | ❌ | `0` | An insert of at most this many rows is stored in the catalog (DuckLake data inlining) instead of a new Parquet file. `0` turns inlining off. Also settable per stream in `stream_defaults`/`stream_overrides`. See [Data inlining](#data-inlining) |
| `inlined_data_flush_rows` | integer | ❌ | `10000` | With inlining on, a stream ends by writing its table's inlined rows to Parquet once they reach this many; `0` never flushes. Also settable per stream. See [Data inlining](#data-inlining) |
| `validate_records` | boolean | ❌ | `false` | Whether to validate the schema of the incoming streams |
| `value_type_checking` | string | ❌ | `permissive` | What to do with a number its column would round: `permissive` rounds it as DuckDB's cast of a DECIMAL does, with a warning; `strict` fails the batch naming the column and the value. See [Values a column would round](#values-a-column-would-round) |
| `type_source` | object | ❌ | — | Per stream only, in `stream_defaults`/`stream_overrides`: a data dictionary (column → type) that types an all-string stream, such as a CSV tap's, as target-iceberg does. Off unless set. See [Typing all-string streams](#typing-all-string-streams-type_source) |
| `on_error` | string | ❌ | `raise` | Per stream only, with `type_source`: what a value that isn't its declared type becomes. `raise` fails the run naming the stream, column and value; `null` stores NULL; `string` keeps the text, and the typed columns are VARCHAR. See [Typing all-string streams](#typing-all-string-streams-type_source) |
| `overwrite_if_no_pk` | boolean | ❌ | `false` | When True, a stream without primary keys replaces the target table's rows, as `load_method: overwrite` does. Overrides `load_method`, except `append` set in the stream's own `stream_overrides` entry. |
| `conform_column_names` | boolean | ❌ | `false` | When True, column names are conformed as the singer-sdk's SQL targets and target-iceberg conform them (`First Name` → `first_name`). Off by default because it renames the columns of existing tables. Also settable per stream in `stream_defaults`/`stream_overrides`. See [Conformed column names](#conformed-column-names) |
| `max_column_length` | integer | ❌ | `63` | Maximum length for column names. Only applied when `catalog_type` is `postgres`. Names exceeding this limit are truncated. Colliding truncated names get a `_{i}` suffix. Set to `0` to disable. See [ducklake#619](https://github.com/duckdb/ducklake/issues/619). |
| `parallel_draining` | boolean | ❌ | `true` | When True, enables parallel draining with `max_parallelism=10` (Meltano default). When False, sets `max_parallelism=1` to disable parallel draining. Ignored when `catalog_type` is `sqlite`: SQLite allows one writer at a time, so its sinks are always drained one at a time (a log line says so). |
| `max_record_age_minutes` | number | ❌ | `10.0` | Maximum age in minutes that records can sit in a sink before a drain is forced. Tune lower for more frequent flushes, higher for larger batches. |
| `nats` | object | ❌ | — | Publish one `writeCompleted` event per table after a completed run, over NATS: `servers` (comma-separated URLs) and `subject` turn it on; also `jetstream` (`auto`, `true`, `false`), `username`/`password`, `token`, `creds_file`, `tls`. The same keys as target-iceberg. Needs the `nats` extra. See [writeCompleted events](#writecompleted-events) |
| `warehouse` | string | ❌ | `data_path` | The warehouse name `writeCompleted` events carry (target-iceberg's setting). Defaults to `data_path` without a trailing slash. |

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
        batch_size_rows: 10000 # Optional (default 10000)
        add_record_metadata: true # Optional
        load_method: merge # Optional (default merge, options: append, merge, overwrite)
        auto_cast_timestamps: true # Optional
        overwrite_if_no_pk: true # Optional (default false)
        partition_fields: {"my_stream": [{"column_name": "created_at", "type": "timestamp", "granularity": ["year", "month"]}]} # Optional
```

For the full loader declaration (every setting, with its kind and whether it is sensitive), copy [plugin-declaration.yml](plugin-declaration.yml); see below.

### xflow admin UI and Meltano plugin declaration

Two generated files describe the settings to tools that configure the target:

- **`spec.json`**: the destination spec the xflow admin UI builds its form from (Airbyte's `ConnectorSpecification` shape: `documentationUrl`, `supportsDbt`, `supportedDestinationSyncModes` and a draft-07 `connectionSpecification`). It is generated from `config_jsonschema` in `target_ducklake/target.py`, so each field's title, description, type, enum, default and `airbyte_secret` flag are the setting's own. `target_ducklake/admin_spec.py` holds what only the form needs: the settings it offers and their order, the required ones, why every other setting is left out (a new setting fails the tests until it is offered or left out with a reason), and the sync modes the load methods give: `append` → `append`, `merge` → `append_dedup`, `overwrite` → `overwrite`. The form offers the catalog, storage and schema settings, `load_method` and `batch_size_rows`; per-pipeline settings (`stream_defaults`, `nats`, ...) are left to an engineer.
- **`plugin-declaration.yml`**: the declaration a Meltano project copies under `plugins.loaders`, since target-ducklake is not on Meltano Hub. It is `meltano.yml`'s `target-ducklake` loader with an installable `pip_url` (`target-ducklake[nats]@git+https://github.com/paulcaron16k/target-ducklake-definite.git@<tag>`: replace `<tag>` with a release tag). Use it only with the first release that contains it, or a later one: earlier tags (v0.0.2) have neither the `nats` extra nor several of the declared settings. Nested settings keep their dotted names (`nats.servers`), and free-form objects (`stream_defaults`, `stream_overrides`, `batch_config`) are one setting of kind `object`.

Credentials (`catalog_url`, `public_key`, `secret_key`, `nats.password`, `nats.token`) are secret on every surface: `secret` in `config_jsonschema`, `airbyte_secret` in `spec.json`, `sensitive: true` and `kind: password` in `meltano.yml` and the declaration.

The declared capabilities are the target's, as far as Meltano names them: `about`, `stream-maps`, `schema-flattening`, `activate-version`, `hard-delete`, `batch` and `soft-delete` (by default `ACTIVATE_VERSION` marks retired rows with `_sdc_deleted_at`). `activate-version`, `hard-delete` and `soft-delete` need `add_record_metadata`: without it records carry no table version, and `ACTIVATE_VERSION` is ignored with a warning.

`spec.json`'s `supportsDbt` is `true`, the same as target-iceberg's, so a DuckLake destination is offered the same pipeline options as an Iceberg one; it doesn't mean the target runs dbt. The form shows `batch_size_rows`'s default (10000) without declaring it in `config_jsonschema`, where it would always win over the deprecated `max_batch_size`.

To change a setting, edit `config_jsonschema` and `meltano.yml`, then regenerate both files:

```bash
uv run bin/render-admin-ui.py          # write spec.json and plugin-declaration.yml
uv run bin/render-admin-ui.py --check  # exit 1 if either is stale
```

A local pre-commit hook (`render-admin-ui`) runs the `--check` when `meltano.yml`, a generated file or the target's code changes.

`tests/test_admin_ui.py` fails while either file is stale, and while `meltano.yml` and `config_jsonschema` disagree on the settings, their kinds, options, defaults or secret flags. It also validates `meltano.yml` and the declaration against Meltano's own schema, vendored in `tests/contracts/meltano.schema.json` at a pinned Meltano release; to move to a newer one, bump `REF` in `bin/refresh-meltano-schema-contract.sh` and run it.

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

### JSON text in object and array properties

Some taps send an object or array as its JSON text: the string `"[1,2]"` in a property their schema declares an `array`. One rule decides what such a string loads as, wherever the property's column is: a top-level column, a flattened `parent__child` column (`flatten_max_level`), a column whose name was conformed (`conform_column_names`) or truncated (`max_column_length`, Postgres catalogs), a STRUCT field or list element (`nested_types: native`), or a nested property or array item inside a JSON column.

- **A property declared an object or an array, that can't be a string**: a string that is the JSON text of the declared container (an array's text for an `array`, an object's for an `object`) loads as that array or object, exactly as if the tap had sent it as one. In a JSON column `"[1,2]"` is stored as `[1, 2]`, the same JSON as the list `[1, 2]`; in a LIST or STRUCT column it is the list or struct; an object sent as text and flattened (`flatten_max_level`) fills its `parent__child` columns. The decoded value is written as the target writes any value: numbers with a fraction keep all their digits (`[1.10]` stays `[1.10]`) but take the writer's form (`1.0e-5` is `0.000010`), and of a repeated object key only the last is kept (`{"a":1,"a":2}` is `{"a": 2}`).
- **Any other string in that property** (text that isn't JSON, such as `abc`, `NaN` or `[1,`; the JSON text of a string, number, boolean or null, such as `"3"` or `"null"`; or the other container's text, such as `{"a":1}` in an `array`) is stored in a JSON column as a JSON string: `abc` is `"abc"`, `3` is `"3"`, and `null` is `"null"`, not NULL. A LIST or STRUCT column can't hold a string, and fails the batch naming the column, as before. An object property that is flattened has no column of its own, so such a string leaves all its `parent__child` columns NULL, as before.
- **A property that may be a string** holds a string as a string: its JSON column stores `"[1,2]"` as the JSON string `"[1,2]"`. That is a property whose `type` includes `string`, whose `enum` or `const` has a string value, or that says nothing of its type (no `type`, `{}`, or a `$ref`, which the target doesn't resolve). Without `type`, the types are read from `anyOf`/`oneOf` (any branch) and `allOf` (every branch that declares one), at any depth.

`validate_records` checks the record as the tap sent it, so it still refuses a string in an `array` property. A `type_source` that declares the property a string makes it a string property.

**Existing tables.** Before DUCKLKD-123 the result depended on where the column was. Rows loaded from now on can differ from the rows already in a table:

- **A key property declared an object or an array** (a JSON key column): a key sent as text was stored as a JSON string (`"[1,2]"`) and is now the array (`[1, 2]`), so a merge no longer matches the rows loaded before, and loads those records again beside them. Before loading such a stream, fully refresh its table, or rewrite those keys to the values they encode. The target warns at startup when such a key's JSON column holds a JSON string that is the declared container's text (a query that stops at the first one, run only for these streams), so a table loaded only by this version isn't warned about. Within a batch, a key sent as text and the same key sent as a list are now one key, and deduplicated.
- **A top-level JSON column** (the common case: an `object` without `properties`, an `array` without typed `items`, or any object or array with `nested_types: json`) stored every string as a JSON string. Strings that are the JSON text of the declared container now load as that container (`"[1,2]"` was `"[1,2]"`, is now `[1, 2]`); every other string loads as before.
- **A flattened column, or a top-level column with a truncated name** on a Postgres catalog, mostly loaded the text as JSON as it was. The JSON text of the declared container still loads as that value, now written as the target writes a list or dict (`[1,2]` is now stored as `[1, 2]`, which compares equal as JSON). Every other string is now a JSON string: the JSON text of a scalar (`3` was `3`, is now `"3"`; `null` was JSON null, is now `"null"`) or of the other container (`{"a":1}` in an `array` was `{"a":1}`, is now `"{\"a\":1}"`), `NaN` (stored as is, now `"NaN"`), and text that isn't JSON, which failed the batch (`abc`, an empty string, `[1,`).
- **An object property sent as text and flattened**: its `parent__child` columns were all NULL, and now hold the object's fields. With `nested_types: native` the same text was refused (`holds objects`), and now loads as the struct.
- **Inside a JSON column**: JSON text in a nested property or array item declared an object or array, which was stored as a JSON string, now loads as its object or array (`{"lst": "[1,2]"}` is now `{"lst": [1, 2]}`). The same goes for a JSON field in a STRUCT or LIST (`nested_types: native`).
- **A LIST or STRUCT column** refused any string; the JSON text of a list (for a LIST) or an object (for a STRUCT) now loads as that list or struct.
- **A JSON field in a STRUCT** declared an `object` and given an array's text (or declared an `array` and given an object's text) stays a JSON string (`"[1,2]"`), as before.
- **A property that may be a string** loads as before in a top-level column with an untruncated name. In a flattened column, or under a truncated name, its strings are now JSON strings, like every other string there (`[1,2]` was `[1,2]`, is now `"[1,2]"`).

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
- **`key_properties`:** a list of the stream's top-level property names (not flattened `a__b` names: the singer-sdk checks keys on the record as the tap sends it). It replaces the keys the tap declares, for a tap that declares none or the wrong ones, and is used for everything that needs keys: merge, dedup within a batch, `delete_rows_with_deleted_at` and `overwrite_if_no_pk` (a stream given keys is no longer keyless). With `conform_column_names` on, a key may also be given by its conformed name (`order_id` for `Order ID`). A name the stream doesn't have fails the stream when its schema arrives, so `key_properties` in `stream_defaults` suits only taps whose streams all have those properties.
- A stream without keys can't merge, so `merge` appends, as before.
- **`overwrite_if_no_pk`** still overrides `load_method` for keyless streams, including one from `stream_defaults`, except `append` (or `write_mode: append-only`) in the stream's own `stream_overrides` entry: a keyless events stream can keep appending.

### Streams that load one table

A stream's table is named after the stream (or its stream map `__alias__`), in `default_target_schema` or, without it, in the schema a `schema-table` stream name gives. So two streams can load one table: two streams aliased to one name, one aliased to another's name, names that differ only in ASCII case (DuckDB identifiers ignore it: `Orders` and `orders`), `public-orders` and `orders` under `default_target_schema: public`, or `a-public-orders` and `b-public-orders` (a three-part name's first part is dropped). The singer-sdk doesn't notice, so before DUCKLKD-44 the streams silently re-keyed or replaced each other's rows. Now, as in target-iceberg, the target checks each stream when its SCHEMA arrives:

- **A warning, once per pair of streams,** when they agree: their rows are combined in the table. This is how two streams are meant to be loaded into one table, with stream maps. One may append and the other merge.
- **The run fails** naming both streams, the table and the conflict, before the second stream changes the table, when:
  - **their key properties differ**, compared as sets of the key columns' final names (after `conform_column_names` and Postgres truncation, ignoring ASCII case). A stream without keys differs from one with keys. Each stream would merge, deduplicate and delete by columns that don't identify the other's rows;
  - **either does a full refresh**: `load_method: overwrite`, `overwrite_if_no_pk` for a keyless stream, or an `ACTIVATE_VERSION` that retires rows (with `add_record_metadata`, whether rows are marked deleted or removed with `hard_delete`). It would replace the other stream's rows. A full refresh stays claimed for the rest of the run, even if the stream's SCHEMA is sent again without one: its old sink still commits the replace.
- **Different table settings are named in the warning,** not refused, so a config shared with target-iceberg (which doesn't check them) keeps working: partition keys or sort orders both streams configure, and `data_inlining_row_limit` both set in `stream_overrides` or `stream_defaults`. A table has one of each, and each stream's setup sets it to its own when it differs. The table follows the stream set up last, its metadata (a new partition spec or sort version, or an inlining change and flush) changes every run, and files follow whichever setting was current when written. No row is lost. A stream that doesn't set one keeps the table's.
- **A stream mapped to null** (`stream_maps: {orders: null}`, or `__else__: null`) is removed: it gets no sink, so it loads no rows and claims nothing, and a retired stream doesn't hold the table its replacement is aliased to. Its table is left alone: not created, not overwritten (`load_method: overwrite`, `overwrite_if_no_pk`), and its `ACTIVATE_VERSION` retires nothing; it holds back no STATE and gets no writeCompleted event (DUCKLKD-127). A `__source__` copy of it still loads. A stream map filtering every record out (`__filter__: false`) is not a removal: the stream loads no rows, so an overwrite empties its table.
- **Two streams aliased to one name with different schemas** share one sink, which the singer-sdk rebuilds whenever either sends its SCHEMA; the warning says so.
- **`type_source`** may differ: each stream's types are fitted to the table's columns, as for a table an earlier run created.
- **Names that differ only in case** load one table with one set of columns, on every catalog. The table and its schema keep the spelling they were created with; a stream `orders` (or a `default_target_schema` of `S`) finds the table `Orders` (in the schema `s`), adds the columns it declares and the table lacks, and loads their values. The same holds for a table an earlier run created in another case, and for its partition keys, sort order, `data_inlining_row_limit`, inlined rows and writeCompleted event. Before DUCKLKD-135 such a stream found no table, so it added none of its new columns and their values were dropped silently. A property that differs from a column only in case (`OrderID` and `orderid`) loads into that column.
- **Drains:** with `parallel_draining`, sinks of one table drain one after another, in the order they were set up (a sink rebuilt for a new SCHEMA goes last), and a failure stops that table's later sinks, as on the serial path. Other tables' sinks drain in parallel. Two merges committed at once would each miss the other's rows and load a key twice. A key both streams send keeps the row of the batch loaded last.
- **When a collision can be seen:** a Singer target learns of a stream only when its SCHEMA arrives, and an overwrite replaces the table only at the end of the run, so a collision with it fails the run first (unless a changed SCHEMA rebuilt the overwrite's sink, which commits its replace at the next drain). An `ACTIVATE_VERSION` that retires rows before the other stream's SCHEMA arrives has retired them by the time that stream fails the run.

Fix a collision by giving one stream its own table with a stream map (`stream_maps: {orders_eu: {__alias__: orders_eu_raw}}`), or by making the streams agree.

### Names with quotes, spaces or dots

Schema, table and column names are used as they are: a stream named `it's`, `say "hi"` or `a.b` loads a table of that name, and a property named `it's "v"` a column of that name. Every name is quoted in the SQL the target writes (a double quote doubled), and every name in a string literal is escaped or bound as a parameter, as are the staging file paths, which hold the stream name. In `sort_order`'s text form a column name can be quoted for a space (`'"First Name" DESC'`), but can't hold a double quote.

- **`-` still splits a stream name** into schema and table (`public-orders`, see `default_target_schema`). A stream whose schema or table part is empty (`orders-`, `-orders` without `default_target_schema`, `db--orders`) fails, naming the stream; rename it with a stream map `__alias__`.
- **A `/` in a stream name** is kept in the table name; the staging directory and files replace it with `_`.
- **Settings are taken literally too.** `catalog_url`, `data_path`, `meta_schema`, `public_key`, `secret_key` and `region` are each written into the SQL as one escaped string. A libpq value quoted in `catalog_url` is written as libpq takes it: `password='my pw'`. Before DUCKLKD-128 they were pasted in unescaped, so a quote had to be doubled in the config (`password=''my pw''`); remove that doubling when upgrading, or libpq gets the quotes doubled.

### Typing all-string streams (type_source)

A file or CSV tap sends every value as a string, so every column of its tables is VARCHAR. `type_source` says what the columns really are: a per-stream data dictionary, set in `stream_defaults` or `stream_overrides` with the same keys and meaning as in target-iceberg, so one config types a stream alike in both targets. Nothing is typed without it; the target never guesses a type from the values.

```yaml
stream_overrides:
  orders:
    type_source:
      order_id: bigint
      total: money
      paid: bool
      ordered_on: {type: date, x-parse-format: "%d/%m/%Y"}
      shipped_at: timestamptz
    on_error: "null"
```

- **Types:** `integer`, `number`, `boolean`, `date`, `time`, `date-time` and `string`, matched case-insensitively, and the SQL names target-iceberg reads for them: `int`, `int32`, `int64`, `bigint`, `smallint` (integer); `float`, `double`, `real`, `numeric`, `decimal`, `money`, `currency` (number); `bool`; `datetime`, `timestamp`, `timestamptz` (date-time); `str`, `text`, `varchar`, `json` (string). An entry is a type name or an object with `type` and, for dates and times, `x-parse-format` (a Python `strptime` pattern such as `%d/%m/%Y`); other keys of the object are ignored, so a dictionary exported with descriptions works. A type name the target doesn't know (`uuid`, a typo) is read as `string`, as target-iceberg reads it; a warning at startup names the stream, the column, the unknown name and the names it knows.
- **Columns:** each named column is declared its type before the stream's columns are built, whatever the tap declares, so the table column is VARCHAR, BIGINT, DECIMAL(38, 9), BOOLEAN, DATE, TIME, or TIMESTAMPTZ (TIMESTAMP with `date_time_type: timestamp`), as for a typed tap (see [Column types](#column-types)). A `string` column's values are stored as their text (`5`, `true`, an object as JSON). Names are the stream's top-level property names (with `conform_column_names` on, their conformed names work too, and an entry types the conformed column). Columns the dictionary doesn't name are left alone; names the stream doesn't have are logged and skipped (a `type_source` in `stream_defaults` serves several streams). An override's `type_source` replaces the one in `stream_defaults` whole.
- **Values:** each value is converted before the record is validated, flattened and staged, so `validate_records` checks the converted value, and BATCH records, which take the RECORD path, are converted too. An empty or blank value is NULL, except in a key column (see **Key columns** below). An integer or number must be exactly a number once its noise is removed: surrounding spaces, one currency symbol (`$ € £ ¥ ₹`) or ISO 4217 currency code in upper case (`USD 100`, `100 EUR`) before or after it, and thousands commas in groups of three. Digits are ASCII (`١٢٣` isn't a number), and three capitals that aren't a currency code (`NOT 5`, `TAX 5`) aren't noise. So `$1,234.50` is 1234.50, `-$5` is -5 and `1,200` is 1200, but `(5)`, `1,5`, `1.234,50`, `12abc`, `12 34`, `0x10` and `1_000` aren't numbers, and `on_error` decides. (target-iceberg drops every character but digits, sign, point and `e`, so it would load those as 5, 15, 1.2345, 12, 1234, 10 and 1000.) A number must also fit its column: an integer within 64 bits, a number within DECIMAL(38, 9)'s 29 integer digits; one that doesn't isn't its type. A boolean is `true`, `t`, `yes`, `y` or `1`, or `false`, `f`, `no`, `n` or `0`, in any case. A date, time or date-time is read with its `x-parse-format`, else as ISO 8601 (`2026-03-04`, `2026-03-04 10:15`, `2026-03-04T10:15:00+02:00`, `...Z`); dates and date-times also read ISO 8601's basic format (`20240301`, `20240301T101500`) and week dates (`2024-W09-5`), which have one meaning each, but not a year, a month or a week without its day (`2024-W09`, also before a time), and only `T` or a space may separate a date from its time (`2024-03-01x10:00` is refused). Without `x-parse-format`, a time must be written with colons (`09:30`, `09:30:00Z`), since basic format would read `20240301` as 20:24:03.01, and may have a fraction only after its seconds (`10:15:30.5`; `10:15.5` is refused, as ISO 8601 reads it as 10:15:30 and Python as 10:15:00.5); it may be a date-time's time of day, but not a date alone. All digits are ASCII.
- **A value that converts only by dropping part of it**, a fraction for an integer (`7.9`), a tenth fraction digit for a number, a time of day for a date, or a seventh fraction digit of a second for a time or date-time (truncated to microseconds, as DuckDB's cast truncates it), follows `value_type_checking`: `permissive` (the default) drops it with a warning (a fraction toward zero for an integer, as target-iceberg does; a number's extra digits rounded half away from zero, as DuckDB's cast to DECIMAL rounds them); `strict` treats the value as not of its type, so `on_error` decides.
- **`on_error`** (per stream, default `raise`) decides what a value that isn't its type becomes:
  - `raise` fails the run with an error naming the stream, column and value, so the dictionary or the source can be corrected. Nothing of the failing batch is loaded.
  - `null` stores NULL.
  - `string` keeps the text the tap sent. Every typed column is then VARCHAR, whatever the tap declares, so that text always fits, and the values that convert are stored as normalised text (`1,200` as `1200`, `Yes` as `true`, `04/03/2026` as `2026-03-04`), so a later cast reads them all. A table that already has a typed column as another type (BIGINT, say) can't hold such text, so the stream is refused at its start, naming the column. target-iceberg types the column under `string` too and keeps the raw text in it, which the typed column can't hold.
  With `null` and `string`, the first such value of each column is logged with a warning, and the count per column when the stream ends, not a line per row.
- **Key columns** (the stream's key properties) never get a NULL from `type_source`. A NULL key matches every other NULL key, so with `null` distinct records would merge into one row, and a delete (`delete_rows_with_deleted_at`) would remove unrelated rows. A blank value, or one that isn't its type, in a key column fails the run under `raise` and under `null` alike, naming the stream, column and value; `string` keeps its text, which is a distinct key. A key value that converts only by dropping part of it (`7.5`, a date-time for a date key) is not its type either, whatever `value_type_checking` says, since `7.5` and `7.2` would both become key 7 and merge. This holds for a key whose name a Postgres catalog truncates to 63 characters too.
- **Existing tables:** a column the table already has as VARCHAR isn't retyped, since in place the target only widens integer and floating-point columns. Its values are still converted, so a bad one is handled by `on_error`, and then stored as the same normalised text `on_error: string` stores (`1200`, `1234.50`, `true`, `2026-03-04`); a warning at the start of the stream names the column. To type it, recreate the column or the table. A column added by the stream is created typed.

### Conformed column names

By default a column is named exactly as the tap names its property (only truncated on a Postgres catalog, see `max_column_length`). With `conform_column_names: true`, names are conformed by the rules of the singer-sdk's `SQLSink.conform_name`, which target-iceberg follows too, so one tap gives the same column names in both targets, and they can be queried without quotes:

- characters other than ASCII letters, digits, `_`, `-`, `.` and whitespace are dropped (`café` → `caf`);
- the name is lowercased and stripped, and `.`, `-` and spaces become `_` (`Created.At` → `created_at`, `e-mail` → `e_mail`);
- a leading digit becomes a letter, 0 → `a`, 1 → `b` and so on (`2fa` → `cfa`).

Dropping characters and replacing a digit lose information, so the target warns, once per stream, naming each name changed that way. Two properties that conform to the same name (`my.col` and `MY-COL`) fail the stream with the singer-sdk's `ConformedNameClashException`, and so does a name with nothing left (`日本語`). Reserved words such as `order` are not renamed, as in the singer-sdk; they still need quotes.

What is conformed:

- **Columns**, after flattening (`flatten_max_level`): `Home Address` → `home_address`, and the flattened field `Zip-Code` of `Billing` (`Billing__Zip-Code`) → `billing__zip_code`. The fields inside a STRUCT column keep their names, as in target-iceberg; they are not identifiers in the catalog.
- **Key properties**, the tap's and the configured ones. `key_properties`, `type_source`, `partition` (and `partition_fields`) and `sort_order` may name a column by the tap's name or the conformed one; a sort entry naming a column with a space quotes it (`'"First Name" DESC'`).
- **`_sdc_*` metadata columns** already conform to themselves, so `_sdc_deleted_at` and `delete_rows_with_deleted_at` work as before.
- On a Postgres catalog, conformed names are then truncated to `max_column_length`.

Table and schema names are not conformed.

**Turning it on for an existing table.** DuckDB folds the case of ASCII letters in names, so a name that only changes case (`Id` → `id`) stays the same column, keeping its old spelling. Any other renamed property becomes a new column beside the old one:

- **A renamed key column fails the stream** (`Order ID` → `order_id`, when the table has `Order ID` and no `order_id`). A merge would match on the new, empty key column, replace no row, and load every row again beside the old ones, with the old key NULL in the new rows. The error names the columns; turn `conform_column_names` off for that stream, or migrate the table first (rename its columns to the conformed names).
- **Other renamed columns** (`First Name` → `first_name`) are added as new columns, with a warning naming them. From then on the old column is NULL in every row loaded, including the rows a merge replaces; earlier rows keep their values only in the old column. Copy them over and drop the old column if you want one.

Turn it on for new tables, or migrate first. Since it can be set per stream, a pipeline can turn it on for its new streams only.

With it off, a key or layout column named by its conformed name (`created_at` for `Created At`, as a config written for target-iceberg names it) fails the stream, and the error says that `conform_column_names` would accept it.

Values load as they do with the setting off, including [JSON text in object and array properties](#json-text-in-object-and-array-properties).

**A fix in the same change, setting on or off.** With `flatten_max_level`, a flattened field that shares its name with a top-level property no longer takes that property's type. For example, with an array property `tags` and an object `meta` with a string field `tags`, the column `meta__tags` used to store `x` as the JSON string `"x"`; it now stores `x`. On an existing table, rows loaded from then on hold the corrected value.

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
- a partition `name`;
- the `truncate` and `void` transforms (that entry is skipped);
- `ordering` with `mode: zorder`;
- the table config's maintenance sections (`partition_evolution`, `retention`, sizing, …), `warehouse` and `version`.

If skipping `truncate`/`void` leaves no partition key, the partition counts as not configured, so an existing table's partitioning is not reset by accident.

`write_mode` and `key_properties` are applied (see [Load method and keys per stream](#load-method-and-keys-per-stream)), and so are `type_source` and `on_error` (see [Typing all-string streams](#typing-all-string-streams-type_source)); target-iceberg's default `on_error: raise` in `stream_defaults` changes nothing without a `type_source`. Unknown keys, unknown transforms and columns the stream lacks are rejected too. Column names may be given in full where a Postgres catalog truncates them to 63 characters.

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

### BATCH messages

A tap with the singer-sdk's BATCH capability sends files instead of RECORD messages: JSONL (plain or gzip) or Parquet, named by URL in each message's manifest. The target reads them and loads each record exactly as the same RECORD message would be loaded:
- stream maps, `_sdc_*` handling (`add_record_metadata`, a tap's `_sdc_deleted_at`) and `validate_records` apply;
- flattening, column-name truncation and type coercion apply;
- merges, deletes and overwrites apply.

RECORD and BATCH messages can be mixed in a stream. The singer-sdk's own BATCH handling skipped all of the above (a flattened column loaded NULL, with no error), so it is not used.

- **Memory:** a file is read in chunks of `batch_size_rows` (Parquet) or line by line (JSONL). Whenever a batch fills, it is loaded, so a large file never sits in memory whole.
- **Where:** each manifest entry is opened with [fsspec](https://filesystem-spec.readthedocs.io/): a local path or `file://` URL, or object storage such as `s3://` (install the `s3` extra for `s3fs`) or `gs://` (install `gcsfs`). `batch_config.storage.params` are passed to the file system, for credentials.
- **Parquet values** are read as their JSON forms, so a schema written for RECORD messages validates and loads them alike:
  - timestamps, dates and times as ISO 8601 text, in the column's time zone and with all nine digits of a nanosecond timestamp (a timestamp without a time zone has no offset, so it is read as UTC with `date_time_type: timestamptz`);
  - floats as the decimal of their shortest text (a float32 `0.1` is `0.1`, as JSON writes it), and decimals unchanged;
  - binary as base64 text (declare `contentEncoding: base64` to load it as BLOB);
  - structs and lists as objects and arrays; maps as objects with text keys, an empty map as `{}`.
- **The message's `version` and `time_extracted`**, if a tap adds them, go with each of its records, as on RECORD messages (for `add_record_metadata` and ACTIVATE_VERSION).

  A type with no JSON form, such as a duration, fails the stream with a message naming it.
- **Speed:** the files replace stdin's JSON parsing, but the per-record work of the RECORD path remains. An Arrow-native path for Parquet is planned separately.

### Schema changes during a run

A tap may send a stream's SCHEMA again during a run, as API taps do when a page brings a field they hadn't seen. A SCHEMA identical to the stream's current one changes nothing. One that changes the schema or the key properties gets the stream a new sink, as in the Singer SDK, and its setup changes the table (a new property adds its column). The records received before the message are loaded first, under the old schema, before the table changes, so a merge keeps the later record of a key. Then:
- **Append and merge** load the later records into the changed table. The earlier rows have NULL in a column added after them.
- **Overwrite** still replaces the table once, at the end of the stream, with the batches staged under both schemas. If the new SCHEMA changes the key properties, each schema's batches are deduplicated by their own keys, and both sets of rows load. A stream that changes from an overwrite to a merge (`overwrite_if_no_pk`, and the new SCHEMA adds key properties) replaces the table with its earlier rows at the change, and merges the later ones into them. The other way round, a merge or append stream that loses its key properties under `overwrite_if_no_pk` stages the records it still holds at the change for the overwrite, so the replace keeps them with the later rows. Batches it already loaded earlier in the run (a full batch, or a drain after `max_record_age_minutes`) are in the table and are replaced with the rest; a warning names the stream when that happens.
- **ACTIVATE_VERSION** after the change retires the rows of other versions as it would without the change, including a version announced before it.

Every load names the table's columns, so a value always lands in its own column, also when another stream loading the same table (see [Streams that load one table](#streams-that-load-one-table)) added a column in between.

### Overwrite and ACTIVATE_VERSION

**Overwrite** (`load_method: overwrite`, or `overwrite_if_no_pk` for a stream without primary keys) replaces the table's rows atomically. Each batch is staged as a Parquet file in `temp_file_dir`, and at the end of the stream one transaction deletes the old rows and inserts every staged batch. Readers see the old table until that commit. A run that fails or is interrupted in the target leaves the old rows in place. With key properties, the new table holds one row per key, the last record received for it, whichever batch each record landed in (and whether or not `delete_rows_with_deleted_at` is on); a NULL key value counts as one value, as in a merge. Without keys (`overwrite_if_no_pk`), every record is a row. If a new SCHEMA changes the key properties mid-stream, the records staged under each set of keys keep their last row per those keys, and all of them load: rows staged under different keys aren't matched to each other. Some limits:
- **STATE is held** while any overwrite stream hasn't committed: mid-run drains don't emit it, so a resumable tap can't skip rows that were never loaded. It is emitted after the commit. STATE is one message for all streams, so other streams' checkpoints wait too. After a crash during a long overwrite, they resume from their last emitted STATE.
- **The swap happens at the end of the input.** If the tap dies but its output ends cleanly, the target swaps in what it received.
- **`temp_file_dir` needs room for the whole stream,** not one batch.
- **Memory grows with the distinct keys** of an overwrite with keys that spans several batches: to pick each key's last row, the replace holds every distinct key (with the position of the batch it was last staged in) in memory, though not the rest of the rows. A keyless overwrite, or one that fits in one batch, streams its rows without that.
- **A stream with no records still empties the table,** as before.
- **A schema change mid-stream** (a new SCHEMA for the stream) keeps one replace: the batches staged under the old schema are loaded with the later ones, in the same commit (see [Schema changes during a run](#schema-changes-during-a-run)).

**ACTIVATE_VERSION**, as full-table taps send it, retires rows that are gone at the source. The tap tags each record with a table version, then sends `ACTIVATE_VERSION` for that version. The target first loads any buffered records, then handles every row whose `_sdc_table_version` is not the activated version, including rows loaded without a version:
- **By default,** as in the Singer SDK, it sets their `_sdc_deleted_at` to the current UTC time and keeps them. A row the source sends again is revived when it is next loaded.
- **With `hard_delete: true`,** it deletes them.

Some cases retire nothing:
- **Without `add_record_metadata`:** records carry no version, so the message is ignored with a warning.
- **A version announced before any of its records** (as taps do at the start of a sync) is a no-op, so the table never empties mid-sync. A second `ACTIVATE_VERSION` for the same version, even with no records between, ends the sync: the source is empty, and every row is retired.
- **An overwrite stream** ignores versions, since it replaces the whole table anyway.

Between a sync's first batch and its `ACTIVATE_VERSION`, readers see the new rows next to rows of the old version. Set `process_activate_version_messages: false` to ignore the message entirely.

### writeCompleted events

With `nats.servers` and `nats.subject` set, the target publishes one `writeCompleted` event per table at the end of a run, so a table maintainer such as [Zamboni](https://github.com/paulcaron16k/Zamboni) can follow the write instead of waiting for its schedule. It is the event target-iceberg publishes: Zamboni's [`write.completed` contract](https://github.com/paulcaron16k/Zamboni/blob/main/docs/write-completed.md), version 1.0, a CloudEvents 1.0 JSON message, and the same `nats` keys, so a shared config works in both targets. Install the client with the extra: `pip install 'target-ducklake[nats]'` (in Meltano, `pip_url: target-ducklake[nats]`); it includes `nkeys`, which a `creds_file` needs. A run with events configured and no client fails at startup, saying so. With only one of `servers` and `subject` set, events are off (as in target-iceberg) and a warning names the missing one. With `warehouse` unset, an INFO line names the `data_path` the events will carry.

- **When:** after every stream is drained and cleaned up, so after the last commit (an overwrite's replace included). A run that fails publishes nothing, even for tables it had already committed to. A table written by several streams gets one event listing them. A table the run committed nothing to (a stream without records, except an overwrite, which empties its table) gets none.
- **Delivery:** `jetstream: auto` publishes through JetStream when a stream covers the subject, with the `Nats-Msg-Id` header set to the event id, so the server drops a duplicate within its window; otherwise over core NATS, which is at-most-once. `true` or `false` forces one. Authentication: `creds_file`, else `token`, else `username`/`password`. `tls: true` connects over TLS only, verifying the server's certificate against its host name and the system's CAs: before connecting, the target reads each server's INFO on a connection of its own, and a server that doesn't require TLS (one that only offers it as optional included, since nats-py never upgrades to it) is left out, and refused with a warning if none is left, before anything is sent. A `tls://` URL or a server that requires TLS gets TLS without the setting too.
- **Failure is a warning:** a broker that is down, refuses the credentials, lacks TLS when `tls` is set, or doesn't answer costs one warning, within 10 seconds, and the run succeeds; its data is already committed. The 10 seconds include resolving host names: the publish runs in a thread the target stops waiting for at the timeout, and names are resolved in threads that don't hold the process's exit either. Servers are tried in order, each with a 2-second connect timeout, until one connects (a server that refuses, can't be reached or stays silent passes to the next); when none does, the warning gives each server's reason. Reading the catalog for an event never fails the run either: a table whose snapshots can't be read gets no event, and a batch whose rows can't be counted leaves `rows` out, each with a warning. Warnings never show a password, a token, credentials in a server URL, or the catalog's password or secret keys.
- **This run's commits only:** the event counts only the snapshots whose commit info carries the run's id (see [Load provenance on snapshots](#load-provenance-on-snapshots)), so another writer's commits and a concurrent compaction are not counted. Schema changes (CREATE, ALTER) are not tagged and not counted: they write no rows, and the contract's counts describe the write.

How DuckLake fills the contract's fields:

| Field | Value |
| --- | --- |
| `warehouse` | the `warehouse` setting, else `data_path` without a trailing slash (DuckLake has no warehouse name) |
| `namespace`, `table` | `[schema]`, table: the DuckLake schema and table written |
| `snapshot_id` | the last DuckLake snapshot this run committed to the table |
| `snapshot_count` | DuckLake snapshots this run committed to the table (its tagged data commits) |
| `data_files_added`, `bytes_added` | data files this run's snapshots added to the table, and their size (inlined rows add none until flushed; delete files are not counted) |
| `rows` | rows of the committed batches: each staged batch's rows after its duplicate keys are dropped, whether or not they changed anything (records with `_sdc_deleted_at` included); left out if a batch couldn't be counted |
| `load_method`, `full_refresh` | `append-only`, `upsert` or `overwrite`, left out when the table's streams used different ones; `full_refresh` is true when an overwrite replaced the table |
| `streams`, `started_at`, `completed_at` | the sinks' stream names that committed (a stream map's alias when one renames the stream, so `a` and `b` aliased to `shared` give `["shared"]`), and the start of the first and end of the last commit |
| `table_uuid` | the table's DuckLake UUID |
| `writer`, `source`, `id`, `dataschema` | `target-ducklake/<version>`, `uri:target-ducklake:<host>`, `<warehouse>/<schema>/<table>@<snapshot_id>`, the contract's URL with `/1.0` |
| `partitions_changed`, `warehouse_id`, `catalog_uri` | left out: the contract says not to estimate partitions, DuckLake has no warehouse id, and the catalog URL can hold a password |

The contract is pinned: `tests/contracts/zamboni-write-completed_spec.json` is a copy of Zamboni's published schema at a tag, refreshed by `bin/refresh-write-completed-contract.sh`, and `tests/test_write_completed_contract.py` validates events from a real run against it and checks its version is `CONTRACT_VERSION` in `target_ducklake/write_completed.py`.

### Load provenance on snapshots

Every transaction that writes a table's rows sets DuckLake's commit message, so the catalog's snapshot history says which run and stream wrote each snapshot, how, and with how many rows. It is on in every run, with or without writeCompleted events. Read it with `snapshots()`:

```sql
SELECT snapshot_id, snapshot_time, commit_message, commit_extra_info
FROM my_lake.snapshots() WHERE author = 'target-ducklake';
```

| Column | Value |
| --- | --- |
| `author` | `target-ducklake` |
| `commit_message` | one line for a person, e.g. `orders: merge, 120 rows` or `snap: overwrite (full-refresh), 3 rows` |
| `commit_extra_info` | a JSON object, with the keys below; keys that don't apply are left out |

| Key | Value |
| --- | --- |
| `run_id` | the run, a fresh id per run; every commit of one run carries the same one |
| `stream` | the stream whose rows the commit writes (a stream map's alias, when one renames the stream) |
| `load_method` | `append`, `merge` or `overwrite`: the method applied, so a stream without keys appends |
| `rows` | the staged rows the commit loads (one batch, or every batch of an overwrite), records with `_sdc_deleted_at` included; for an overwrite with keys, each batch's rows, before a key staged in several batches is reduced to its last row; left out when the commit loads no rows of its own, or a batch couldn't be counted (with a warning) |
| `operation` | only on commits that do more, or other, than load rows: `full-refresh` when an overwrite replaces the table's rows (`load_method: overwrite`, or `overwrite_if_no_pk`), `activate-version` when ACTIVATE_VERSION deletes or marks the rows of other versions, `flush-inlined-data` when the table's inlined rows are written to Parquet (named after the stream whose end triggered it) |

Each batch of an append or merge is a commit of its own; an overwrite's batches are one commit. So the rows a run staged for a stream are the sum of `rows` over its commits, which only loads carry (`activate-version` and `flush-inlined-data` commits have none). With `delete_rows_with_deleted_at`, `rows` counts the records marked deleted too, which delete rows rather than load them. The tag is part of the transaction's SQL, so a retried commit keeps it. Schema changes (CREATE, ALTER) carry no commit message. These are the facts target-iceberg writes into its snapshot summaries, under DuckLake's own column names and this target's load method names.

### SQLite catalogs and other processes

SQLite allows one writer at a time, so the target drains a SQLite catalog's sinks one at a time (see `parallel_draining`). Another process can still hold the catalog file: a reader with an open transaction keeps a commit from finishing. DuckDB waits for it up to SQLite's busy timeout (5 s), then the commit fails with `database is locked`. DuckDB 1.5's sqlite extension then keeps the failed transaction open, and the catalog attachment stays locked for every later query, whatever the other process does (DUCKLKD-82).

So when a query on a SQLite catalog fails with `database is locked`, the target detaches the catalog at once, which rolls the failed transaction back and releases the lock, and attaches it again before retrying (3 attempts, 2 s then 4 s apart, as for connection errors). The retry loads the batch once: nothing of the failed commit was written to the catalog, and DuckLake removed its data files. If the catalog is still locked at the last attempt, the run fails with an error naming the catalog file. Postgres, MySQL and DuckDB catalogs are unaffected.

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
