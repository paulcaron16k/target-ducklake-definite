# CLAUDE.md

## Project Overview

target-ducklake is a Singer target for DuckLake, built on the Meltano Singer SDK. It loads data into DuckLake-backed storage (GCS, S3, or local) using DuckDB as the in-memory engine and a configurable catalog database (Postgres, SQLite, MySQL, or DuckDB).

## Commands

```bash
# Install dependencies
uv sync

# Run tests (no external database required)
uv run pytest tests/ -v

# Lint
ruff check --fix --exit-non-zero-on-fix --show-fixes

# Format
ruff format

# Type check
mypy

# Pre-commit hooks
pre-commit run --all-files

# Regenerate spec.json and plugin-declaration.yml (--check: exit 1 if stale)
uv run bin/render-admin-ui.py

# Run the target
uv run target-ducklake --version
```

## Architecture

**Data flow:** Records → preprocess (Decimal→float) → flatten → batch → write parquet → insert/merge into DuckLake → cleanup temp files

Key files:
- `target_ducklake/target.py` — `Targetducklake(SQLTarget)`: config parsing, entry point
- `target_ducklake/sinks.py` — `ducklakeSink(SQLSink)`: record processing, batching, parquet temp files, load methods (append/merge/overwrite)
- `target_ducklake/connector.py` — `DuckLakeConnector(SQLConnector)`: DuckDB connection, DDL, insert/merge operations, centralized retry logic in `execute()`
- `target_ducklake/flatten.py` — Schema/record flattening, auto-timestamp casting, column name truncation
- `target_ducklake/conform.py` — Column-name conforming at parity with the singer-sdk's `SQLSink.conform_name` (opt-in `conform_column_names`), with clash detection
- `target_ducklake/parquet_utils.py` — PyArrow schema conversion, datetime normalization, nested (STRUCT/LIST) staging
- `target_ducklake/duckdb_types.py` — Parses DuckDB type strings (DDL or `PRAGMA table_info`) into scalar/STRUCT/list trees, and renders them back with quoted names
- `target_ducklake/type_source.py` — Per-stream `type_source` data dictionary (as in target-iceberg): declares an all-string stream's columns their types and converts values before validation, with the `on_error` policy
- `target_ducklake/batch_files.py` — Reads Singer BATCH files (JSONL/Parquet via fsspec) as records in their JSON forms; `Targetducklake._process_batch_message` sends each through the RECORD path
- `target_ducklake/write_completed.py` — writeCompleted events (Zamboni's contract): the run's ledger of commits per table, and the event built from it and the catalog's snapshots
- `target_ducklake/nats_publish.py` — The `nats` setting, and publishing events over NATS (optional `nats` extra; failures are redacted warnings)
- `target_ducklake/admin_spec.py` — Builds `spec.json` (the xflow admin UI's destination form) from `config_jsonschema`, with the form-only metadata: offered settings and order, required ones, exclusion reasons, load method → sync mode
- `bin/render-admin-ui.py` — Writes `spec.json` and `plugin-declaration.yml` (from `meltano.yml`'s loader); `--check` fails if either is stale
- `target_ducklake/collisions.py` — Streams that load one table: `TableClaims` warns once per pair and refuses different keys, a full refresh, or different table settings (DUCKLKD-44)
- `target_ducklake/logger.py` — Logging config

## Code Style

- Ruff for linting (all rules enabled) and formatting
- Google-style docstrings
- mypy for type checking
- `from __future__ import annotations` in all modules
- Classes: PascalCase, functions: snake_case, constants: UPPER_SNAKE_CASE
- Private methods: leading underscore

## Testing

Tests are in `tests/test_comprehensive.py`, native STRUCT/LIST columns in `tests/test_nested_types.py`, and `type_source` in `tests/test_type_source.py`. All tests are unit tests using mocks — no external database connections needed. Run with `uv run pytest tests/ -v`.

`tests/test_write_completed_contract.py` validates writeCompleted events from a real run against Zamboni's event schema, vendored the same way by `bin/refresh-write-completed-contract.sh`.

`tests/test_admin_ui.py` keeps `spec.json`, `plugin-declaration.yml` and `meltano.yml` in agreement with `config_jsonschema` (settings, kinds, defaults, secret flags) and validates the Meltano files against Meltano's schema, vendored by `bin/refresh-meltano-schema-contract.sh`. After changing a setting, run `bin/render-admin-ui.py`.

`tests/test_table_config_contract.py` checks `layout.py`'s `table_config` reader against Zamboni's published table-config spec, vendored in `tests/contracts/` at a pinned Zamboni tag. To move to a newer Zamboni format, bump `REF` in `bin/refresh-table-config-contract.sh`, run it, and let the contract tests name what changed; never edit the vendored files by hand.

## Retry Logic

All queries go through `DuckLakeConnector.execute()`, which retries on two categories of transient errors with exponential backoff:

1. **`duckdb.IOException`** — covers Postgres catalog connectivity errors and S3/GCS storage errors (via `duckdb.HTTPException`, a subclass)
2. **`duckdb.Error` with transient message** — covers SSL/connection drops that DuckLake wraps in a plain `duckdb.Error` (e.g., "SSL connection has been closed unexpectedly"). Detected by `is_transient_error()` in `connector.py`, which checks against `_TRANSIENT_ERROR_PATTERNS`.

All other exceptions are raised immediately without retry.

- **Max retries:** 3 attempts (configurable via `max_retries` parameter)
- **Backoff:** Exponential — `2^attempt` seconds (2s, 4s, 8s)
- **Retried errors:** `duckdb.IOException` + `duckdb.Error` matching `_TRANSIENT_ERROR_PATTERNS`
- **Not retried:** Programming errors, type mismatches, constraint violations, non-transient `duckdb.Error`, etc.
- **DML safety:** `insert_into_table` and `merge_into_table` wrap operations in `BEGIN TRANSACTION` / `COMMIT`, so failed attempts are rolled back before retry
- **No reconnect needed:** On retry, we do NOT need to DETACH + re-ATTACH the DuckLake catalog. The DuckDB `postgres` extension uses a connection pool (`PostgresConnectionPool`) that calls `PQreset()` on bad connections when they are returned to the pool. After a failed query, the next retry gets a repaired or fresh Postgres connection automatically. See: [postgres_connection_pool.cpp#L93-L99](https://github.com/duckdb/duckdb-postgres/blob/main/src/storage/postgres_connection_pool.cpp#L93-L99)
- **`ducklake_max_retry_count` does NOT cover connectivity errors.** DuckLake's internal retry loop only retries concurrency conflicts (primary key/unique violations, conflict errors) during transaction commit. Postgres "Connection refused" errors are NOT retried by DuckLake — our `execute()` retry is the only layer handling these. See: [ducklake_transaction.cpp#L2518-L2531](https://github.com/duckdb/ducklake/blob/main/src/storage/ducklake_transaction.cpp#L2518-L2531)

## Column Name Truncation

When `catalog_type` is `postgres`, column names longer than 63 characters are automatically truncated to match PostgreSQL's identifier length limit (NAMEDATALEN). This is a workaround for [ducklake#619](https://github.com/duckdb/ducklake/issues/619).

- Controlled by `max_column_length` config (default 63, set to 0 to disable)
- Only applies when `catalog_type == "postgres"`
- Collision handling: if truncation produces duplicate names, columns are shortened further and given `_{i}` suffixes
- Implementation: `truncate_column_names()` in `flatten.py`, called via `ducklakeSink._rename_columns()` in `sinks.py`, after `conform_column_names` conforming (if on)
- The rename mapping is also applied to `key_properties` (the property; `_key_properties` keeps the records' names, which the SDK checks records against) and to each record in `flatten_record()`

## Branch Model: `main` vs `definite`

This repo has two long-lived branches:

- **`main`** — public, general-purpose target-ducklake. External users depend on it.
- **`definite`** — Definite-internal branch. Adds the GCS ADC path (`_use_definite_gcp_credential_chain`, `DEFINITE_EXTENSION_REPO`, `gcss://` URI rewrite) and the `meta_role` config. Tracks `main` as upstream.

### Branch naming

- **`def/<topic>`** — Definite-only branches (PR'd into `definite`).
- **`general/<topic>`** — branches with shared changes that go into both `main` and `definite` (PR'd into `main` first, then synced into `definite`).
- **`sync/main-into-definite-YYYY-MM-DD`** — sync branches that bring `main` into `definite`.

The prefix makes the intended PR base obvious from the branch name and reduces the risk of `gh pr create` defaulting to the wrong base.

### Decision tree

```text
Is the change Definite-specific (touches Definite-only code, only useful to Definite)?
├── YES → branch `def/<topic>` off `definite`, PR into `definite`. Done.
└── NO (shared)
    ├── 1. branch `general/<topic>` off `main`, PR into `main`, merge.
    └── 2. then branch `sync/main-into-definite-YYYY-MM-DD` off `definite`,
           `git merge origin/main`, resolve conflicts (keep BOTH the new shared
           change AND the Definite-only code), PR into `definite`.
```

### Scenario 1: Definite-only change

```bash
git checkout definite && git pull
git checkout -b def/some-feature
# edits + commit
git push -u origin def/some-feature
gh pr create --base definite --title "..." --body "..."
```

The critical flag is `--base definite`. The default is `main`, so it's easy to accidentally PR into the public branch.

### Scenario 2: Shared change (both branches)

**Part A — land on `main`:**

```bash
git checkout main && git pull
git checkout -b general/some-fix
# edits + commit
git push -u origin general/some-fix
gh pr create --base main --title "..." --body "..."
# (review + merge via GitHub UI)
```

**Part B — sync into `definite`:**

```bash
git checkout definite && git pull
git checkout -b sync/main-into-definite-YYYY-MM-DD
git merge origin/main
# resolve any conflicts, then:
git push -u origin sync/main-into-definite-YYYY-MM-DD
gh pr create --base definite --title "sync main into definite" --body "..."
```

### Conflict-resolution rules during sync

When `git merge origin/main` hits a conflict during a Scenario 2 sync:

- **Conflict in a Definite-specific block** (e.g., shared change touches `connector.py` near `_build_gcp_credential_chain_script`): resolve by hand, keeping BOTH the new shared change AND the Definite block. Do NOT `--ours` here — that drops the shared change.
- **Conflict in a file with no Definite code**: just resolve the textual conflict normally.

### Gotchas

- **Always check `--base` on PRs.** A shared change PR'd into `definite` will never reach `main` and won't help upstream users.
- **Do not `git merge -s ours origin/main` for Scenario 2 syncs.** That strategy was used exactly once, for the initial split (PR #66, where `main`'s removal commit had to be absorbed without re-applying the deletion). Using it for shared-change syncs would silently drop real shared changes.
- **Sync promptly.** The longer `definite` lags `main`, the more conflicts pile up. Aim for same-day or weekly batched syncs.
- **Do not squash-merge sync PRs.** Squashing destroys the merge-commit metadata git uses to recognize main↔definite as related, which makes the *next* sync conflict-heavy. Use "Create a merge commit" on sync PRs (regular feature PRs can squash).
- **If a Definite-only commit later turns out useful upstream:** cherry-pick it onto a `general/` branch off `main`, PR into `main`, then sync as Scenario 2.

## Key Dependencies

- `duckdb>=1.5.2,<1.6.0`, `singer-sdk~=0.46.4`, `pyarrow>=20.0.0`, `polars>=1.31.0`, `sqlalchemy>=2.0.41`
