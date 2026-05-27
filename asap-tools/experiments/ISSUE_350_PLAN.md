# ClickHouse Label Discovery — Implementation Plan (Issue #350)

## Context for new sessions

**Objective**: Let `metadata_columns` (the label/dimension columns the planner tracks for
rollup) be inferred from a live ClickHouse instance via its native HTTP API, instead of
requiring the user to list them manually under `dataset.precompute.label_cols` in the
experiment config. This mirrors how the PromQL planner discovers label sets from Prometheus.

**Read these files first before writing anything**:
- `asap-planner-rs/src/prometheus_client.rs` — the discovery pattern to copy
- `asap-planner-rs/src/promql/controller.rs` — how discovery feeds into plan generation
- `asap-planner-rs/src/config/input.rs` — `TableDefinition` (the struct to extend)
- `asap-planner-rs/src/sql/controller.rs` — where `from_file_with_discovery` goes
- `asap-planner-rs/src/main.rs` — CLI arg wiring
- `experiment_utils/services/misc.py` — `ControllerService` to extend
- `experiment_utils/config.py` — `generate_sql_planner_input` to update
- `experiment_run_clickhouse.py` — the runner that calls `ControllerService`
- `config/experiment_type/clickhouse.yaml` — config knobs affected

**Current state (what already exists)**:
- `experiment_run_clickhouse.py` fully orchestrates baseline + sketchdb modes
- `ClickHouseService` / `ClickHouseDataLoaderService` exist and work
- `SQLController` already plans from explicit `metadata_columns` in `TableDefinition`
- `asap-planner --query-language sql` already accepts `--data-ingestion-interval`
- `dataset.precompute.label_cols` is currently REQUIRED in the experiment config
- `reqwest` is already a Cargo dependency (used by `prometheus_client.rs`)
- The ClickHouse HTTP API is directly accessible at `:8123` — no adapter needed

**Why this is not blocked on #336**:
The original plan noted "blocked on #336" because an earlier design routed discovery
through the ASAP ClickHouse adapter. This plan queries ClickHouse directly via its native
HTTP API (`SELECT name, type FROM system.columns WHERE …`), which requires no changes to
the adapter and is independent of #336.

**Design decisions (do not re-open)**:
- Discovery queries `system.columns` (not `information_schema`): same result, more idiomatic ClickHouse.
- `metadata_columns` = all columns that are NOT the `time_column` and NOT in `value_columns`.
  No type-based filtering — the user-provided time/value column names are the only excludes.
- When `--clickhouse-url` is absent, the planner requires all `metadata_columns` to be
  explicit (current behavior unchanged). No silent fallback to empty.
- `--clickhouse-database` defaults to `"default"`.
- In the Python runner, `label_cols` becomes optional. When absent (or empty list), the
  planner input YAML omits `metadata_columns` and `--clickhouse-url` is passed so the
  planner infers them.

---

## What changes in each component

| Component | Change |
|-----------|--------|
| `asap-planner-rs/src/clickhouse_client.rs` | **Create** — HTTP query to `system.columns` |
| `asap-planner-rs/src/lib.rs` | Export new module |
| `asap-planner-rs/src/config/input.rs` | `metadata_columns` gets `#[serde(default)]` |
| `asap-planner-rs/src/sql/controller.rs` | `from_file_with_discovery()` constructor |
| `asap-planner-rs/src/main.rs` | `--clickhouse-url`, `--clickhouse-database` flags |
| `experiment_utils/services/misc.py` | `ControllerService.start()` + bare-metal/containerized helpers |
| `experiment_utils/config.py` | `generate_sql_planner_input()` — `label_cols` optional |
| `config/experiment_type/clickhouse.yaml` | `label_cols` becomes optional, documented |
| `experiment_run_clickhouse.py` | Pass `clickhouse_url` to controller in sketchdb mode |

---

## Checkpoint 1 — `clickhouse_client.rs`: schema discovery

**What**: New Rust module that queries the ClickHouse HTTP API to discover table column
names and types, then infers which columns are metadata (dimension) columns.

**ClickHouse HTTP API**: a simple GET to `:8123/?query=<url-encoded-sql>` with
`default_format=JSONEachRow`. Response is newline-delimited JSON.

Example:
```
GET http://localhost:8123/?query=SELECT+name%2Ctype+FROM+system.columns+WHERE+database%3D'default'+AND+table%3D'hits'&default_format=JSONEachRow
→ {"name":"WatchID","type":"UInt64"}
   {"name":"JavaEnable","type":"UInt8"}
   {"name":"EventTime","type":"DateTime"}
   ...
```

**Files**:
- `asap-planner-rs/src/clickhouse_client.rs` (new)
  ```rust
  // Fetch (name, type) pairs for all columns in the given table.
  pub fn fetch_columns_for_table(
      clickhouse_url: &str,
      database: &str,
      table: &str,
  ) -> Result<Vec<(String, String)>, ControllerError>

  // Return all column names that are not the time column or a value column.
  pub fn infer_metadata_columns(
      clickhouse_url: &str,
      database: &str,
      table_name: &str,
      time_column: &str,
      value_columns: &[String],
  ) -> Result<Vec<String>, ControllerError>
  ```
  - `fetch_columns_for_table`: GET `<url>/?query=SELECT+name,type+FROM+system.columns+WHERE+database='<db>'+AND+table='<table>'&default_format=JSONEachRow`
  - Uses `reqwest::blocking::Client` (already a dependency)
  - Retry loop (up to 15 attempts, 2 s delay) matching `prometheus_client.rs` pattern
  - `infer_metadata_columns`: calls `fetch_columns_for_table`, filters out `time_column`
    and all entries in `value_columns`; returns the rest sorted
- `asap-planner-rs/src/lib.rs` — add `pub mod clickhouse_client;`

**Error variant** (add to `error.rs`):
```rust
#[error("ClickHouse client error: {0}")]
ClickHouseClient(String),
```

---

## Checkpoint 2 — Make `metadata_columns` optional in `TableDefinition`

**What**: Change the serde annotation so that a `TableDefinition` with no `metadata_columns`
field is valid YAML. When the planner is given `--clickhouse-url`, it fills in missing
columns via Checkpoint 1. When no URL is given and `metadata_columns` is empty, the planner
errors with a clear message.

**Files**:
- `asap-planner-rs/src/config/input.rs`
  ```rust
  #[derive(Debug, Clone, Deserialize)]
  pub struct TableDefinition {
      pub name: String,
      pub time_column: String,
      pub value_columns: Vec<String>,
      #[serde(default)]           // ← add this
      pub metadata_columns: Vec<String>,
  }
  ```
- `asap-planner-rs/src/sql/controller.rs`
  - Keep existing `from_file` / `from_yaml` constructors unchanged
  - Add new constructor:
    ```rust
    pub fn from_file_with_discovery(
        path: &Path,
        clickhouse_url: &str,
        clickhouse_database: &str,
        opts: SQLRuntimeOptions,
    ) -> Result<Self, ControllerError>
    ```
    Reads YAML, then for each `TableDefinition` where `metadata_columns.is_empty()`,
    calls `clickhouse_client::infer_metadata_columns(url, db, table, time_col, value_cols)`.
- `asap-planner-rs/src/sql/generator.rs`
  - At the top of `generate_sql_plan`, validate that every table has non-empty
    `metadata_columns` (they must be filled in before this point):
    ```rust
    for t in &config.tables {
        if t.metadata_columns.is_empty() {
            return Err(ControllerError::PlannerError(format!(
                "Table '{}' has no metadata_columns. List them in the config file \
                 or pass --clickhouse-url for auto-discovery.",
                t.name
            )));
        }
    }
    ```

Existing unit tests in `tests/sql_integration.rs` continue to pass unchanged because they
supply explicit `metadata_columns`.

---

## Checkpoint 3 — Add `--clickhouse-url` / `--clickhouse-database` CLI flags

**What**: Wire the new constructor into `main.rs`.

**File**: `asap-planner-rs/src/main.rs`

```rust
/// ClickHouse base URL for auto-inferring metadata_columns when not specified
/// in the config file. Example: http://localhost:8123
#[arg(long = "clickhouse-url", required = false)]
clickhouse_url: Option<String>,

#[arg(long = "clickhouse-database", default_value = "default")]
clickhouse_database: String,
```

In the SQL arm of `main()`:
```rust
let controller = match args.clickhouse_url {
    Some(url) => SQLController::from_file_with_discovery(
        &config_path, &url, &args.clickhouse_database, opts,
    )?,
    None => SQLController::from_file(&config_path, opts)?,
};
controller.generate_to_dir(&args.output_dir)?;
```

**Test**: `cargo build --release` on the CloudLab node; run with `--clickhouse-url` against
a loaded ClickHouse instance; confirm the generated `streaming_config.yaml` lists the
correct label columns under `rollup`.

---

## Checkpoint 4 — Python infra: `ControllerService` + `generate_sql_planner_input`

**What**: Plumb the ClickHouse URL through the Python layer.

**Files**:
- `experiment_utils/services/misc.py` (`ControllerService`):
  - `start()`: add `clickhouse_url: Optional[str] = None`, `clickhouse_database: str = "default"`
  - `_start_bare_metal()` / `_start_containerized()`: when `query_language == "sql"` and `clickhouse_url` is set:
    ```python
    if clickhouse_url:
        cmd += f" --clickhouse-url {clickhouse_url}"
        cmd += f" --clickhouse-database {clickhouse_database}"
    ```

- `experiment_utils/config.py` (`generate_sql_planner_input`):
  - Read `label_cols` from `precompute_cfg`; if missing or empty, emit `metadata_columns: []`
    (the planner will error if no `--clickhouse-url` is given, or auto-discover if it is).
  - Remove the hard assertion that `label_cols` is non-empty.

- `config/experiment_type/clickhouse.yaml`:
  ```yaml
  precompute:
    value_col: ResolutionWidth
    label_cols: []          # empty = auto-discover from ClickHouse; list explicitly to override
    timestamp_col: EventTime
  ```

---

## Checkpoint 5 — Wire into `experiment_run_clickhouse.py`

**What**: Pass `clickhouse_url` to `controller_service.start()` in the sketchdb mode branch.
ClickHouse is already loaded and running at this point, so it can answer `system.columns`
queries immediately.

**File**: `experiment_run_clickhouse.py` — in the sketchdb block, update the
`controller_service.start()` call:

```python
controller_service.start(
    controller_input_file=os.path.join(remote_controller_dir, "planner_input.yaml"),
    streaming_engine="precompute",
    controller_remote_output_dir=remote_controller_dir,
    punting=False,
    query_language="sql",
    data_ingestion_interval=data_ingestion_interval,
    clickhouse_url=clickhouse_url,           # ← new
    clickhouse_database=CLICKHOUSE_DATABASE, # ← new
)
```

**Test**: end-to-end run without `label_cols` in the experiment config. Verify:
1. `controller.log` shows the planner output (no error about missing metadata_columns)
2. `streaming_config.yaml` and `inference_config.yaml` are written with correct rollup labels
3. The query engine starts, ingests, and the query client gets results

---

## Dependency Graph

```
[1] clickhouse_client.rs           — independent
[2] TableDefinition optional       — independent
[3] CLI flags                      — depends on [1][2]
[4] Python infra                   — depends on [3]
[5] Runner wiring                  — depends on [4]
```

Checkpoints 1 and 2 can be done in parallel (both are Rust, same crate).
Checkpoint 3 needs both. 4 and 5 need 3 to be built.

---

## What stays unchanged

- `experiment_run_e2e.py` — untouched
- All existing SQL planner unit tests (`metadata_columns` explicit still works)
- `benchmark/` scripts — untouched
- `ClickHouseService` / `ClickHouseDataLoaderService` — untouched
- `ControllerService.start()` existing call sites — new params default to `None`; no breakage
