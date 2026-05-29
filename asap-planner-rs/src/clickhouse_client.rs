use std::collections::HashSet;
use std::thread;
use std::time::Duration;

use serde::Deserialize;
use tracing::{debug, warn};

use crate::error::ControllerError;

const MAX_RETRIES: u32 = 15;
const RETRY_DELAY: Duration = Duration::from_secs(2);

#[derive(Deserialize)]
struct ColumnRow {
    name: String,
    #[serde(rename = "type")]
    column_type: String,
}

/// Fetch `(name, type)` pairs for all columns in `database.table` via the
/// ClickHouse HTTP API (`system.columns`).
fn fetch_columns_for_table(
    clickhouse_url: &str,
    database: &str,
    table: &str,
) -> Result<Vec<(String, String)>, ControllerError> {
    let base_url = clickhouse_url.trim_end_matches('/');
    let sql = format!(
        "SELECT name, type FROM system.columns WHERE database = '{}' AND table = '{}'",
        database, table
    );
    let client = reqwest::blocking::Client::new();

    for attempt in 1..=MAX_RETRIES {
        let response = client
            .get(base_url)
            .query(&[("query", sql.as_str()), ("default_format", "JSONEachRow")])
            .send()
            .map_err(|e| {
                ControllerError::ClickHouseClient(format!(
                    "HTTP request failed for table '{}.{}': {}",
                    database, table, e
                ))
            })?;

        let status = response.status();

        if status == reqwest::StatusCode::SERVICE_UNAVAILABLE {
            warn!(
                "ClickHouse returned 503 for table '{}.{}' (attempt {}/{}); retrying in {}s",
                database,
                table,
                attempt,
                MAX_RETRIES,
                RETRY_DELAY.as_secs(),
            );
            thread::sleep(RETRY_DELAY);
            continue;
        }

        if !status.is_success() {
            return Err(ControllerError::ClickHouseClient(format!(
                "ClickHouse returned HTTP {} for table '{}.{}'",
                status, database, table
            )));
        }

        let body = response.text().map_err(|e| {
            ControllerError::ClickHouseClient(format!(
                "Failed to read ClickHouse response for table '{}.{}': {}",
                database, table, e
            ))
        })?;

        let mut columns = Vec::new();
        for line in body.lines() {
            let row: ColumnRow = serde_json::from_str(line).map_err(|e| {
                ControllerError::ClickHouseClient(format!(
                    "Failed to parse ClickHouse column row {:?}: {}",
                    line, e
                ))
            })?;
            columns.push((row.name, row.column_type));
        }

        debug!(
            "Fetched {} columns for table '{}.{}'",
            columns.len(),
            database,
            table
        );
        return Ok(columns);
    }

    Err(ControllerError::ClickHouseClient(format!(
        "ClickHouse returned 503 for table '{}.{}' after {} attempts; giving up",
        database, table, MAX_RETRIES
    )))
}

/// Query `system.columns` and return all column names that are not the time
/// column or one of the value columns, sorted alphabetically.
///
/// These are the metadata (dimension) columns the planner uses for rollup,
/// analogous to PromQL label sets discovered from Prometheus.
pub fn infer_metadata_columns(
    clickhouse_url: &str,
    database: &str,
    table_name: &str,
    time_column: &str,
    value_columns: &[String],
) -> Result<Vec<String>, ControllerError> {
    let all_columns = fetch_columns_for_table(clickhouse_url, database, table_name)?;

    let exclude: HashSet<&str> = std::iter::once(time_column)
        .chain(value_columns.iter().map(String::as_str))
        .collect();

    let mut metadata: Vec<String> = all_columns
        .into_iter()
        .map(|(name, _)| name)
        .filter(|name| !exclude.contains(name.as_str()))
        .collect();
    metadata.sort();

    debug!(
        "Inferred metadata columns for table '{}': {:?}",
        table_name, metadata
    );
    Ok(metadata)
}
