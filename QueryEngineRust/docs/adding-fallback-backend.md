# Adding a Fallback Backend

Fallback backends allow forwarding unsupported queries to external systems. This guide shows how to add support for a new fallback backend.

## Overview

A fallback backend:
- Accepts queries in a specific language (SQL, PromQL, etc.)
- Makes HTTP/gRPC/native calls to external system
- Returns results in a generic format
- Optionally provides runtime/health information

## Example: Adding DuckDB HTTP Fallback

### Step 1: Create the Fallback Client

Create `src/drivers/query/fallback/duckdb.rs`:

```rust
/// Fallback client for DuckDB HTTP API
pub struct DuckDBHttpFallback {
    client: Client,
    base_url: String,
}

impl DuckDBHttpFallback {
    pub fn new(base_url: String) -> Self {
        Self {
            client: Client::new(),
            base_url,
        }
    }
}

#[derive(Debug, Deserialize)]
struct DuckDBResponse {
    success: bool,
    data: Option<Vec<Vec<Value>>>,
    columns: Option<Vec<String>>,
    error: Option<String>,
}

#[async_trait]
impl FallbackClient for DuckDBHttpFallback {
    async fn execute_query(
        &self,
        request: &ParsedQueryRequest,
    ) -> Result<Json<Value>, StatusCode> {
        ...
    }

    async fn get_runtime_info(&self) -> Result<Value, StatusCode> {
        ...
    }
}
```

### Step 2: Export from Module

Update `src/drivers/query/fallback/mod.rs`:

```rust
mod duckdb;
pub use duckdb::DuckDBHttpFallback;
```

### Step 3: Use in Configuration

The fallback client can now be used in adapter configuration:

```rust
use crate::drivers::query::adapters::AdapterConfig;
use crate::drivers::query::fallback::DuckDBHttpFallback;
use std::sync::Arc;

// Create adapter config with DuckDB fallback
let fallback = Some(Arc::new(
    DuckDBHttpFallback::new("http://localhost:8080".to_string())
) as Arc<dyn FallbackClient>);

let config = AdapterConfig::new(
    QueryProtocol::PrometheusHttp,  // Protocol for incoming queries
    QueryLanguage::sql,              // Query language
    fallback,                        // DuckDB fallback
);
```

### Step 4: Add Tests

Add tests in `duckdb.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_duckdb_fallback_creation() {
        ...
    }

    // Mock DuckDB server test would go here
}
```

## FallbackClient Trait Methods

### Required: `execute_query()`
- Accepts a `ParsedQueryRequest` (query string + time)
- Makes external call to backend
- Returns `Json<Value>` response
- Should handle all error cases gracefully

### Optional: `get_runtime_info()`
- Returns health/status information from backend
- Has default implementation (returns empty JSON)
- Override if backend has health endpoint
