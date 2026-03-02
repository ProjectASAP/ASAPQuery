# Adding a Protocol Adapter

Protocol adapters handle protocol-specific request/response formatting and query language parsing. This guide shows how to add support for a new query protocol.

## Overview

A protocol adapter:
- Parses incoming requests (GET/POST parameters, headers, etc.)
- Translates queries to internal format
- Formats query results for the protocol
- Defines protocol-specific endpoints

## Example: Adding ClickHouse HTTP Adapter

### Step 1: Create the Adapter File

Create `src/drivers/query/adapters/clickhouse_http.rs`:

```rust

/// ClickHouse HTTP protocol adapter
pub struct ClickHouseHttpAdapter {
    config: AdapterConfig,
}

impl ClickHouseHttpAdapter {
    ...
}

#[async_trait]
impl QueryRequestAdapter for ClickHouseHttpAdapter {
    ...
}

#[async_trait]
impl QueryResponseAdapter for ClickHouseHttpAdapter {
    ...
}

#[async_trait]
impl HttpProtocolAdapter for ClickHouseHttpAdapter {
    ...
}
```

### Step 2: Add Protocol Enum Variant

Update `src/data_model/enums.rs` to add the new protocol:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryProtocol {
    ...
    ClickHouseHttp,  // Add this
}
```

### Step 3: Export from Module

Update `src/drivers/query/adapters/mod.rs`:

```rust
pub mod clickhouse_http;
pub use clickhouse_http::ClickHouseHttpAdapter;
```

### Step 4: Add to Factory

Update `src/drivers/query/adapters/factory.rs`:

```rust
pub fn create_http_adapter(config: AdapterConfig) -> Arc<dyn HttpProtocolAdapter> {
    match config.protocol {
        ...
        QueryProtocol::ClickHouseHttp => {  // Add this
            Arc::new(ClickHouseHttpAdapter::new(config))
        }
    }
}
```

### Step 5: Add Convenience Constructor (Optional)

Update `src/drivers/query/adapters/config.rs`:

```rust
impl AdapterConfig {
    pub fn clickhouse_http(fallback_url: String, forward_unsupported: bool) -> Self {
        ...
    }
}
```

### Step 6: Test the Adapter

Add tests in `clickhouse_http.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_parse_get_request() {
        ...
    }
}
```

## Key Traits to Implement

### Required: `QueryRequestAdapter`
- `parse_get_request()` - Parse GET requests
- `parse_post_request()` - Parse POST requests
- `get_query_endpoint()` - Return endpoint path

### Required: `QueryResponseAdapter`
- `format_success_response()` - Format successful query results
- `format_error_response()` - Format errors
- `format_unsupported_query_response()` - Format unsupported query errors

### Required: `HttpProtocolAdapter`
- `adapter_name()` - Return adapter name for logging
- `get_runtime_info_path()` - Return health/status endpoint path
- `handle_runtime_info()` - Handle health/status requests

## Common Gotchas

- Don't implement query execution in the adapter - that's the engine's job
- Don't hard-code URLs or configuration - use `AdapterConfig`
- Handle both GET and POST requests appropriately
- Return protocol-specific error formats
- Use existing types from `traits.rs` (`ParsedQueryRequest`, `QueryExecutionResult`)
