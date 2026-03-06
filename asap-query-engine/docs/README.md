# QueryEngineRust Developer Documentation

Welcome to the QueryEngineRust developer documentation! This directory contains guides for extending the system with new components.

## Architecture Overview

QueryEngineRust is organized into clear, extensible layers:

```
┌─────────────────────────────────────────────────────────┐
│                   Client Applications                    │
└─────────────────────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────┐
│              Protocol Servers (HTTP, etc.)               │
│  - Parse protocol-specific requests                      │
│  - Route to appropriate adapter                          │
│  - Handle protocol-specific endpoints                    │
└─────────────────────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────┐
│         Protocol Adapters (Prometheus, etc.)             │
│  - Parse query language (PromQL, SQL, etc.)             │
│  - Format responses for protocol                         │
│  - Determine if query is supported                       │
└─────────────────────────────────────────────────────────┘
                           │
                    ┌──────┴──────┐
                    ▼             ▼
        ┌─────────────────┐   ┌──────────────────┐
        │  Query Engine   │   │ Fallback Client  │
        │  - Execute      │   │  - Forward       │
        │    queries      │   │    unsupported   │
        │  - Return       │   │    queries       │
        │    results      │   │                  │
        └────────┬────────┘   └──────────────────┘
                 │
                 ▼
        ┌─────────────────┐
        │     Store       │
        │  - Data storage │
        │  - Sketches     │
        └─────────────────┘
                 ▲
                 │
        ┌────────┴────────┐
        │ Ingest Drivers  │
        │  - Kafka, etc.  │
        └─────────────────┘
```

## Directory Structure

```
src/drivers/
├── ingest/           # Data ingestion (Kafka, etc.)
├── query/
│   ├── adapters/     # Protocol adapters (Prometheus HTTP, etc.)
│   ├── fallback/     # Fallback backends (Prometheus, ClickHouse, etc.)
│   └── servers/      # Protocol servers (HTTP, Flight SQL, etc.)
```

## Extension Guides

- **[Adding a Protocol Adapter](./adding-protocol-adapter.md)** - Add support for new query protocols (e.g., ClickHouse HTTP API)
- **[Adding a Fallback Backend](./adding-fallback-backend.md)** - Add new fallback query backends (e.g., DuckDB, Elasticsearch)
- **[Adding a Protocol Server](./adding-protocol-server.md)** - Add new protocol servers (e.g., Flight SQL, gRPC)

## Key Concepts

### Protocol Adapter
Handles protocol-specific request/response formatting and query parsing. Examples: Prometheus HTTP API, ClickHouse HTTP API.

### Fallback Backend
External query system to forward unsupported queries to. Examples: Prometheus, ClickHouse, DuckDB.

### Protocol Server
Handles network communication for a specific protocol. Examples: HTTP server, Flight SQL server.

## Quick Reference

### Adding a Protocol Adapter
1. Create `src/drivers/query/adapters/my_adapter.rs`
2. Implement `HttpProtocolAdapter` trait
3. Add to factory in `factory.rs`
4. Update `QueryProtocol` enum

### Adding a Fallback Backend
1. Create `src/drivers/query/fallback/my_backend.rs`
2. Implement `FallbackClient` trait
3. Export from `fallback/mod.rs`

### Adding a Protocol Server
1. Create `src/drivers/query/servers/my_server.rs`
2. Implement server logic with appropriate adapter
3. Export from `servers/mod.rs`

## Testing

Each component should include:
- Unit tests in the same file
- Integration tests in `src/tests/`
- Example usage in documentation

## Contributing

When adding new components:
1. Follow existing naming conventions
2. Add comprehensive documentation
3. Include tests
4. Update this documentation
5. Keep backward compatibility
