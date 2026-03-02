# Adding a Protocol Server

Protocol servers handle network communication for specific protocols. This guide shows how to add a new protocol server (like Flight SQL, gRPC, etc.).

## Overview

A protocol server:
- Listens on a network port
- Handles protocol-specific requests
- Uses adapters to process queries
- Returns protocol-specific responses

## Example: Adding Flight SQL Server

Flight SQL is Apache Arrow's SQL protocol over gRPC. Here's how to add it:

### Step 1: Create the Server

Create `src/drivers/query/servers/flight_sql.rs`:

```rust
#[derive(Debug, Clone)]
pub struct FlightSqlServerConfig {
    pub port: u16,
    pub adapter_config: AdapterConfig,
}

pub struct FlightSqlServer {
    config: FlightSqlServerConfig,
    query_engine: Arc<SimpleEngine>,
    store: Arc<dyn Store>,
}

impl FlightSqlServer {
    pub fn new(
        config: FlightSqlServerConfig,
        query_engine: Arc<SimpleEngine>,
        store: Arc<dyn Store>,
    ) -> Self {
        Self {
            config,
            query_engine,
            store,
        }
    }

    pub async fn run(self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        ...
    }
}
```

### Step 3: Export from Module

Update `src/drivers/query/servers/mod.rs`:

```rust
pub mod flight_sql;
pub use flight_sql::{FlightSqlServer, FlightSqlServerConfig};
```

### Step 4: Update Main Binary

Update `src/main.rs` to support choosing the server:

```rust
#[derive(Parser, Debug)]
struct Args {
    // ... existing args ...

    /// Server protocol to use (http, flight_sql)
    #[arg(long, default_value = "http")]
    server_protocol: String,

    // ... rest of args ...
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // ... setup engine, store, etc. ...

    match args.server_protocol.as_str() {
        "http" => {
            let server = HttpServer::new(http_config, engine, store);
            server.run().await?;
        }
        "flight_sql" => {
            let flight_config = FlightSqlServerConfig {
                port: args.http_port,
                adapter_config,
            };
            let server = FlightSqlServer::new(flight_config, engine, store);
            server.run().await?;
        }
        _ => {
            eprintln!("Unknown server protocol: {}", args.server_protocol);
            std::process::exit(1);
        }
    }

    Ok(())
}
```
