pub mod adapters;
pub mod fallback;
pub mod servers;

// Re-export commonly used types for convenience
pub use adapters::{create_http_adapter, AdapterConfig, HttpProtocolAdapter};
pub use fallback::FallbackClient;
pub use servers::{HttpServer, HttpServerConfig};
