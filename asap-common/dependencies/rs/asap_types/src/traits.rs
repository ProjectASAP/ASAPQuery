use serde_json::Value;
use std::error::Error as StdError;
use std::fmt;

/// A serialization boundary failed. Carries the failing type's name so the
/// caller can report which accumulator/value produced empty or missing
/// output instead of silently persisting nothing.
#[derive(Debug)]
pub enum SerializationError {
    Bytes {
        type_name: &'static str,
        source: Box<dyn StdError + Send + Sync>,
    },
    Json {
        type_name: &'static str,
        source: Box<dyn StdError + Send + Sync>,
    },
}

impl fmt::Display for SerializationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SerializationError::Bytes { type_name, source } => {
                write!(f, "failed to serialize {type_name} to bytes: {source}")
            }
            SerializationError::Json { type_name, source } => {
                write!(f, "failed to serialize {type_name} to JSON: {source}")
            }
        }
    }
}

impl StdError for SerializationError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            SerializationError::Bytes { source, .. } | SerializationError::Json { source, .. } => {
                Some(source.as_ref())
            }
        }
    }
}

/// Trait for objects that can be serialized to different formats
pub trait SerializableToSink {
    fn serialize_to_json(&self) -> Result<Value, SerializationError>;
    fn serialize_to_bytes(&self) -> Result<Vec<u8>, SerializationError>;
}
