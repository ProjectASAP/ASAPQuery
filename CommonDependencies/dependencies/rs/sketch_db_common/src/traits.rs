use serde_json::Value;

/// Trait for objects that can be serialized to different formats
pub trait SerializableToSink {
    fn serialize_to_json(&self) -> Value;
    fn serialize_to_bytes(&self) -> Vec<u8>;
}
