use serde::{Deserialize, Serialize};
// use std::collections::HashMap;
use std::hash::{Hash, Hasher};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeyByLabelValues {
    // pub labels: HashMap<String, String>,
    pub labels: Vec<String>,
}

impl KeyByLabelValues {
    pub fn new() -> Self {
        Self { labels: Vec::new() }
    }

    pub fn new_with_labels(labels: Vec<String>) -> Self {
        Self { labels }
    }

    pub fn insert(&mut self, value: String) {
        self.labels.push(value);
    }

    pub fn get(&self, index: usize) -> Option<&String> {
        self.labels.get(index)
    }

    pub fn serialize_to_json(&self) -> serde_json::Value {
        serde_json::to_value(&self.labels).unwrap_or(serde_json::Value::Null)
    }

    pub fn deserialize_from_json(data: &serde_json::Value) -> Result<Self, serde_json::Error> {
        let labels: Vec<String> = serde_json::from_value(data.clone())?;
        Ok(Self { labels })
    }

    pub fn serialize_to_bytes(&self) -> Vec<u8> {
        bincode::serialize(&self.labels).unwrap_or_default()
    }

    pub fn deserialize_from_bytes(buffer: &[u8]) -> Result<Self, Box<dyn std::error::Error>> {
        let labels: Vec<String> = bincode::deserialize(buffer)?;
        Ok(Self { labels })
    }

    /// Encode labels as a semicolon-joined string — the canonical key format used
    /// for all sketch hashing (CountMinSketch, HydraKLL, SetAggregator, DeltaSet).
    pub fn to_semicolon_str(&self) -> String {
        self.labels.join(";")
    }

    /// Decode a semicolon-joined string back into a KeyByLabelValues.
    pub fn from_semicolon_str(s: &str) -> Self {
        Self {
            labels: s.split(';').map(|s| s.to_string()).collect(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.labels.is_empty()
    }

    pub fn len(&self) -> usize {
        self.labels.len()
    }
}

impl Hash for KeyByLabelValues {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Create a sorted vector of key-value pairs for consistent hashing
        let mut sorted_pairs: Vec<_> = self.labels.iter().collect();
        sorted_pairs.sort();

        for value in sorted_pairs {
            value.hash(state);
        }
    }
}

impl Default for KeyByLabelValues {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for KeyByLabelValues {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{{")?;
        let mut first = true;
        for value in &self.labels {
            if !first {
                write!(f, ", ")?;
            }
            write!(f, "{value}")?;
            first = false;
        }
        write!(f, "}}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_key_by_label_values() {
        let mut key = KeyByLabelValues::new();
        key.insert("localhost:8080".to_string());
        key.insert("prometheus".to_string());

        assert_eq!(key.len(), 2);
        assert_eq!(key.get(0), Some(&"localhost:8080".to_string()));
        assert_eq!(key.get(1), Some(&"prometheus".to_string()));
    }

    #[test]
    fn test_serialization() {
        let mut key = KeyByLabelValues::new();
        key.insert("test".to_string());

        let json = key.serialize_to_json();
        let deserialized = KeyByLabelValues::deserialize_from_json(&json).unwrap();
        assert_eq!(key, deserialized);
    }

    #[test]
    fn test_byte_serialization() {
        let mut key = KeyByLabelValues::new();
        key.insert("test".to_string());

        let bytes = key.serialize_to_bytes();
        let deserialized = KeyByLabelValues::deserialize_from_bytes(&bytes).unwrap();
        assert_eq!(key, deserialized);
    }

    #[test]
    fn test_semicolon_roundtrip() {
        let key = KeyByLabelValues::new_with_labels(vec!["web".to_string(), "prod".to_string()]);
        assert_eq!(key.to_semicolon_str(), "web;prod");
        let roundtripped = KeyByLabelValues::from_semicolon_str("web;prod");
        assert_eq!(roundtripped, key);
    }

    #[test]
    fn test_hash_consistency() {
        let mut key1 = KeyByLabelValues::new();
        key1.insert("a".to_string());
        key1.insert("b".to_string());

        let mut key2 = KeyByLabelValues::new();
        key2.insert("b".to_string());
        key2.insert("a".to_string());

        // Should hash to the same value regardless of insertion order
        let mut hasher1 = std::collections::hash_map::DefaultHasher::new();
        let mut hasher2 = std::collections::hash_map::DefaultHasher::new();

        key1.hash(&mut hasher1);
        key2.hash(&mut hasher2);

        assert_eq!(hasher1.finish(), hasher2.finish());
    }
}
