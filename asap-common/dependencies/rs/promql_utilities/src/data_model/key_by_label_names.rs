use serde::{Deserialize, Serialize};
use tracing::debug;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct KeyByLabelNames {
    pub labels: Vec<String>, // Renamed from label_names to match query_logics usage
}

impl KeyByLabelNames {
    pub fn new(label_names: Vec<String>) -> Self {
        debug!("Creating KeyByLabelNames with {} labels", label_names.len());
        let mut sorted_names = label_names;
        sorted_names.sort(); // Match Python behavior - keys are sorted
        debug!("Sorted labels: {:?}", sorted_names);
        Self {
            labels: sorted_names,
        }
    }

    pub fn empty() -> Self {
        Self::new(Vec::new())
    }

    pub fn from_names(names: Vec<String>) -> Self {
        Self::new(names)
    }

    pub fn push(&mut self, name: String) {
        debug!("Adding label: {}", name);
        self.labels.push(name);
        self.labels.sort(); // Keep sorted
    }

    /// Set difference operation - remove labels that are in the other set
    /// Based on Python implementation: KeyByLabelNames.__sub__
    pub fn difference(&self, other: &KeyByLabelNames) -> KeyByLabelNames {
        debug!(
            "Computing difference between {:?} and {:?}",
            self.labels, other.labels
        );
        let other_set: std::collections::HashSet<_> = other.labels.iter().collect();
        let result: Vec<String> = self
            .labels
            .iter()
            .filter(|label| !other_set.contains(label))
            .cloned()
            .collect();
        KeyByLabelNames::new(result)
    }

    /// Set union operation - combine labels from both sets
    /// Based on Python implementation: KeyByLabelNames.__add__
    pub fn union(&self, other: &KeyByLabelNames) -> KeyByLabelNames {
        debug!(
            "Computing union between {:?} and {:?}",
            self.labels, other.labels
        );
        let mut combined = std::collections::HashSet::new();
        for label in &self.labels {
            combined.insert(label.clone());
        }
        for label in &other.labels {
            combined.insert(label.clone());
        }
        KeyByLabelNames::new(combined.into_iter().collect())
    }

    pub fn serialize_to_json(&self) -> serde_json::Value {
        serde_json::to_value(&self.labels).unwrap_or(serde_json::Value::Null)
    }

    pub fn deserialize_from_json(data: &serde_json::Value) -> Result<Self, serde_json::Error> {
        let names: Vec<String> = serde_json::from_value(data.clone())?;
        Ok(Self::new(names))
    }

    pub fn is_empty(&self) -> bool {
        self.labels.is_empty()
    }

    pub fn len(&self) -> usize {
        self.labels.len()
    }
}

impl Default for KeyByLabelNames {
    fn default() -> Self {
        Self::empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_key_by_label_names() {
        let key = KeyByLabelNames::new(vec!["instance".to_string(), "job".to_string()]);

        assert_eq!(key.len(), 2);
        assert_eq!(key.labels, vec!["instance".to_string(), "job".to_string()]);

        let mut key = KeyByLabelNames::new(vec!["instance".to_string(), "job".to_string()]);
        key.push("new_label".to_string());
        assert_eq!(key.len(), 3);
        // After sorting, should be in alphabetical order
        assert!(key.labels.contains(&"instance".to_string()));
        assert!(key.labels.contains(&"job".to_string()));
        assert!(key.labels.contains(&"new_label".to_string()));
    }

    #[test]
    fn test_difference() {
        let key1 = KeyByLabelNames::new(vec!["a".to_string(), "b".to_string(), "c".to_string()]);
        let key2 = KeyByLabelNames::new(vec!["b".to_string(), "c".to_string()]);

        let diff = key1.difference(&key2);
        assert_eq!(diff.len(), 1);
        assert_eq!(diff.labels, vec!["a".to_string()]);
    }

    #[test]
    fn test_union() {
        let key1 = KeyByLabelNames::new(vec!["a".to_string(), "b".to_string()]);
        let key2 = KeyByLabelNames::new(vec!["b".to_string(), "c".to_string()]);

        let union = key1.union(&key2);
        assert_eq!(union.len(), 3);
        assert_eq!(
            union.labels,
            vec!["a".to_string(), "b".to_string(), "c".to_string()]
        );
    }
}
