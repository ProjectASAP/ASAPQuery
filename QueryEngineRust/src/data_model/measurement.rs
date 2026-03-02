use serde::{Deserialize, Serialize};
use std::ops::Add;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Measurement {
    pub value: f64,
}

impl Measurement {
    pub fn new(value: f64) -> Self {
        Self { value }
    }

    pub fn serialize_to_bytes(&self) -> Vec<u8> {
        self.value.to_le_bytes().to_vec()
    }

    pub fn serialize_to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "value": self.value
        })
    }

    pub fn deserialize_from_json(data: &serde_json::Value) -> Result<Self, serde_json::Error> {
        let value = data["value"].as_f64().ok_or_else(|| {
            serde_json::Error::io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Missing or invalid 'value' field",
            ))
        })?;
        Ok(Self::new(value))
    }

    pub fn deserialize_from_bytes(buffer: &[u8]) -> Result<Self, Box<dyn std::error::Error>> {
        if buffer.len() < 8 {
            return Err("Buffer too short for f64".into());
        }
        let value = f64::from_le_bytes([
            buffer[0], buffer[1], buffer[2], buffer[3], buffer[4], buffer[5], buffer[6], buffer[7],
        ]);
        Ok(Self::new(value))
    }
}

impl Add for Measurement {
    type Output = Measurement;

    fn add(self, other: Measurement) -> Measurement {
        Measurement::new(self.value + other.value)
    }
}

impl Add for &Measurement {
    type Output = Measurement;

    fn add(self, other: &Measurement) -> Measurement {
        Measurement::new(self.value + other.value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_measurement_creation() {
        let measurement = Measurement::new(42.5);
        assert_eq!(measurement.value, 42.5);
    }

    #[test]
    fn test_measurement_addition() {
        let m1 = Measurement::new(10.0);
        let m2 = Measurement::new(20.0);
        let result = m1 + m2;
        assert_eq!(result.value, 30.0);
    }

    #[test]
    fn test_serialization() {
        let measurement = Measurement::new(42.5);
        let json = measurement.serialize_to_json();
        let deserialized = Measurement::deserialize_from_json(&json).unwrap();
        assert_eq!(measurement, deserialized);
    }

    #[test]
    fn test_byte_serialization() {
        let measurement = Measurement::new(42.5);
        let bytes = measurement.serialize_to_bytes();
        let deserialized = Measurement::deserialize_from_bytes(&bytes).unwrap();
        assert_eq!(measurement, deserialized);
    }
}
