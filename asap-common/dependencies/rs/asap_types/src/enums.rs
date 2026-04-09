use std::fmt;
use std::str::FromStr;

#[derive(clap::ValueEnum, Clone, Copy, Debug, PartialEq)]
#[allow(non_camel_case_types)]
pub enum QueryLanguage {
    #[value(alias = "SQL")]
    sql,
    #[value(alias = "PROMQL")]
    promql,
    #[value(alias = "ElasticQueryDSL")]
    elastic_querydsl,
    #[value(alias = "ElasticSQL")]
    elastic_sql,
}

/// Policy for cleaning up old aggregates from the store.
/// Must be explicitly specified in inference_config.yaml.
#[derive(Clone, Debug, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CleanupPolicy {
    /// Keep only the N most recent aggregates (circular buffer behavior)
    CircularBuffer,
    /// Remove aggregates after they've been read N times
    ReadBased,
    /// Never clean up aggregates
    NoCleanup,
}

impl fmt::Display for CleanupPolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CleanupPolicy::CircularBuffer => write!(f, "circular_buffer"),
            CleanupPolicy::ReadBased => write!(f, "read_based"),
            CleanupPolicy::NoCleanup => write!(f, "no_cleanup"),
        }
    }
}

impl FromStr for CleanupPolicy {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "circular_buffer" => Ok(CleanupPolicy::CircularBuffer),
            "read_based" => Ok(CleanupPolicy::ReadBased),
            "no_cleanup" => Ok(CleanupPolicy::NoCleanup),
            _ => Err(format!("Unknown cleanup policy: '{s}'")),
        }
    }
}

/// Window type for streaming aggregations.
#[derive(
    Clone, Debug, Copy, Default, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum WindowType {
    #[default]
    Tumbling,
    Sliding,
}

impl fmt::Display for WindowType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WindowType::Tumbling => write!(f, "tumbling"),
            WindowType::Sliding => write!(f, "sliding"),
        }
    }
}

impl FromStr for WindowType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "tumbling" => Ok(WindowType::Tumbling),
            "sliding" => Ok(WindowType::Sliding),
            _ => Err(format!("Unknown window type: '{s}'")),
        }
    }
}
