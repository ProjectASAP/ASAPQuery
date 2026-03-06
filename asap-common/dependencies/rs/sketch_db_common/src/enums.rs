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
