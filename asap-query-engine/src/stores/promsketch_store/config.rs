use anyhow::{Context, Result};
use serde::Deserialize;

/// Configuration for ExponentialHistogram wrapping UnivMon sketches.
#[derive(Clone, Debug, Deserialize)]
pub struct EHUnivConfig {
    /// Number of EH buckets (k parameter for ExponentialHistogram).
    pub k: usize,
    /// Time window size in milliseconds.
    pub time_window: u64,
}

impl Default for EHUnivConfig {
    fn default() -> Self {
        Self {
            k: 50,
            time_window: 1_000_000,
        }
    }
}

/// Configuration for ExponentialHistogram wrapping KLL sketches.
#[derive(Clone, Debug, Deserialize)]
pub struct EHKLLConfig {
    /// Number of EH buckets (k parameter for ExponentialHistogram).
    pub k: usize,
    /// KLL sketch k parameter (controls accuracy vs memory).
    pub kll_k: i32,
    /// Time window size in milliseconds.
    pub time_window: u64,
}

impl Default for EHKLLConfig {
    fn default() -> Self {
        Self {
            k: 50,
            kll_k: 256,
            time_window: 1_000_000,
        }
    }
}

/// Configuration for ExponentialHistogram wrapping UniformSampling sketches.
#[derive(Clone, Debug, Deserialize)]
pub struct SamplingConfig {
    /// Fraction of data points to sample (0.0 to 1.0).
    pub sample_rate: f64,
    /// Time window size in milliseconds.
    pub time_window: u64,
}

impl Default for SamplingConfig {
    fn default() -> Self {
        Self {
            sample_rate: 0.2,
            time_window: 1_000_000,
        }
    }
}

/// Bundled configuration for all sketch types.
#[derive(Clone, Debug, Default, Deserialize)]
pub struct PromSketchConfig {
    pub eh_univ: EHUnivConfig,
    pub eh_kll: EHKLLConfig,
    pub sampling: SamplingConfig,
}

impl PromSketchConfig {
    /// Load a PromSketchConfig from a YAML file.
    pub fn from_yaml_file(path: &str) -> Result<Self> {
        let contents = std::fs::read_to_string(path)
            .with_context(|| format!("Failed to read sketch config file: {path}"))?;
        let config: PromSketchConfig = serde_yaml::from_str(&contents)
            .with_context(|| format!("Failed to parse sketch config YAML from: {path}"))?;
        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config_values() {
        let config = PromSketchConfig::default();

        assert_eq!(config.eh_univ.k, 50);
        assert_eq!(config.eh_univ.time_window, 1_000_000);

        assert_eq!(config.eh_kll.k, 50);
        assert_eq!(config.eh_kll.kll_k, 256);
        assert_eq!(config.eh_kll.time_window, 1_000_000);

        assert!((config.sampling.sample_rate - 0.2).abs() < f64::EPSILON);
        assert_eq!(config.sampling.time_window, 1_000_000);
    }

    #[test]
    fn test_yaml_deserialization() {
        let yaml = r#"
eh_univ:
  k: 100
  time_window: 2000000
eh_kll:
  k: 80
  kll_k: 512
  time_window: 3000000
sampling:
  sample_rate: 0.5
  time_window: 4000000
"#;
        let config: PromSketchConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.eh_univ.k, 100);
        assert_eq!(config.eh_univ.time_window, 2_000_000);
        assert_eq!(config.eh_kll.k, 80);
        assert_eq!(config.eh_kll.kll_k, 512);
        assert_eq!(config.eh_kll.time_window, 3_000_000);
        assert!((config.sampling.sample_rate - 0.5).abs() < f64::EPSILON);
        assert_eq!(config.sampling.time_window, 4_000_000);
    }

    #[test]
    fn test_from_yaml_file() {
        use std::io::Write;
        use tempfile::NamedTempFile;

        let yaml = r#"
eh_univ:
  k: 30
  time_window: 500000
eh_kll:
  k: 40
  kll_k: 128
  time_window: 600000
sampling:
  sample_rate: 0.1
  time_window: 700000
"#;
        let mut tmp = NamedTempFile::new().unwrap();
        write!(tmp, "{}", yaml).unwrap();

        let config = PromSketchConfig::from_yaml_file(tmp.path().to_str().unwrap()).unwrap();
        assert_eq!(config.eh_univ.k, 30);
        assert_eq!(config.eh_kll.kll_k, 128);
        assert!((config.sampling.sample_rate - 0.1).abs() < f64::EPSILON);
    }

    #[test]
    fn test_from_yaml_file_nonexistent() {
        let result = PromSketchConfig::from_yaml_file("/nonexistent/path.yaml");
        assert!(result.is_err());
    }
}
