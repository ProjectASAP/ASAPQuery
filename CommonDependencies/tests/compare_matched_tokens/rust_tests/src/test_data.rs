use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestData {
    pub test_cases: Vec<TestCase>,
    pub pattern_builder_tests: Vec<PatternBuilderTest>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestCase {
    pub id: String,
    pub description: String,
    pub query: String,
    pub expected_pattern_type: String,
    pub expected_tokens: HashMap<String, ExpectedToken>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ExpectedToken {
    Metric(MetricToken),
    Function(FunctionToken),
    Aggregation(AggregationToken),
    RangeVector(RangeToken),
    FunctionArgs(Vec<serde_json::Value>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricToken {
    pub name: String,
    pub labels: HashMap<String, String>,
    pub at_modifier: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunctionToken {
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregationToken {
    pub op: String,
    pub modifier: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RangeToken {
    pub range: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PatternBuilderTest {
    pub id: String,
    pub description: String,
    pub builder_call: String,
    pub parameters: serde_json::Value,
    pub expected_pattern: serde_json::Value,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TestResult {
    pub test_id: String,
    pub success: bool,
    pub error_message: Option<String>,
    pub actual_pattern_type: Option<String>,
    pub actual_tokens: Option<serde_json::Value>,
    pub execution_time_ms: f64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TestSuiteResult {
    pub language: String,
    pub timestamp: String,
    pub total_tests: usize,
    pub passed_tests: usize,
    pub failed_tests: usize,
    pub results: Vec<TestResult>,
}

impl TestData {
    pub fn load_from_file(path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let content = std::fs::read_to_string(path)?;
        let test_data: TestData = serde_json::from_str(&content)?;
        Ok(test_data)
    }
}
