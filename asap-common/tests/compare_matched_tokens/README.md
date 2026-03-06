# Cross-Language PromQL Pattern Testing Framework

This framework provides comprehensive testing to compare the functionality between Python and Rust implementations of PromQL pattern matching utilities.

## Directory Structure

```
tests/cross_language_comparison/
├── test_data/
│   └── promql_queries.json          # Test cases and expected results
├── python_tests/
│   ├── test_data.py                 # Python test data structures
│   ├── pattern_tests.py             # Python pattern testing logic
│   └── test_runner.py               # Python test runner
├── rust_tests/
│   ├── src/
│   │   ├── main.rs                  # Rust test runner entry point
│   │   ├── test_data.rs             # Rust test data structures
│   │   └── pattern_tests.rs         # Rust pattern testing logic
│   └── Cargo.toml                   # Rust project configuration
├── comparison_tests/
│   └── result_comparator.py         # Cross-language result comparison
├── utilities/
│   └── master_test_runner.py        # Orchestrates all tests
└── README.md                        # This file
```

## Quick Start

### Prerequisites

1. **Python**: Ensure Python 3.8+ is installed with access to the `promql_utilities` package
2. **Rust**: Ensure Rust 1.70+ is installed with Cargo
3. **Dependencies**: The promql_utilities packages for both Python and Rust must be available

### Running All Tests

```bash
# From the project root directory
cd tests/cross_language_comparison
python utilities/master_test_runner.py
```

This will:
1. Run Python pattern tests
2. Run Rust pattern tests
3. Compare results between both implementations
4. Generate comprehensive reports

### Running Individual Test Suites

#### Python Tests Only
```bash
cd tests/cross_language_comparison/python_tests
python test_runner.py ../test_data/promql_queries.json
```

#### Rust Tests Only
```bash
cd tests/cross_language_comparison/rust_tests
cargo run --release -- ../test_data/promql_queries.json
```

#### Comparison Only
```bash
cd tests/cross_language_comparison/comparison_tests
python result_comparator.py ../python_tests/python_test_results.json ../rust_tests/rust_test_results.json
```

## Test Data Format

The test data is defined in `test_data/promql_queries.json`:

```json
{
  "test_cases": [
    {
      "id": "unique_test_id",
      "description": "Human readable description",
      "query": "actual_promql_query",
      "expected_pattern_type": "ONLY_TEMPORAL|ONLY_SPATIAL|ONE_TEMPORAL_ONE_SPATIAL",
      "expected_tokens": {
        "metric": {"name": "...", "labels": {...}},
        "function": {"name": "..."},
        "aggregation": {"op": "..."}
      }
    }
  ],
  "pattern_builder_tests": [
    // Tests for PromQLPatternBuilder functionality
  ]
}
```

## Adding New Test Cases

1. **Add test case to JSON**: Edit `test_data/promql_queries.json` to include new queries
2. **Update patterns if needed**: Modify pattern definitions in both Python and Rust implementations
3. **Run tests**: Execute the master test runner to validate new cases

### Example Test Case

```json
{
  "id": "custom_aggregation",
  "description": "Custom aggregation test",
  "query": "avg(cpu_usage{instance=\"server1\"})",
  "expected_pattern_type": "ONLY_SPATIAL",
  "expected_tokens": {
    "metric": {
      "name": "cpu_usage",
      "labels": {"instance": "server1"},
      "at_modifier": null
    },
    "aggregation": {
      "op": "avg",
      "modifier": null
    }
  }
}
```

## Output Files

After running tests, several output files are generated:

- `python_tests/python_test_results.json` - Python test results
- `rust_tests/rust_test_results.json` - Rust test results
- `comparison_tests/comparison_report.json` - Detailed comparison report
- `test_summary.json` - High-level test execution summary

## Understanding Results

### Success Metrics
- **Both Passed**: Both implementations correctly handled the test case
- **Pattern Type Match**: Both implementations identified the same pattern type
- **Token Similarity**: Measure of how similar the extracted tokens are (0.0-1.0)

### Common Issues
- **Pattern Type Mismatch**: Implementations categorize queries differently
- **Token Extraction Differences**: Different token data extracted from the same query
- **Success Rate Differences**: One implementation handles a query that the other doesn't

### Performance Comparison
The framework also compares execution times between implementations to identify performance characteristics.

## Extending the Framework

### Adding New Pattern Types
1. Update both Python and Rust `QueryPatternType` enums
2. Add corresponding patterns to both test implementations
3. Update test data with examples of the new pattern type

### Adding New Token Types
1. Define token structures in both `test_data.py` and `test_data.rs`
2. Update token extraction logic in both pattern testers
3. Update comparison logic in `result_comparator.py`

## Troubleshooting

### Common Issues

**"Module not found" errors**: Ensure the promql_utilities packages are properly installed and accessible

**Rust build failures**: Check that all Rust dependencies are available and versions are compatible

**Path issues**: Run commands from the correct directories as shown in the examples

**Missing test files**: Ensure all required files are present and have correct permissions

### Debug Mode

For more detailed output, you can run individual components with verbose logging or add debug prints to the test implementations.

## Contributing

When contributing new tests or improvements:

1. Follow the existing code patterns
2. Add appropriate documentation
3. Test both happy path and edge cases
4. Ensure cross-platform compatibility
5. Update this README with any new features
