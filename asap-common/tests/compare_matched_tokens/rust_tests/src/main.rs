mod pattern_tests;
mod test_data;

use pattern_tests::PatternTester;
use std::env;
use test_data::*;
use tracing_subscriber::filter::LevelFilter;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing with debug level
    tracing_subscriber::fmt()
        .with_max_level(LevelFilter::DEBUG)
        .init();

    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: {} <test_data_file>", args[0]);
        std::process::exit(1);
    }

    let test_data_file = &args[1];
    let test_data = TestData::load_from_file(test_data_file)?;

    let tester = PatternTester::new();
    let mut results = Vec::new();

    println!("Running Rust PromQL Pattern Tests...");
    println!("=====================================");

    for test_case in &test_data.test_cases {
        println!("Running test: {} - {}", test_case.id, test_case.description);
        let result = tester.test_query(test_case);

        if result.success {
            println!("✅ PASSED ({}ms)", result.execution_time_ms);
        } else {
            println!(
                "❌ FAILED ({}ms): {}",
                result.execution_time_ms,
                result.error_message.as_deref().unwrap_or("Unknown error")
            );
        }

        results.push(result);
    }

    let passed = results.iter().filter(|r| r.success).count();
    let total = results.len();

    println!("\nTest Summary:");
    println!(
        "Total: {}, Passed: {}, Failed: {}",
        total,
        passed,
        total - passed
    );

    // Create test suite result
    let suite_result = TestSuiteResult {
        language: "rust".to_string(),
        timestamp: chrono::Utc::now().to_rfc3339(),
        total_tests: total,
        passed_tests: passed,
        failed_tests: total - passed,
        results,
    };

    // Write results to file
    let output_file = "rust_test_results.json";
    let json_output = serde_json::to_string_pretty(&suite_result)?;
    std::fs::write(output_file, json_output)?;

    println!("Results written to: {}", output_file);

    Ok(())
}
