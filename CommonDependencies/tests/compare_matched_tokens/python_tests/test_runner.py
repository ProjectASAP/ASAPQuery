#!/usr/bin/env python3

import sys
import os
from test_data import TestData
from pattern_tests import PatternTester

def main():
    if len(sys.argv) < 2:
        print("Usage: python test_runner.py <test_data_file>")
        sys.exit(1)

    test_data_file = sys.argv[1]

    try:
        test_data = TestData.load_from_file(test_data_file)
    except Exception as e:
        print(f"Failed to load test data: {e}")
        sys.exit(1)

    tester = PatternTester()
    results = []

    print("Running Python PromQL Pattern Tests...")
    print("======================================")

    for test_case in test_data.test_cases:
        print(f"Running test: {test_case.id} - {test_case.description}")
        result = tester.test_query(test_case)

        if result.success:
            print(f"✅ PASSED ({result.execution_time_ms:.2f}ms)")
        else:
            print(f"❌ FAILED ({result.execution_time_ms:.2f}ms): {result.error_message}")

        results.append(result)

    passed = sum(1 for r in results if r.success)
    total = len(results)

    print(f"\nTest Summary:")
    print(f"Total: {total}, Passed: {passed}, Failed: {total - passed}")

    # Save results
    output_file = "python_test_results.json"
    test_data.save_results(results, output_file)
    print(f"Results written to: {output_file}")

if __name__ == "__main__":
    main()
