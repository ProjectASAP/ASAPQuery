#!/usr/bin/env python3

import json
import sys
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime

@dataclass
class ComparisonResult:
    test_id: str
    python_success: bool
    rust_success: bool
    both_passed: bool
    pattern_type_match: bool
    token_similarity: float
    execution_time_diff_ms: float
    issues: List[str]

@dataclass
class ComparisonSummary:
    total_tests: int
    both_passed: int
    python_only_passed: int
    rust_only_passed: int
    both_failed: int
    pattern_type_matches: int
    avg_token_similarity: float
    avg_execution_time_python: float
    avg_execution_time_rust: float
    results: List[ComparisonResult]

class ResultComparator:
    def __init__(self):
        pass

    def compare_results(self, python_results_file: str, rust_results_file: str) -> ComparisonSummary:
        """Compare Python and Rust test results"""

        with open(python_results_file, 'r') as f:
            python_data = json.load(f)

        with open(rust_results_file, 'r') as f:
            rust_data = json.load(f)

        # Create lookup maps
        python_results = {r['test_id']: r for r in python_data['results']}
        rust_results = {r['test_id']: r for r in rust_data['results']}

        comparison_results = []
        both_passed = 0
        python_only_passed = 0
        rust_only_passed = 0
        both_failed = 0
        pattern_type_matches = 0
        total_token_similarity = 0.0
        total_python_time = 0.0
        total_rust_time = 0.0

        all_test_ids = set(python_results.keys()) | set(rust_results.keys())

        for test_id in all_test_ids:
            python_result = python_results.get(test_id)
            rust_result = rust_results.get(test_id)

            if not python_result:
                print(f"Warning: Test {test_id} missing from Python results")
                continue
            if not rust_result:
                print(f"Warning: Test {test_id} missing from Rust results")
                continue

            python_success = python_result['success']
            rust_success = rust_result['success']

            # Count success patterns
            if python_success and rust_success:
                both_passed += 1
            elif python_success and not rust_success:
                python_only_passed += 1
            elif not python_success and rust_success:
                rust_only_passed += 1
            else:
                both_failed += 1

            # Check pattern type match
            pattern_type_match = (
                python_result.get('actual_pattern_type') ==
                rust_result.get('actual_pattern_type')
            )
            if pattern_type_match:
                pattern_type_matches += 1

            # Calculate token similarity
            token_similarity = self._calculate_token_similarity(
                python_result.get('actual_tokens', {}),
                rust_result.get('actual_tokens', {})
            )
            total_token_similarity += token_similarity

            # Calculate execution time difference
            python_time = python_result.get('execution_time_ms', 0.0)
            rust_time = rust_result.get('execution_time_ms', 0.0)
            total_python_time += python_time
            total_rust_time += rust_time
            execution_time_diff = abs(python_time - rust_time)

            # Identify issues
            issues = []
            if not pattern_type_match:
                issues.append(f"Pattern type mismatch: Python={python_result.get('actual_pattern_type')}, Rust={rust_result.get('actual_pattern_type')}")
            if token_similarity < 0.8:
                issues.append(f"Low token similarity: {token_similarity:.2f}")
            if python_success != rust_success:
                issues.append(f"Success mismatch: Python={python_success}, Rust={rust_success}")
            if execution_time_diff > 100:  # More than 100ms difference
                issues.append(f"Large execution time difference: {execution_time_diff:.2f}ms")

            comparison_result = ComparisonResult(
                test_id=test_id,
                python_success=python_success,
                rust_success=rust_success,
                both_passed=python_success and rust_success,
                pattern_type_match=pattern_type_match,
                token_similarity=token_similarity,
                execution_time_diff_ms=execution_time_diff,
                issues=issues
            )
            comparison_results.append(comparison_result)

        total_tests = len(comparison_results)
        avg_token_similarity = total_token_similarity / max(total_tests, 1)
        avg_python_time = total_python_time / max(total_tests, 1)
        avg_rust_time = total_rust_time / max(total_tests, 1)

        return ComparisonSummary(
            total_tests=total_tests,
            both_passed=both_passed,
            python_only_passed=python_only_passed,
            rust_only_passed=rust_only_passed,
            both_failed=both_failed,
            pattern_type_matches=pattern_type_matches,
            avg_token_similarity=avg_token_similarity,
            avg_execution_time_python=avg_python_time,
            avg_execution_time_rust=avg_rust_time,
            results=comparison_results
        )

    def _calculate_token_similarity(self, python_tokens: Dict[str, Any], rust_tokens: Dict[str, Any]) -> float:
        """Calculate similarity between token dictionaries (0.0 to 1.0)"""
        if not python_tokens and not rust_tokens:
            return 1.0
        if not python_tokens or not rust_tokens:
            return 0.0

        # Compare keys
        python_keys = set(python_tokens.keys())
        rust_keys = set(rust_tokens.keys())
        common_keys = python_keys & rust_keys
        total_keys = python_keys | rust_keys

        if not total_keys:
            return 1.0

        key_similarity = len(common_keys) / len(total_keys)

        # Compare values for common keys
        value_matches = 0
        for key in common_keys:
            if self._tokens_match(python_tokens[key], rust_tokens[key]):
                value_matches += 1

        value_similarity = value_matches / max(len(common_keys), 1)

        # Weight: 50% key similarity, 50% value similarity
        return (key_similarity + value_similarity) / 2

    def _tokens_match(self, python_token: Any, rust_token: Any) -> bool:
        """Check if individual tokens match"""
        # Handle different token representations
        if isinstance(python_token, dict) and isinstance(rust_token, dict):
            # Compare key token fields
            if 'name' in python_token and 'name' in rust_token:
                return python_token['name'] == rust_token['name']
            if 'op' in python_token and 'op' in rust_token:
                return python_token['op'] == rust_token['op']
            if 'range' in python_token and 'range' in rust_token:
                return python_token['range'] == rust_token['range']

        # Fallback to direct comparison
        return python_token == rust_token

    def generate_report(self, summary: ComparisonSummary, output_file: str):
        """Generate a detailed comparison report"""
        report = {
            'timestamp': datetime.utcnow().isoformat(),
            'summary': {
                'total_tests': summary.total_tests,
                'both_passed': summary.both_passed,
                'python_only_passed': summary.python_only_passed,
                'rust_only_passed': summary.rust_only_passed,
                'both_failed': summary.both_failed,
                'pattern_type_matches': summary.pattern_type_matches,
                'pattern_type_match_rate': summary.pattern_type_matches / max(summary.total_tests, 1),
                'avg_token_similarity': summary.avg_token_similarity,
                'avg_execution_time_python_ms': summary.avg_execution_time_python,
                'avg_execution_time_rust_ms': summary.avg_execution_time_rust,
                'performance_ratio': summary.avg_execution_time_rust / max(summary.avg_execution_time_python, 0.001)
            },
            'detailed_results': [
                {
                    'test_id': r.test_id,
                    'python_success': r.python_success,
                    'rust_success': r.rust_success,
                    'both_passed': r.both_passed,
                    'pattern_type_match': r.pattern_type_match,
                    'token_similarity': r.token_similarity,
                    'execution_time_diff_ms': r.execution_time_diff_ms,
                    'issues': r.issues
                }
                for r in summary.results
            ]
        }

        with open(output_file, 'w') as f:
            json.dump(report, f, indent=2)

def main():
    if len(sys.argv) < 3:
        print("Usage: python result_comparator.py <python_results.json> <rust_results.json>")
        sys.exit(1)

    python_file = sys.argv[1]
    rust_file = sys.argv[2]

    comparator = ResultComparator()

    print("Comparing Python and Rust test results...")
    print("==========================================")

    try:
        summary = comparator.compare_results(python_file, rust_file)

        print(f"\nComparison Summary:")
        print(f"Total tests: {summary.total_tests}")
        print(f"Both passed: {summary.both_passed}")
        print(f"Python only passed: {summary.python_only_passed}")
        print(f"Rust only passed: {summary.rust_only_passed}")
        print(f"Both failed: {summary.both_failed}")
        print(f"Pattern type matches: {summary.pattern_type_matches}/{summary.total_tests} ({summary.pattern_type_matches/max(summary.total_tests,1)*100:.1f}%)")
        print(f"Average token similarity: {summary.avg_token_similarity:.2f}")
        print(f"Avg execution time - Python: {summary.avg_execution_time_python:.2f}ms")
        print(f"Avg execution time - Rust: {summary.avg_execution_time_rust:.2f}ms")

        # Show tests with issues
        issues_found = [r for r in summary.results if r.issues]
        if issues_found:
            print(f"\nTests with issues ({len(issues_found)}):")
            for result in issues_found:
                print(f"  {result.test_id}: {', '.join(result.issues)}")

        # Generate detailed report
        output_file = "comparison_report.json"
        comparator.generate_report(summary, output_file)
        print(f"\nDetailed report written to: {output_file}")

    except Exception as e:
        print(f"Error during comparison: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
