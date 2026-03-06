import json
from dataclasses import dataclass
from typing import Dict, List, Any, Optional
from datetime import datetime

@dataclass
class MetricToken:
    name: str
    labels: Dict[str, str]
    at_modifier: Optional[str]

@dataclass
class FunctionToken:
    name: str

@dataclass
class AggregationToken:
    op: str
    modifier: Optional[str]

@dataclass
class RangeToken:
    range: str

@dataclass
class TestCase:
    id: str
    description: str
    query: str
    expected_pattern_type: str
    expected_tokens: Dict[str, Any]

@dataclass
class PatternBuilderTest:
    id: str
    description: str
    builder_call: str
    parameters: Dict[str, Any]
    expected_pattern: Dict[str, Any]

@dataclass
class TestResult:
    test_id: str
    success: bool
    error_message: Optional[str] = None
    actual_pattern_type: Optional[str] = None
    actual_tokens: Optional[Dict[str, Any]] = None
    execution_time_ms: float = 0.0

@dataclass
class TestSuiteResult:
    language: str
    timestamp: str
    total_tests: int
    passed_tests: int
    failed_tests: int
    results: List[TestResult]

class TestData:
    def __init__(self, test_cases: List[TestCase], pattern_builder_tests: List[PatternBuilderTest]):
        self.test_cases = test_cases
        self.pattern_builder_tests = pattern_builder_tests

    @classmethod
    def load_from_file(cls, file_path: str) -> 'TestData':
        with open(file_path, 'r') as f:
            data = json.load(f)

        test_cases = [
            TestCase(
                id=case['id'],
                description=case['description'],
                query=case['query'],
                expected_pattern_type=case['expected_pattern_type'],
                expected_tokens=case['expected_tokens']
            )
            for case in data['test_cases']
        ]

        pattern_builder_tests = [
            PatternBuilderTest(
                id=test['id'],
                description=test['description'],
                builder_call=test['builder_call'],
                parameters=test['parameters'],
                expected_pattern=test['expected_pattern']
            )
            for test in data['pattern_builder_tests']
        ]

        return cls(test_cases, pattern_builder_tests)

    def save_results(self, results: List[TestResult], output_file: str):
        passed = sum(1 for r in results if r.success)
        total = len(results)

        suite_result = TestSuiteResult(
            language="python",
            timestamp=datetime.utcnow().isoformat(),
            total_tests=total,
            passed_tests=passed,
            failed_tests=total - passed,
            results=results
        )

        with open(output_file, 'w') as f:
            json.dump(suite_result.__dict__, f, indent=2, default=self._serialize_result)

    def _serialize_result(self, obj):
        if hasattr(obj, '__dict__'):
            return obj.__dict__
        return str(obj)
