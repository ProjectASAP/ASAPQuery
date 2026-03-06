import sys
import os
import time
import promql_parser
from typing import Any, Dict, List, Optional, Tuple

# Add the dependencies to the path
sys.path.append(
    os.path.join(
        os.path.dirname(__file__),
        "../../../dependencies/py/promql_utilities",
    )
)

from promql_utilities.ast_matching.PromQLPattern import PromQLPattern, MatchResult
from promql_utilities.ast_matching.PromQLPatternBuilder import PromQLPatternBuilder
# Using string keys for pattern categories instead of QueryPatternType enum

from test_data import TestCase, TestResult


class PatternTester:
    def __init__(self):
        self.patterns = self._build_patterns()

    def _build_patterns(self) -> Dict[str, List[PromQLPattern]]:
        patterns = {}

        # ONLY_TEMPORAL patterns
        temporal_patterns = [
            # Rate/increase pattern
            PromQLPattern(
                PromQLPatternBuilder.function(
                    ["rate", "increase"],
                    PromQLPatternBuilder.matrix_selector(
                        PromQLPatternBuilder.metric(collect_as="metric"),
                        collect_as="range_vector",
                    ),
                    collect_as="function",
                )
            ),
            # Quantile over time pattern
            PromQLPattern(
                PromQLPatternBuilder.function(
                    "quantile_over_time",
                    PromQLPatternBuilder.number(),
                    PromQLPatternBuilder.matrix_selector(
                        PromQLPatternBuilder.metric(collect_as="metric"),
                        collect_as="range_vector",
                    ),
                    collect_as="function",
                    collect_args_as="function_args",
                )
            ),
            # Other over_time functions
            PromQLPattern(
                PromQLPatternBuilder.function(
                    [
                        "sum_over_time",
                        "count_over_time",
                        "avg_over_time",
                        "min_over_time",
                        "max_over_time",
                    ],
                    PromQLPatternBuilder.matrix_selector(
                        PromQLPatternBuilder.metric(collect_as="metric"),
                        collect_as="range_vector",
                    ),
                    collect_as="function",
                )
            ),
        ]

        # ONLY_SPATIAL patterns
        spatial_patterns = [
            # Aggregation pattern
            PromQLPattern(
                PromQLPatternBuilder.aggregation(
                    ["sum", "count", "avg", "quantile", "min", "max"],
                    PromQLPatternBuilder.metric(collect_as="metric"),
                    collect_as="aggregation",
                )
            ),
            # Simple metric pattern (for standalone metrics)
            PromQLPattern(PromQLPatternBuilder.metric(collect_as="metric")),
        ]

        # ONE_TEMPORAL_ONE_SPATIAL patterns
        combined_patterns = [
            # Aggregation of quantile_over_time
            PromQLPattern(
                PromQLPatternBuilder.aggregation(
                    ["sum", "count", "avg", "quantile", "min", "max"],
                    PromQLPatternBuilder.function(
                        "quantile_over_time",
                        PromQLPatternBuilder.number(),
                        PromQLPatternBuilder.matrix_selector(
                            PromQLPatternBuilder.metric(collect_as="metric"),
                            collect_as="range_vector",
                        ),
                        collect_as="function",
                        collect_args_as="function_args",
                    ),
                    collect_as="aggregation",
                )
            ),
            # Aggregation of other temporal functions
            PromQLPattern(
                PromQLPatternBuilder.aggregation(
                    ["sum", "count", "avg", "quantile", "min", "max"],
                    PromQLPatternBuilder.function(
                        [
                            "sum_over_time",
                            "count_over_time",
                            "avg_over_time",
                            "min_over_time",
                            "max_over_time",
                            "rate",
                            "increase",
                        ],
                        PromQLPatternBuilder.matrix_selector(
                            PromQLPatternBuilder.metric(collect_as="metric"),
                            collect_as="range_vector",
                        ),
                        collect_as="function",
                    ),
                    collect_as="aggregation",
                )
        ),
        ]

        # ONLY_VECTOR mirrors ONLY_SPATIAL but represents plain instant vector selectors
        patterns["ONLY_TEMPORAL"] = temporal_patterns
        patterns["ONLY_SPATIAL"] = spatial_patterns
        patterns["ONLY_VECTOR"] = spatial_patterns
        patterns["ONE_TEMPORAL_ONE_SPATIAL"] = combined_patterns

        return patterns

    def test_query(self, test_case: TestCase) -> TestResult:
        start_time = time.time()
        test_id = test_case.id

        try:
            # Parse the query
            ast = promql_parser.parse(test_case.query)
        except Exception as e:
            return TestResult(
                test_id=test_id,
                success=False,
                error_message=f"Failed to parse query: {str(e)}",
                execution_time_ms=(time.time() - start_time) * 1000,
            )

        # Try to match against all patterns
        matched_pattern_type = None
        matched_tokens = None
        matched_raw = None

        for pattern_type, pattern_list in self.patterns.items():
            for pattern in pattern_list:
                match_result: MatchResult = pattern.matches(ast)
                if match_result.matches:
                    matched_raw = (pattern_type, match_result)
                    break
            if matched_raw:
                break

        if matched_raw:
            pattern_type, match_result = matched_raw
            # If a plain vector selector matched under the spatial patterns, classify as ONLY_VECTOR
            if pattern_type == "ONLY_SPATIAL":
                if "metric" in match_result.tokens and "aggregation" not in match_result.tokens:
                    matched_pattern_type = "ONLY_VECTOR"
                else:
                    matched_pattern_type = "ONLY_SPATIAL"
            else:
                matched_pattern_type = self._pattern_type_to_string(pattern_type)

            matched_tokens = self._serialize_tokens(match_result.tokens)

        execution_time = (time.time() - start_time) * 1000

        # Check if results match expectations
        expected_type = test_case.expected_pattern_type
        success = matched_pattern_type == expected_type

        return TestResult(
            test_id=test_id,
            success=success,
            error_message=(
                None
                if success
                else f"Pattern type mismatch. Expected: {expected_type}, Got: {matched_pattern_type}"
            ),
            actual_pattern_type=matched_pattern_type,
            actual_tokens=matched_tokens,
            execution_time_ms=execution_time,
        )

    def _pattern_type_to_string(self, pattern_type: Any) -> str:
        # pattern_type is already a string in this decoupled design
        return pattern_type if isinstance(pattern_type, str) else str(pattern_type)

    def _serialize_tokens(self, tokens: Dict) -> Dict:
        """Convert tokens to JSON-serializable format"""
        serialized = {}
        for key, value in tokens.items():
            if hasattr(value, "__dict__"):
                serialized[key] = value.__dict__
            else:
                serialized[key] = value
        return serialized
