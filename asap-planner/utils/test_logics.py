"""Unit tests for logics.py cleanup parameter calculations."""

import pytest
from datetime import timedelta
from unittest.mock import MagicMock

from promql_utilities.query_logics.enums import QueryPatternType, CleanupPolicy
from logics import get_cleanup_param


def create_mock_match(range_seconds: int) -> MagicMock:
    """Create a mock match result with the given range duration."""
    mock = MagicMock()
    mock.tokens = {"range_vector": {"range": timedelta(seconds=range_seconds)}}
    return mock


class TestGetCleanupParamValidation:
    """Tests for validation logic in get_cleanup_param."""

    def test_range_duration_without_step_raises_error(self):
        """range_duration > 0 with step = 0 is invalid."""
        mock_match = create_mock_match(900)
        with pytest.raises(ValueError, match="must both be 0.*or both > 0"):
            get_cleanup_param(
                cleanup_policy=CleanupPolicy.CIRCULAR_BUFFER,
                query_pattern_type=QueryPatternType.ONLY_TEMPORAL,
                query_pattern_match=mock_match,
                t_repeat=30,
                window_type="tumbling",
                range_duration=3600,
                step=0,
            )

    def test_step_without_range_duration_raises_error(self):
        """step > 0 with range_duration = 0 is invalid."""
        mock_match = create_mock_match(900)
        with pytest.raises(ValueError, match="must both be 0.*or both > 0"):
            get_cleanup_param(
                cleanup_policy=CleanupPolicy.CIRCULAR_BUFFER,
                query_pattern_type=QueryPatternType.ONLY_TEMPORAL,
                query_pattern_match=mock_match,
                t_repeat=30,
                window_type="tumbling",
                range_duration=0,
                step=60,
            )

    def test_instant_query_both_zero_is_valid(self):
        """Instant queries: both range_duration=0 and step=0 is valid."""
        mock_match = create_mock_match(900)
        result = get_cleanup_param(
            cleanup_policy=CleanupPolicy.CIRCULAR_BUFFER,
            query_pattern_type=QueryPatternType.ONLY_TEMPORAL,
            query_pattern_match=mock_match,
            t_repeat=30,
            window_type="tumbling",
            range_duration=0,
            step=0,
        )
        assert result == 30  # ceil(900 / 30) = 30


class TestSlidingWindowCleanupParam:
    """Tests for sliding window cleanup parameter calculations."""

    def test_sliding_instant_query(self):
        """Sliding window instant query returns 1."""
        mock_match = create_mock_match(900)
        result = get_cleanup_param(
            cleanup_policy=CleanupPolicy.CIRCULAR_BUFFER,
            query_pattern_type=QueryPatternType.ONLY_TEMPORAL,
            query_pattern_match=mock_match,
            t_repeat=30,
            window_type="sliding",
            range_duration=0,
            step=0,
        )
        assert result == 1

    def test_sliding_range_query(self):
        """Sliding window: range_duration / step + 1."""
        mock_match = create_mock_match(900)
        # range_duration=3600, step=60 -> 3600/60 + 1 = 61
        result = get_cleanup_param(
            cleanup_policy=CleanupPolicy.CIRCULAR_BUFFER,
            query_pattern_type=QueryPatternType.ONLY_TEMPORAL,
            query_pattern_match=mock_match,
            t_repeat=30,
            window_type="sliding",
            range_duration=3600,
            step=60,
        )
        assert result == 61

    def test_sliding_same_for_both_policies(self):
        """Sliding windows use same formula for both policies."""
        mock_match = create_mock_match(900)
        result_cb = get_cleanup_param(
            cleanup_policy=CleanupPolicy.CIRCULAR_BUFFER,
            query_pattern_type=QueryPatternType.ONLY_TEMPORAL,
            query_pattern_match=mock_match,
            t_repeat=30,
            window_type="sliding",
            range_duration=3600,
            step=60,
        )
        result_rb = get_cleanup_param(
            cleanup_policy=CleanupPolicy.READ_BASED,
            query_pattern_type=QueryPatternType.ONLY_TEMPORAL,
            query_pattern_match=mock_match,
            t_repeat=30,
            window_type="sliding",
            range_duration=3600,
            step=60,
        )
        assert result_cb == result_rb == 61


class TestTumblingCircularBufferCleanupParam:
    """Tests for tumbling window + circular_buffer cleanup parameter."""

    def test_instant_query(self):
        """Instant query: T_lookback / T_repeat."""
        mock_match = create_mock_match(900)  # 15 minutes
        # T_lookback=900, T_repeat=30 -> 900/30 = 30
        result = get_cleanup_param(
            cleanup_policy=CleanupPolicy.CIRCULAR_BUFFER,
            query_pattern_type=QueryPatternType.ONLY_TEMPORAL,
            query_pattern_match=mock_match,
            t_repeat=30,
            window_type="tumbling",
            range_duration=0,
            step=0,
        )
        assert result == 30

    def test_range_query(self):
        """Range query: (T_lookback + range_duration) / min(T_repeat, step)."""
        mock_match = create_mock_match(900)  # 15 minutes
        # T_lookback=900, range_duration=3600, T_repeat=30, step=60
        # effective_repeat = min(30, 60) = 30
        # (900 + 3600) / 30 = 150
        result = get_cleanup_param(
            cleanup_policy=CleanupPolicy.CIRCULAR_BUFFER,
            query_pattern_type=QueryPatternType.ONLY_TEMPORAL,
            query_pattern_match=mock_match,
            t_repeat=30,
            window_type="tumbling",
            range_duration=3600,
            step=60,
        )
        assert result == 150

    def test_step_smaller_than_t_repeat(self):
        """When step < T_repeat, use step as effective_repeat."""
        mock_match = create_mock_match(900)
        # T_lookback=900, range_duration=3600, T_repeat=60, step=30
        # effective_repeat = min(60, 30) = 30
        # (900 + 3600) / 30 = 150
        result = get_cleanup_param(
            cleanup_policy=CleanupPolicy.CIRCULAR_BUFFER,
            query_pattern_type=QueryPatternType.ONLY_TEMPORAL,
            query_pattern_match=mock_match,
            t_repeat=60,
            window_type="tumbling",
            range_duration=3600,
            step=30,
        )
        assert result == 150


class TestTumblingReadBasedCleanupParam:
    """Tests for tumbling window + read_based cleanup parameter."""

    def test_instant_query(self):
        """Instant query: (T_lookback / T_repeat) * 1."""
        mock_match = create_mock_match(900)  # 15 minutes
        # T_lookback=900, T_repeat=30 -> (900/30) * 1 = 30
        result = get_cleanup_param(
            cleanup_policy=CleanupPolicy.READ_BASED,
            query_pattern_type=QueryPatternType.ONLY_TEMPORAL,
            query_pattern_match=mock_match,
            t_repeat=30,
            window_type="tumbling",
            range_duration=0,
            step=0,
        )
        assert result == 30

    def test_range_query(self):
        """Range query: (T_lookback / min(T_repeat, step)) * (range_duration / step + 1)."""
        mock_match = create_mock_match(900)  # 15 minutes
        # T_lookback=900, range_duration=3600, T_repeat=30, step=60
        # effective_repeat = min(30, 60) = 30
        # lookback_buckets = 900 / 30 = 30
        # num_steps = 3600 / 60 + 1 = 61
        # result = 30 * 61 = 1830
        result = get_cleanup_param(
            cleanup_policy=CleanupPolicy.READ_BASED,
            query_pattern_type=QueryPatternType.ONLY_TEMPORAL,
            query_pattern_match=mock_match,
            t_repeat=30,
            window_type="tumbling",
            range_duration=3600,
            step=60,
        )
        assert result == 1830


class TestOnlySpatialQueries:
    """Tests for ONLY_SPATIAL queries (T_lookback = T_repeat)."""

    def test_only_spatial_instant_query(self):
        """ONLY_SPATIAL uses T_lookback = T_repeat."""
        mock_match = MagicMock()  # No range_vector token needed
        # T_lookback = T_repeat = 30
        # circular_buffer instant: T_lookback / T_repeat = 30/30 = 1
        result = get_cleanup_param(
            cleanup_policy=CleanupPolicy.CIRCULAR_BUFFER,
            query_pattern_type=QueryPatternType.ONLY_SPATIAL,
            query_pattern_match=mock_match,
            t_repeat=30,
            window_type="tumbling",
            range_duration=0,
            step=0,
        )
        assert result == 1

    def test_only_spatial_range_query(self):
        """ONLY_SPATIAL range query uses T_lookback = T_repeat."""
        mock_match = MagicMock()
        # T_lookback = T_repeat = 30, range_duration=3600, step=60
        # effective_repeat = min(30, 60) = 30
        # circular_buffer: (30 + 3600) / 30 = 121
        result = get_cleanup_param(
            cleanup_policy=CleanupPolicy.CIRCULAR_BUFFER,
            query_pattern_type=QueryPatternType.ONLY_SPATIAL,
            query_pattern_match=mock_match,
            t_repeat=30,
            window_type="tumbling",
            range_duration=3600,
            step=60,
        )
        assert result == 121


class TestMinimumResult:
    """Tests that result is always at least 1."""

    def test_minimum_result_is_one(self):
        """Result should never be less than 1."""
        mock_match = create_mock_match(10)  # Very small lookback
        result = get_cleanup_param(
            cleanup_policy=CleanupPolicy.CIRCULAR_BUFFER,
            query_pattern_type=QueryPatternType.ONLY_TEMPORAL,
            query_pattern_match=mock_match,
            t_repeat=100,  # Larger than lookback
            window_type="tumbling",
            range_duration=0,
            step=0,
        )
        # 10 / 100 = 0, but should be at least 1
        assert result == 1


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
