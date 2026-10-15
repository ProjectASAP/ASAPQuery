"""Tests for the query engine's retention slack derived from the workload."""

import unittest

from omegaconf import OmegaConf

from experiment_utils.config import CLIENT_SEND_LAG_SLACK_MS, get_retention_slack_ms


class RetentionSlackTest(unittest.TestCase):
    def test_largest_query_time_offset_plus_one_ingestion_interval(self):
        # A 1s-window instant query evaluated 10s in the past fell back to
        # Prometheus every time until retention covered the offset.
        groups = OmegaConf.create(
            [
                {"client_options": {"query_time_offset": 10}},
                {"client_options": {"query_time_offset": 4}},
            ]
        )
        self.assertEqual(
            get_retention_slack_ms(groups, 1000), 11_000 + CLIENT_SEND_LAG_SLACK_MS
        )

    def test_covers_a_send_lagging_behind_the_offset(self):
        # A 1s-window instant query sent ~12.4s after its evaluation time
        # (offset 10s, client ~2.4s behind while a 60s group fired) found its
        # window already evicted with only offset + one interval of slack.
        groups = OmegaConf.create([{"client_options": {"query_time_offset": 10}}])
        self.assertGreaterEqual(get_retention_slack_ms(groups, 1000), 12_400 + 1000)

    def test_missing_offset_counts_as_zero(self):
        groups = OmegaConf.create([{"client_options": {}}, {}])
        self.assertEqual(
            get_retention_slack_ms(groups, 15_000), 15_000 + CLIENT_SEND_LAG_SLACK_MS
        )

    def test_no_query_groups(self):
        self.assertEqual(
            get_retention_slack_ms(None, 1000), 1000 + CLIENT_SEND_LAG_SLACK_MS
        )


if __name__ == "__main__":
    unittest.main()
