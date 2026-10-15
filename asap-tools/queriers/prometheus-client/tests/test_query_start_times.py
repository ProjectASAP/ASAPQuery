"""Tests for main_prometheus_client.get_query_start_times."""

import importlib.util
import unittest
from typing import Any, Dict
from unittest import mock

import requests

URL = "http://sketchdb:8088"
QUERY = "sum by (job) (data)"


def runtime_info(start_ms_by_id: Dict[int, int]) -> Dict[str, Any]:
    return {
        "earliest_timestamp_per_aggregation_id": {
            str(k): v for k, v in start_ms_by_id.items()
        }
    }


@unittest.skipUnless(
    importlib.util.find_spec("promql_utilities") is not None,
    "main_prometheus_client needs asap-common's promql_utilities",
)
class GetQueryStartTimesTest(unittest.TestCase):
    def setUp(self) -> None:
        import main_prometheus_client

        self.main = main_prometheus_client
        sleep = mock.patch.object(main_prometheus_client.time, "sleep")
        self.sleep = sleep.start()
        self.addCleanup(sleep.stop)

    def start_times(self, config: Dict[str, Any], *infos: Any) -> Dict[str, float]:
        with mock.patch.object(
            self.main, "fetch_runtime_info", side_effect=list(infos)
        ) as fetch:
            result = self.main.get_query_start_times(URL, config)
        fetch.assert_called_with(URL)
        self.assertEqual(fetch.call_count, len(infos))
        return result

    def test_waits_for_query_aggregations_then_returns_seconds(self) -> None:
        config = {
            "queries": [{"query": QUERY, "aggregations": [{"aggregation_id": 3}]}]
        }
        result = self.start_times(
            config, runtime_info({}), runtime_info({3: 1_500, 9: 0})
        )
        self.assertEqual(result, {QUERY: 1.5})
        self.sleep.assert_called_once()

    def test_runtime_info_errors_are_not_retried(self) -> None:
        config = {
            "queries": [{"query": QUERY, "aggregations": [{"aggregation_id": 3}]}]
        }
        with self.assertRaises(requests.ConnectionError):
            self.start_times(config, requests.ConnectionError("down"))

    def test_expr_only_plan_waits_for_rqe_leaf_aggregations(self) -> None:
        # An --expr-only plan: no per-query configs, so no per-query start time.
        config = {
            "rqe_configs": {
                "t/sum_by_job/keys/1s": {
                    "aggregations": [
                        {"aggregation_id": 4, "num_aggregates_to_retain": 1}
                    ],
                    "query_time_aggregations": [],
                },
                "t/max_by_job/keys/1s": {
                    "aggregations": [{"aggregation_id": 5, "read_count_threshold": 2}],
                    "query_time_aggregations": [],
                },
            }
        }
        result = self.start_times(
            config, runtime_info({4: 0}), runtime_info({4: 0, 5: 1_000})
        )
        self.assertEqual(result, {})
        self.sleep.assert_called_once()

    def test_waits_for_rqe_leaves_next_to_query_configs(self) -> None:
        config = {
            "queries": [{"query": QUERY, "aggregations": [{"aggregation_id": 3}]}],
            "rqe_configs": {
                "t/sum_by_job/keys/1s": {"aggregations": [{"aggregation_id": 4}]}
            },
        }
        result = self.start_times(
            config, runtime_info({3: 1_000}), runtime_info({3: 1_000, 4: 0})
        )
        self.assertEqual(result, {QUERY: 1.0})

    def test_required_ids_with_empty_sections(self) -> None:
        self.assertEqual(
            self.main.required_aggregation_ids({"queries": None, "rqe_configs": {}}),
            set(),
        )


if __name__ == "__main__":
    unittest.main()
