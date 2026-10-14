"""Tests for the rqe protocol client and its side file."""

import importlib.util
import json
import os
import tempfile
import unittest
from typing import Any, Dict
from unittest import mock

import requests

from classes.config import ServerConfig
from classes.prometheus_query_client import normalize_vector
from classes.query_client_factory import QueryClientFactory
from classes.rqe_query_client import (
    RQE_QUERY_PATH,
    RqeQueryClient,
    load_rqe_exprs,
    resolve_rqe_exprs_file,
)

URL = "http://sketchdb:8088"
PROMQL = "sum by (job) (data)"
SUM_BY_JOB = {
    "rqe": {
        "id": "t/sum_by_job/keys/1s",
        "metric": "data",
        "capability": "sum",
        "lookback_ms": 100,
        "grouping": ["job"],
    }
}
SQL_TEMPLATE = "SELECT job, SUM(v) FROM t WHERE ts <= '{{ end_time }}' GROUP BY job"
VECTOR = [
    {"metric": {"job": "a"}, "value": [1791885600, "3"]},
    {"metric": {"job": "b"}, "value": [1791885600, "4.5"]},
]


def response(status_code: int, body: Any) -> mock.Mock:
    r = mock.Mock(spec=requests.Response)
    r.status_code = status_code
    r.json.return_value = body
    r.text = json.dumps(body)
    return r


class RqeQueryClientTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.side_file = os.path.join(self.tmp.name, "rqe_exprs.json")
        self.write_side_file({PROMQL: SUM_BY_JOB, SQL_TEMPLATE: SUM_BY_JOB})
        self.client = RqeQueryClient(URL, "sketchdb", self.side_file)
        self.post = mock.Mock(
            return_value=response(
                200,
                {
                    "status": "success",
                    "data": {"resultType": "vector", "result": VECTOR},
                },
            )
        )
        self.client.session.post = self.post  # type: ignore[method-assign]

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def write_side_file(self, exprs: Any) -> None:
        with open(self.side_file, "w") as f:
            json.dump(exprs, f)

    def test_posts_the_tree_and_parses_like_prometheus(self) -> None:
        result = self.client.execute_query(PROMQL, query_time=1791885600)
        self.post.assert_called_once()
        args, kwargs = self.post.call_args
        self.assertEqual(args, (URL + RQE_QUERY_PATH,))
        self.assertEqual(kwargs["json"], {"expr": SUM_BY_JOB, "time": 1791885600.0})
        self.assertTrue(result.success)
        # raw_response is what QueryResult.result stores for Prometheus.
        self.assertEqual(result.raw_response, VECTOR)
        self.assertEqual(result.data, normalize_vector(VECTOR))

    def test_looks_up_the_unrendered_config_text(self) -> None:
        rendered = SQL_TEMPLATE.replace("{{ end_time }}", "2026-10-13 10:00:00")
        result = self.client.execute_query(
            rendered, query_time=1791885600, config_query=SQL_TEMPLATE
        )
        self.assertTrue(result.success, result.error_message)

    def test_query_missing_from_side_file_fails_without_a_request(self) -> None:
        result = self.client.execute_query("rate(data_total[15m])", query_time=1)
        self.assertFalse(result.success)
        assert result.error_message is not None
        self.assertIn(
            "no RQE expression for query 'rate(data_total[15m])'", result.error_message
        )
        self.assertIn(self.side_file, result.error_message)
        self.post.assert_not_called()

    def test_rendered_text_is_not_a_key(self) -> None:
        # Only the config's text is a key; a rendered template never matches.
        rendered = SQL_TEMPLATE.replace("{{ end_time }}", "2026-10-13 10:00:00")
        result = self.client.execute_query(rendered, query_time=1791885600)
        self.assertFalse(result.success)
        self.post.assert_not_called()

    def test_needs_an_evaluation_time(self) -> None:
        result = self.client.execute_query(PROMQL)
        self.assertFalse(result.success)
        self.post.assert_not_called()

    def test_bad_request_is_a_failure(self) -> None:
        self.post.return_value = response(
            400,
            {"status": "error", "errorType": "bad_data", "error": "unknown RQE id"},
        )
        result = self.client.execute_query(PROMQL, query_time=1)
        self.assertFalse(result.success)
        assert result.error_message is not None
        self.assertIn("HTTP 400", result.error_message)
        self.assertIn("unknown RQE id", result.error_message)

    def test_error_status_in_a_200_is_a_failure(self) -> None:
        self.post.return_value = response(200, {"status": "error", "error": "x"})
        self.assertFalse(self.client.execute_query(PROMQL, query_time=1).success)

    def test_connection_error_is_a_failure(self) -> None:
        self.post.side_effect = requests.exceptions.ConnectionError("refused")
        result = self.client.execute_query(PROMQL, query_time=1)
        self.assertFalse(result.success)
        assert result.error_message is not None
        self.assertIn("ConnectionError", result.error_message)

    def test_empty_result_is_a_success(self) -> None:
        self.post.return_value = response(
            200, {"status": "success", "data": {"resultType": "vector", "result": []}}
        )
        result = self.client.execute_query(PROMQL, query_time=1)
        self.assertTrue(result.success)
        self.assertEqual(result.data, {})

    def test_side_file_must_be_an_object(self) -> None:
        self.write_side_file([SUM_BY_JOB])
        with self.assertRaisesRegex(ValueError, "expected a JSON object"):
            load_rqe_exprs(self.side_file)

    def test_factory_creates_rqe_clients(self) -> None:
        client = QueryClientFactory.create(
            "rqe", URL, "sketchdb", rqe_exprs_file=self.side_file
        )
        self.assertIsInstance(client, RqeQueryClient)
        self.assertEqual(client.protocol_name, "rqe")


class RqeServerConfigTest(unittest.TestCase):
    def test_rqe_needs_a_side_file(self) -> None:
        with self.assertRaisesRegex(ValueError, "needs rqe_exprs_file"):
            ServerConfig.from_dict({"name": "sketchdb", "url": URL, "protocol": "rqe"})

    def test_side_file_read_from_server_config(self) -> None:
        server = ServerConfig.from_dict(
            {
                "name": "sketchdb",
                "url": URL,
                "protocol": "rqe",
                "rqe_exprs_file": "rqe_exprs.json",
            }
        )
        self.assertEqual(server.rqe_exprs_file, "rqe_exprs.json")

    def test_relative_side_file_is_next_to_the_config(self) -> None:
        self.assertEqual(
            resolve_rqe_exprs_file(
                "/app/prometheus_client_config.yaml", "rqe_exprs.json"
            ),
            "/app/rqe_exprs.json",
        )
        self.assertEqual(
            resolve_rqe_exprs_file("/app/c.yaml", "/data/rqe_exprs.json"),
            "/data/rqe_exprs.json",
        )


@unittest.skipUnless(
    importlib.util.find_spec("promql_utilities") is not None,
    "main_prometheus_client needs asap-common's promql_utilities",
)
class ExecuteSingleQueryTest(unittest.TestCase):
    def test_client_gets_the_rendered_and_the_config_text(self) -> None:
        from main_prometheus_client import execute_single_query

        client = mock.Mock()
        client.execute_query.return_value = mock.Mock(success=True, raw_response=VECTOR)
        result = execute_single_query(
            "sketchdb",
            client,
            SQL_TEMPLATE,
            query_idx=0,
            repetition_idx=0,
            query_unix_time=1791885600,
            dry_run=False,
            query_group_idx=0,
            time_window_seconds=0,
        )
        kwargs: Dict[str, Any] = client.execute_query.call_args.kwargs
        self.assertEqual(kwargs["config_query"], SQL_TEMPLATE)
        self.assertNotIn("{{", kwargs["query"])
        # QueryResult keys the vector by label set, as for Prometheus.
        self.assertEqual(result.result, normalize_vector(VECTOR))


if __name__ == "__main__":
    unittest.main()
