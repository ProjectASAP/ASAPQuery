"""Tests for running e2e experiments on prebuilt planner configs."""

import os
import tempfile
import unittest

import yaml

from experiment_utils.config import copy_prebuilt_controller_output

QUERY = "sum by (job) (data)"
SQL_QUERY = "SELECT count() FROM hits"


class CopyPrebuiltControllerOutputTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.src = os.path.join(self.tmp.name, "src")
        self.dest = os.path.join(self.tmp.name, "dest")
        self.client = os.path.join(self.tmp.name, "client.yaml")
        os.makedirs(self.src)
        self.write(
            os.path.join(self.src, "inference_config.yaml"),
            {"queries": [{"query": QUERY}]},
        )
        self.write(
            os.path.join(self.src, "streaming_config.yaml"),
            {"aggregations": [{"aggregationId": 1, "slideIntervalMs": 1000}]},
        )
        self.write(self.client, {"query_groups": [{"queries": [QUERY]}]})

    def tearDown(self):
        self.tmp.cleanup()

    @staticmethod
    def write(path, content):
        with open(path, "w") as f:
            yaml.safe_dump(content, f)

    def copy(self, src=None, dest=None, scrape_interval_ms=1000):
        copy_prebuilt_controller_output(
            src or self.src, dest or self.dest, self.client, scrape_interval_ms
        )

    def copy_sql(self, src=None, dest=None, scrape_interval_ms=1000):
        copy_prebuilt_controller_output(
            src or self.src,
            dest or self.dest,
            self.client,
            scrape_interval_ms,
            query_language="sql",
        )

    def test_replaces_stale_output(self):
        os.makedirs(self.dest)
        with open(os.path.join(self.dest, "controller.log"), "w") as f:
            f.write("stale")
        self.copy()
        self.assertEqual(
            sorted(os.listdir(self.dest)),
            ["inference_config.yaml", "streaming_config.yaml"],
        )

    def test_refuses_missing_config(self):
        os.remove(os.path.join(self.src, "inference_config.yaml"))
        with self.assertRaisesRegex(ValueError, "inference_config.yaml"):
            self.copy()

    def test_refuses_nested_dirs(self):
        for src, dest in [
            (self.src, self.src),
            (self.src, os.path.dirname(self.src)),
            (self.src, os.path.join(self.src, "out")),
        ]:
            with self.assertRaisesRegex(ValueError, "contain one another"):
                self.copy(src, dest)
        self.assertTrue(os.path.isfile(os.path.join(self.src, "streaming_config.yaml")))

    def test_accepts_arithmetic_over_planned_leaves(self):
        self.write(
            self.client,
            {"query_groups": [{"queries": [f"({QUERY}) / {QUERY} * 100"]}]},
        )
        self.copy()

    def test_refuses_an_unplanned_query(self):
        self.write(self.client, {"query_groups": [{"queries": [QUERY, "other"]}]})
        with self.assertRaisesRegex(ValueError, "other"):
            self.copy()

    def test_refuses_another_scrape_interval(self):
        with self.assertRaisesRegex(ValueError, "15000 ms scrape interval"):
            self.copy(scrape_interval_ms=15000)

    def test_sql_output_requires_a_sql_schema(self):
        with self.assertRaisesRegex(ValueError, "not an SQL inference config"):
            self.copy_sql()

    def test_sql_output_with_a_sql_schema_is_copied(self):
        self.write(self.client, {"query_groups": [{"queries": [SQL_QUERY]}]})
        self.write(
            os.path.join(self.src, "inference_config.yaml"),
            {
                "tables": [
                    {
                        "name": "hits",
                        "time_column": "EventTime",
                        "value_columns": ["ResolutionWidth"],
                        "metadata_columns": [],
                    }
                ],
                "queries": [{"query": SQL_QUERY}],
            },
        )

        self.copy_sql()

        self.assertTrue(
            os.path.isfile(os.path.join(self.dest, "inference_config.yaml"))
        )

    def test_sql_output_refuses_an_invalid_table_schema(self):
        self.write(
            os.path.join(self.src, "inference_config.yaml"),
            {"tables": [{}], "queries": [{"query": QUERY}]},
        )

        with self.assertRaisesRegex(ValueError, "invalid table schema"):
            self.copy_sql()


if __name__ == "__main__":
    unittest.main()
