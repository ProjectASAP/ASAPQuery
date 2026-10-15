"""Tests for each ClickHouse mode's query groups in its client config."""

import os
import tempfile
import unittest

import yaml

from experiment_utils.config import generate_clickhouse_client_configs

URLS = {"baseline": "http://localhost:8123", "sketchdb": "http://localhost:8088"}


class ModeSqlFilesTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()

    def tearDown(self):
        self.tmp.cleanup()

    def sql(self, name):
        path = os.path.join(self.tmp.name, f"{name}.sql")
        with open(path, "w") as f:
            f.write(f"SELECT '{name}';\n")
        return path

    def client_queries(self, groups, mode_server_urls=URLS):
        generate_clickhouse_client_configs(groups, self.tmp.name, mode_server_urls)
        out = {}
        for mode in mode_server_urls:
            path = os.path.join(
                self.tmp.name, "controller_client_configs", f"{mode}.yaml"
            )
            with open(path) as f:
                out[mode] = [
                    (g["id"], g["queries"]) for g in yaml.safe_load(f)["query_groups"]
                ]
        return out

    def test_sketchdb_runs_its_own_sql(self):
        groups = [{"sql_file": {"baseline": self.sql("b"), "sketchdb": self.sql("s")}}]
        self.assertEqual(
            self.client_queries(groups),
            {"baseline": [(0, ["SELECT 'b'"])], "sketchdb": [(0, ["SELECT 's'"])]},
        )

    def test_sketchdb_skips_trailing_groups_without_its_sql(self):
        # Its queries keep baseline's indices, which pair the modes' results.
        groups = [
            {"sql_file": {"baseline": self.sql("b0"), "sketchdb": self.sql("s0")}},
            {"sql_file": {"baseline": self.sql("b1")}},
        ]
        self.assertEqual(
            self.client_queries(groups)["sketchdb"], [(0, ["SELECT 's0'"])]
        )

    def test_sketchdb_skipping_a_leading_group_is_refused(self):
        # Its later queries would take the skipped group's indices.
        groups = [
            {"sql_file": {"baseline": self.sql("b0")}},
            {"sql_file": {"baseline": self.sql("b1"), "sketchdb": self.sql("s1")}},
        ]
        with self.assertRaisesRegex(ValueError, "only trailing groups"):
            self.client_queries(groups)

    def test_sketchdb_without_any_sql_is_refused(self):
        groups = [{"sql_file": {"baseline": self.sql("b")}}]
        with self.assertRaisesRegex(ValueError, "No query group .* 'sketchdb'"):
            self.client_queries(groups)

    def test_statement_count_mismatch_is_refused(self):
        # sketchdb's later queries would be scored against the wrong
        # baseline queries.
        two = os.path.join(self.tmp.name, "two.sql")
        with open(two, "w") as f:
            f.write("SELECT 1;\nSELECT 2;\n")
        groups = [{"sql_file": {"baseline": two, "sketchdb": self.sql("s")}}]
        with self.assertRaisesRegex(ValueError, "different numbers of statements"):
            self.client_queries(groups)

    def test_plain_string_sql_file_is_refused_clearly(self):
        groups = [{"sql_file": self.sql("b")}]
        with self.assertRaisesRegex(ValueError, "must map mode name"):
            self.client_queries(groups, {"baseline": URLS["baseline"]})

    def test_other_modes_need_every_group(self):
        groups = [
            {"sql_file": {"baseline": self.sql("b0")}},
            {"sql_file": {"baseline_mv": self.sql("m1")}},
        ]
        with self.assertRaisesRegex(
            ValueError, "missing sql_file entry for mode 'baseline'"
        ):
            self.client_queries(groups, {"baseline": URLS["baseline"]})


if __name__ == "__main__":
    unittest.main()
