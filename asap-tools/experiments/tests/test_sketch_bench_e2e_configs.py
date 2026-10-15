"""Tests for sketch_bench_e2e_configs.py (e2e configs from a sketch-bench table)."""

import json
import os
import shutil
import tempfile
import unittest

import yaml
from hydra import compose, initialize_config_dir
from omegaconf import OmegaConf

from experiment_utils import config as experiment_config
from experiment_utils.core import read_exporter_config
from experiment_utils.services.paced_feeder import (
    aligned_interval_lcm,
    check_paced_feeder_config,
    set_data_start,
)
from sketch_bench_e2e import sketch_bench_e2e_configs as sbe
from test_paced_feeder import E2E_METRICS

CONFIG_DIR = os.path.join(sbe.EXPERIMENTS_DIR, "config")
PREFIX = "synthetic/templates=classic/shared=1"


def leaf(capability, rqe_id, lookback_ms, grouping, **extra):
    return {
        "rqe": {
            "capability": capability,
            "grouping": grouping,
            "id": rqe_id,
            "lookback_ms": lookback_ms,
            "metric": "data",
            **extra,
        }
    }


SUM_BY_JOB = leaf("sum", f"{PREFIX}/sum_by_job/keys/1s", 100, ["job"])
SUM_OVER_TIME = leaf(
    "sum", f"{PREFIX}/sum_over_time/keys/15m", 900000, ["job", "label_0"]
)
QUANTILE_BY_JOB = leaf(
    "quantile", f"{PREFIX}/quantile_by_job_p0.5/values/1s", 100, ["job"], phi=0.5
)
DISTINCT_SRC = {
    "rqe": {
        "capability": "cardinality",
        "grouping": ["dst_subnet"],
        "id": "synthetic/templates=all/shared=1/t11_distinct_src/keys/5m",
        "lookback_ms": 300000,
        "metric": "flows",
        "value": "src_ip",
    }
}
SQL_STATEMENT = "SELECT dst_subnet, uniq(src_ip) FROM flows GROUP BY dst_subnet"

# A slice of synthetic-templatesclassic-shared1.json's e2e.queries, plus a
# structured entry from the multi-grouping tables. `rate` has no `expr`.
QUERIES = [
    {
        "expr": SUM_BY_JOB,
        "interval_secs": 1,
        "promql": "sum by (job) (data)",
        "rqes": [f"{PREFIX}/sum_by_job/keys/1s"],
    },
    {
        "expr": SUM_OVER_TIME,
        "interval_secs": 60,
        "promql": "sum_over_time(data[15m])",
        "rqes": [f"{PREFIX}/sum_over_time/keys/15m"],
    },
    {
        "expr": QUANTILE_BY_JOB,
        "interval_secs": 1,
        "promql": "quantile by (job) (0.5, data)",
        "rqes": [f"{PREFIX}/quantile_by_job_p0.5/values/1s"],
    },
    {
        "interval_secs": 60,
        "promql": "rate(data_total[15m])",
        "rqes": [f"{PREFIX}/rate/keys/15m"],
    },
    {
        "capability": "cardinality",
        "expr": DISTINCT_SRC,
        "grouping": ["dst_subnet"],
        "interval_secs": 60,
        "rqes": ["synthetic/templates=all/shared=1/t11_distinct_src/keys/5m"],
        "sql": "SELECT dst_subnet, COUNT(DISTINCT src_ip) FROM flows GROUP BY dst_subnet",
        "table": "flows",
        "value": "src_ip",
        "window_secs": 300,
    },
]


class BuildConfigTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.table = os.path.join(self.tmp.name, "table.json")
        with open(self.table, "w") as f:
            json.dump(
                {"workloads": [{"e2e": {"metrics": E2E_METRICS, "queries": QUERIES}}]},
                f,
            )

    def tearDown(self):
        self.tmp.cleanup()

    def build(self, **kwargs):
        if "sql_queries_file" in kwargs and "prebuilt_output_dir" not in kwargs:
            prebuilt = os.path.join(self.tmp.name, "sql_prebuilt")
            os.makedirs(prebuilt, exist_ok=True)
            for name in experiment_config.PLANNER_OUTPUT_FILES:
                open(os.path.join(prebuilt, name), "w").close()
            kwargs["prebuilt_output_dir"] = prebuilt
        return sbe.build_config(
            self.table,
            seed=7,
            starting_delay_secs=120,
            query_duration_secs=600,
            **kwargs,
        )

    def test_query_groups_by_interval_in_table_order(self):
        config, _, _ = self.build()
        groups = config["experiment_params"]["query_groups"]
        self.assertEqual(
            [(g["id"], g["repetition_delay_ms"], g["queries"]) for g in groups],
            [
                (1, 1000, ["sum by (job) (data)", "quantile by (job) (0.5, data)"]),
                (2, 60000, ["sum_over_time(data[15m])", "rate(data_total[15m])"]),
            ],
        )
        self.assertEqual(
            [g["client_options"] for g in groups],
            [
                {
                    "repetitions": repetitions,
                    "starting_delay": 120,
                    "query_time_offset": sbe.DEFAULT_QUERY_TIME_OFFSET_SECS,
                    "align_to_interval": True,
                }
                for repetitions in (600, 10)
            ],
        )

    def test_feeder_sends_the_classic_metrics_without_sql(self):
        config, notes, _ = self.build()
        params = config["experiment_params"]
        self.assertEqual(
            params["metrics"],
            [
                {
                    "metric": "data",
                    "labels": ["job", "label_0"],
                    "exporter": "paced_feeder",
                },
                {
                    "metric": "data_total",
                    "labels": ["job", "label_0"],
                    "exporter": "paced_feeder",
                },
            ],
        )
        self.assertEqual(
            params["exporters"]["exporter_list"]["paced_feeder"],
            {
                "workload_table": self.table,
                "metrics": ["data", "data_total"],
                "seed": 7,
                "duration_secs": 120 + 600 + sbe.FEEDER_SLACK_SECS,
            },
        )
        self.assertIn(
            "skipped records metric 'flows': only structured queries read it", notes
        )
        self.assertEqual(
            [m["mode"] for m in params["experiment"]], ["sketchdb", "baseline"]
        )

    def test_structured_queries_skipped_without_sql(self):
        config, notes, _ = self.build()
        self.assertEqual(len(config["experiment_params"]["query_groups"]), 2)
        self.assertIn(
            "skipped 1 structured (multi-grouping) queries: no --sql-queries file",
            notes,
        )

    def test_structured_queries_from_sql_file(self):
        sql = os.path.join(self.tmp.name, "multigroup.sql")
        with open(sql, "w") as f:
            f.write("SELECT dst_subnet, uniq(src_ip) FROM flows GROUP BY dst_subnet;\n")
        config, notes, _ = self.build(sql_queries_file=sql)
        group = config["experiment_params"]["query_groups"][2]
        self.assertEqual(group["id"], 3)
        self.assertEqual(group["repetition_delay_ms"], 60000)
        self.assertEqual(
            group["queries"],
            ["SELECT dst_subnet, uniq(src_ip) FROM flows GROUP BY dst_subnet"],
        )
        # The client refuses to render {{ end_time }} templates without a
        # window; the SQL carries its own, so 0. PromQL groups have none.
        self.assertEqual(group["time_window_seconds"], 0)
        self.assertNotIn(
            "time_window_seconds", config["experiment_params"]["query_groups"][0]
        )
        self.assertFalse(any("structured" in n for n in notes))
        # The records metric goes to ASAPQuery only: fed, but no baseline
        # mode and not under `metrics`.
        params = config["experiment_params"]
        self.assertEqual(
            params["exporters"]["exporter_list"]["paced_feeder"]["metrics"],
            ["data", "data_total", "flows"],
        )
        self.assertEqual([m["mode"] for m in params["experiment"]], ["sketchdb"])
        self.assertEqual(
            [m["metric"] for m in params["metrics"]], ["data", "data_total"]
        )
        self.assertIn(
            "no baseline mode: records metrics ['flows'] go to ASAPQuery only", notes
        )

    def test_records_rate_only_with_records_metrics(self):
        sql = os.path.join(self.tmp.name, "multigroup.sql")
        with open(sql, "w") as f:
            f.write("SELECT 1;\n")
        config, _, _ = self.build(sql_queries_file=sql, records_per_sec=1000)
        feeder = config["experiment_params"]["exporters"]["exporter_list"]
        self.assertEqual(feeder["paced_feeder"]["records_per_sec"], 1000)
        config, _, _ = self.build(records_per_sec=1000)
        feeder = config["experiment_params"]["exporters"]["exporter_list"]
        self.assertNotIn("records_per_sec", feeder["paced_feeder"])

    def test_sql_file_must_match_the_structured_queries(self):
        sql = os.path.join(self.tmp.name, "multigroup.sql")
        with open(sql, "w") as f:
            f.write("SELECT 1;\nSELECT 2;\n")
        with self.assertRaisesRegex(ValueError, "2 statements; the table has 1"):
            self.build(sql_queries_file=sql)

    def test_query_phase_shorter_than_an_interval(self):
        with self.assertRaisesRegex(ValueError, "shorter than the 60 s"):
            sbe.build_config(
                self.table, seed=7, starting_delay_secs=0, query_duration_secs=30
            )

    def test_sql_queries_need_prebuilt_planner_configs(self):
        # The planner rejects the SQL groups' time_window_seconds.
        with self.assertRaisesRegex(ValueError, "--prebuilt-output-dir"):
            sbe.build_config(
                self.table,
                seed=7,
                starting_delay_secs=120,
                query_duration_secs=600,
                sql_queries_file=os.path.join(self.tmp.name, "q.sql"),
            )

    def test_prebuilt_output_dir(self):
        prebuilt = os.path.join(self.tmp.name, "prebuilt")
        os.makedirs(prebuilt)
        with self.assertRaisesRegex(ValueError, "streaming_config.yaml"):
            self.build(prebuilt_output_dir=prebuilt)
        for name in experiment_config.PLANNER_OUTPUT_FILES:
            open(os.path.join(prebuilt, name), "w").close()
        config, _, _ = self.build(prebuilt_output_dir=prebuilt)
        self.assertEqual(config["controller"], {"prebuilt_output_dir": prebuilt})

    def write_prebuilt(self, inference_config):
        prebuilt = os.path.join(self.tmp.name, "prebuilt")
        os.makedirs(prebuilt, exist_ok=True)
        open(os.path.join(prebuilt, "streaming_config.yaml"), "w").close()
        with open(os.path.join(prebuilt, "inference_config.yaml"), "w") as f:
            yaml.safe_dump(inference_config, f)
        return prebuilt

    RQE_CONFIGS = {
        f"{PREFIX}/sum_by_job/keys/1s": {
            "aggregations": [{"aggregation_id": 1, "num_aggregates_to_retain": 1}],
            "query_time_aggregations": [],
        }
    }

    def test_rqe_accepts_an_expr_only_plan(self):
        for queries in ({}, {"queries": []}):
            prebuilt = self.write_prebuilt({**queries, "rqe_configs": self.RQE_CONFIGS})
            config, _, _ = self.build(prebuilt_output_dir=prebuilt, frontend="rqe")
            self.assertEqual(config["controller"], {"prebuilt_output_dir": prebuilt})

    def test_rqe_refuses_a_plan_with_query_configs(self):
        prebuilt = self.write_prebuilt(
            {
                "queries": [{"query": "rate(data_total[15m])", "aggregations": []}],
                "rqe_configs": self.RQE_CONFIGS,
            }
        )
        with self.assertRaisesRegex(
            ValueError,
            r"is not an --expr-only plan \(per-query configs under 'queries'\).*"
            r"`asap-planner --planner saved --expr-only`",
        ):
            self.build(prebuilt_output_dir=prebuilt, frontend="rqe")
        # PromQL runs the per-query configs, so the same plan is fine there.
        self.build(prebuilt_output_dir=prebuilt)

    def test_rqe_refuses_a_plan_without_rqe_configs(self):
        for inference_config in (
            {"queries": [{"query": "sum by (job) (data)"}]},
            {"queries": [], "rqe_configs": {}},
            None,
        ):
            prebuilt = self.write_prebuilt(inference_config)
            with self.assertRaisesRegex(
                ValueError,
                r"is not an --expr-only plan \(no rqe_configs.*"
                r"`asap-planner --planner saved --expr-only`",
            ):
                self.build(prebuilt_output_dir=prebuilt, frontend="rqe")

    def compose_and_validate(self, built, overrides=()):
        # A dry run of what experiment_run_e2e.py does with the file before it
        # starts any service; returns the experiment modes.
        config, _, rqe_exprs = built
        config_dir = os.path.join(self.tmp.name, "config")
        shutil.rmtree(config_dir, ignore_errors=True)
        shutil.copytree(CONFIG_DIR, config_dir)
        sbe.write_config(
            config,
            rqe_exprs,
            self.table,
            os.path.join(config_dir, "experiment_type", "sketch_bench_e2e", "t.yaml"),
        )
        with initialize_config_dir(version_base=None, config_dir=config_dir):
            cfg = compose(
                "config", overrides=["experiment_type=sketch_bench_e2e/t", *overrides]
            )
        params = cfg.experiment_params
        experiment_config.validate_experiment_config(params)
        exporter_config, reason = read_exporter_config(params)
        self.assertIsNotNone(exporter_config, reason)
        check_paced_feeder_config(
            OmegaConf.to_container(exporter_config.exporter_list.paced_feeder),
            modes=OmegaConf.to_container(params.experiment),
            parallelism=cfg.streaming.parallelism,
            min_feed_secs=cfg.flow.steady_state_wait
            + experiment_config.get_minimum_experiment_running_time(params),
            # The generated config overrides config.yaml's `true`.
            forward_unsupported_queries=cfg.streaming.forward_unsupported_queries,
            auto_discover_labels=cfg.controller.auto_discover_labels,
        )
        modes, _ = experiment_config.generate_controller_client_configs(
            params,
            self.tmp.name,
            rqe_exprs_file=experiment_config.rqe_exprs_file_for_frontend(
                cfg.query_engine
            ),
        )
        return modes

    def test_generated_config_composes_and_validates(self):
        self.assertEqual(
            self.compose_and_validate(self.build()), ["sketchdb", "baseline"]
        )

    def test_runner_can_anchor_the_generated_client_config(self):
        # What experiment_run_e2e.py does to the mode's client config once the
        # feeder starts: every group must learn where the data starts.
        self.compose_and_validate(self.build())
        path = os.path.join(self.tmp.name, "controller_client_configs", "sketchdb.yaml")
        with open(path) as f:
            client_config = yaml.safe_load(f)
        self.assertEqual(aligned_interval_lcm(client_config), 60)
        set_data_start(client_config, 1791885600000)
        self.assertEqual(
            {
                g["client_options"]["data_start_time"]
                for g in client_config["query_groups"]
            },
            {1791885600},
        )

    def test_generated_config_with_records_validates(self):
        sql = os.path.join(self.tmp.name, "multigroup.sql")
        with open(sql, "w") as f:
            f.write("SELECT 1;\n")
        built = self.build(sql_queries_file=sql)
        self.assertEqual(self.compose_and_validate(built), ["sketchdb"])

    def client_config(self, mode):
        path = os.path.join(self.tmp.name, "controller_client_configs", f"{mode}.yaml")
        with open(path) as f:
            return yaml.safe_load(f)

    def write_sql_file(self):
        sql = os.path.join(self.tmp.name, "multigroup.sql")
        with open(sql, "w") as f:
            f.write(SQL_STATEMENT + ";\n")
        return sql

    def test_side_file_maps_config_text_to_expr(self):
        config, _, rqe_exprs = self.build()
        # `rate` has no expr: it stays in the PromQL config, not in the side file.
        self.assertEqual(
            rqe_exprs,
            {
                "sum by (job) (data)": SUM_BY_JOB,
                "sum_over_time(data[15m])": SUM_OVER_TIME,
                "quantile by (job) (0.5, data)": QUANTILE_BY_JOB,
            },
        )
        queries = [
            q for g in config["experiment_params"]["query_groups"] for q in g["queries"]
        ]
        self.assertIn("rate(data_total[15m])", queries)
        self.assertNotIn("query_engine", config)

    def test_side_file_keys_sql_by_the_statement_in_the_config(self):
        # Not by the table's own `sql`: the config runs the --sql-queries text.
        config, _, rqe_exprs = self.build(sql_queries_file=self.write_sql_file())
        self.assertEqual(rqe_exprs[SQL_STATEMENT], DISTINCT_SRC)
        self.assertEqual(
            config["experiment_params"]["query_groups"][2]["queries"], [SQL_STATEMENT]
        )
        self.assertNotIn(QUERIES[4]["sql"], rqe_exprs)

    def test_rqe_frontend_leaves_out_queries_without_expr(self):
        config, notes, rqe_exprs = self.build(frontend="rqe")
        groups = config["experiment_params"]["query_groups"]
        self.assertEqual(
            [g["queries"] for g in groups],
            [
                ["sum by (job) (data)", "quantile by (job) (0.5, data)"],
                ["sum_over_time(data[15m])"],
            ],
        )
        self.assertIn(
            "skipped 1 queries without an RQE expression (--frontend rqe): "
            "['rate(data_total[15m])']",
            notes,
        )
        self.assertEqual(set(rqe_exprs), {q for g in groups for q in g["queries"]})
        self.assertEqual(config["query_engine"], {"frontend": "rqe"})

    def test_unknown_frontend_rejected(self):
        with self.assertRaisesRegex(ValueError, "unknown frontend 'sql'"):
            self.build(frontend="sql")

    def test_one_text_with_two_exprs_rejected(self):
        with open(self.table, "w") as f:
            json.dump(
                {
                    "workloads": [
                        {
                            "e2e": {
                                "metrics": E2E_METRICS,
                                "queries": [
                                    QUERIES[0],
                                    {**QUERIES[0], "expr": QUANTILE_BY_JOB},
                                ],
                            }
                        }
                    ]
                },
                f,
            )
        with self.assertRaisesRegex(ValueError, "two different exprs"):
            self.build()

    def test_side_file_written_next_to_the_config(self):
        config, _, rqe_exprs = self.build(frontend="rqe")
        output = os.path.join(self.tmp.name, "out", "t.yaml")
        sbe.write_config(config, rqe_exprs, self.table, output)
        side_file = os.path.join(self.tmp.name, "out", "t.rqe_exprs.json")
        with open(side_file) as f:
            self.assertEqual(json.load(f), rqe_exprs)
        with open(output) as f:
            written = yaml.safe_load(f)
        self.assertEqual(
            written["query_engine"],
            {"frontend": "rqe", "rqe_exprs_file": side_file},
        )

    def test_promql_frontend_keeps_the_prometheus_protocol(self):
        self.compose_and_validate(self.build())
        for mode in ("sketchdb", "baseline"):
            for server in self.client_config(mode)["servers"]:
                self.assertNotIn("protocol", server)
        self.assertFalse(
            os.path.exists(
                os.path.join(
                    self.tmp.name, "controller_client_configs", "rqe_exprs.json"
                )
            )
        )

    def test_rqe_frontend_switches_the_sketchdb_server(self):
        _, _, rqe_exprs = built = self.build(frontend="rqe")
        self.assertEqual(self.compose_and_validate(built), ["sketchdb", "baseline"])
        self.assertEqual(
            self.client_config("sketchdb")["servers"],
            [
                {
                    "name": "sketchdb",
                    "url": "http://localhost:8088",
                    "protocol": "rqe",
                    # Relative to the client config, which it travels with.
                    "rqe_exprs_file": "rqe_exprs.json",
                }
            ],
        )
        # The baseline keeps PromQL.
        self.assertEqual(
            self.client_config("baseline")["servers"],
            [{"name": "prometheus", "url": "http://localhost:9090"}],
        )
        with open(
            os.path.join(self.tmp.name, "controller_client_configs", "rqe_exprs.json")
        ) as f:
            self.assertEqual(json.load(f), rqe_exprs)

    def test_promql_config_has_no_side_file(self):
        config, _, rqe_exprs = self.build()
        output = os.path.join(self.tmp.name, "out", "t.yaml")
        sbe.write_config(config, rqe_exprs, self.table, output)
        self.assertFalse(os.path.exists(sbe.rqe_exprs_path(output)))
        with open(output) as f:
            self.assertNotIn("query_engine", yaml.safe_load(f))

    def test_rqe_switch_on_a_promql_config_needs_a_side_file(self):
        # A promql config has no side file, so it can't switch on the command
        # line alone; this fails before any service.
        with self.assertRaisesRegex(ValueError, "needs query_engine.rqe_exprs_file"):
            self.compose_and_validate(
                self.build(), overrides=["query_engine.frontend=rqe"]
            )

    def test_rqe_switch_fails_on_a_query_without_expr(self):
        # The rqe querier has no fallback, so this fails before any service.
        config, notes, rqe_exprs = self.build()
        side_file = os.path.join(self.tmp.name, "exprs.json")
        with open(side_file, "w") as f:
            json.dump(rqe_exprs, f)
        with self.assertRaisesRegex(
            ValueError, r"no RQE expression for \['rate\(data_total\[15m\]\)'\]"
        ):
            self.compose_and_validate(
                (config, notes, rqe_exprs),
                overrides=[
                    "query_engine.frontend=rqe",
                    f"query_engine.rqe_exprs_file={side_file}",
                ],
            )


class RqeFrontendConfigTest(unittest.TestCase):
    def test_promql_needs_no_side_file(self):
        self.assertIsNone(
            experiment_config.rqe_exprs_file_for_frontend(
                OmegaConf.create({"frontend": "promql", "rqe_exprs_file": None})
            )
        )

    def test_rqe_needs_a_side_file(self):
        with self.assertRaisesRegex(ValueError, "needs query_engine.rqe_exprs_file"):
            experiment_config.rqe_exprs_file_for_frontend(
                OmegaConf.create({"frontend": "rqe", "rqe_exprs_file": None})
            )

    def test_unknown_frontend_rejected(self):
        with self.assertRaisesRegex(ValueError, "got 'sql'"):
            experiment_config.rqe_exprs_file_for_frontend(
                OmegaConf.create({"frontend": "sql", "rqe_exprs_file": "x.json"})
            )

    def test_promql_only_runners_accept_promql(self):
        experiment_config.require_promql_frontend(
            OmegaConf.create({"frontend": "promql"}), "runner.py"
        )

    def test_promql_only_runners_refuse_rqe(self):
        with self.assertRaisesRegex(
            ValueError, "runner.py supports only query_engine.frontend='promql'"
        ):
            experiment_config.require_promql_frontend(
                OmegaConf.create({"frontend": "rqe"}), "runner.py"
            )

    def test_rqe_needs_a_sketchdb_mode(self):
        with tempfile.TemporaryDirectory() as tmp:
            side_file = os.path.join(tmp, "x.json")
            with open(side_file, "w") as f:
                json.dump({}, f)
            params = OmegaConf.create(
                {
                    "experiment": [{"mode": "baseline", "server": "prometheus"}],
                    "servers": [{"name": "prometheus", "url": "http://x"}],
                    "query_groups": [],
                    "metrics": [],
                }
            )
            with self.assertRaisesRegex(ValueError, "needs a 'sketchdb'"):
                experiment_config.generate_controller_client_configs(
                    params, tmp, rqe_exprs_file=side_file
                )


if __name__ == "__main__":
    unittest.main()
