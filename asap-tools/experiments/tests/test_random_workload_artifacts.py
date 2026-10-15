"""Tests for manifest-to-planner workload rendering."""

import math
import os
import random
import shutil
import subprocess
import sys
import json
import tempfile
import unittest
from pathlib import Path

import yaml
from hydra import compose, initialize_config_dir
from omegaconf import OmegaConf

from experiment_run_clickhouse import _check_clickhouse_paced_feeder
from experiment_utils import config as experiment_config
from experiment_utils.core import read_exporter_config
from experiment_utils.services.paced_feeder import check_paced_feeder_config

sys.path.insert(
    0, os.path.join(os.path.dirname(__file__), "..", "datasets", "fake_metric")
)

import random_workload_artifacts as artifacts  # noqa: E402
import random_workloads as workloads  # noqa: E402
import sketch_bench_workloads as sbw  # noqa: E402

PILOT_DIR = (
    Path(__file__).parent.parent
    / "datasets/fake_metric/random_workloads/seed-862/workload-01-pilot"
)
CLICKHOUSE = shutil.which("clickhouse")


def all_workloads():
    return [
        w
        for variant in workloads.VARIANTS
        for w in workloads.generate_workloads(seed=862, variant=variant)
    ] + workloads.generate_workloads(seed=862, pooled=True)


def pilot_dir_of(workload):
    set_dir = PILOT_DIR.parent.parent / artifacts.workload_set(workload)
    return set_dir / f"{workload['id']}-pilot"


def pilot():
    workload = workloads.generate_workloads(seed=862)[0]
    files = artifacts.pilot_configs(workload, PILOT_DIR)
    return workload, {path.name: content for path, content in files.items()}


class RenderArtifactsTest(unittest.TestCase):
    def test_promql_input_groups_non_sql_queries_by_repeat_interval(self):
        workload = workloads.generate_workloads(seed=862)[0]
        rendered = artifacts.render(workload)

        planner = rendered["promql_planner"]
        expected = [q for q in rendered["queries"] if q["promql"]]
        self.assertEqual(
            sum(len(group["queries"]) for group in planner["query_groups"]),
            len(expected),
        )
        self.assertEqual(
            {tuple(metric["labels"]) for metric in planner["metrics"]},
            {("region", "service", "job", "label_0")},
        )
        self.assertEqual(rendered["data_facts"], workload["data_facts"])
        for group in planner["query_groups"]:
            self.assertTrue(
                all(
                    query["interval_ms"] == group["repetition_delay_ms"]
                    for query in expected
                    if query["promql"] in group["queries"]
                )
            )

    def test_sql_input_contains_only_sql_queries_and_their_table_schema(self):
        rendered = artifacts.render(workloads.generate_workloads(seed=862)[0])

        self.assertTrue(rendered["sql_planner"]["query_groups"])
        self.assertEqual(rendered["sql_planner"]["tables"][0]["name"], "events")
        sql = [
            query
            for group in rendered["sql_planner"]["query_groups"]
            for query in group["queries"]
        ]
        self.assertTrue(all("COUNT(DISTINCT user_id)" in query for query in sql))

    def test_paced_feeder_table_preserves_all_manifest_data_facts(self):
        table = artifacts.paced_feeder_table(workloads.generate_workloads(seed=862)[0])
        metrics = table["workloads"][0]["e2e"]["metrics"]

        self.assertEqual(metrics["data"]["samples_per_sec_per_series"], 10)
        self.assertEqual(metrics["data"]["values"]["value"]["keys"], 10_000)
        self.assertEqual(metrics["data_total"]["type"], "counter")
        self.assertEqual(metrics["events"]["records_per_sec"], 100_000)
        self.assertEqual(
            metrics["events"]["values"]["user_id"]["rotating_pool"],
            {"universe_keys": 10_000, "keys_per_series": 1_000},
        )

    def test_archived_pilot_plans_read_the_manifests_pilot_queries(self):
        # An archived plan is only valid for the queries it was solved for.
        for workload in all_workloads():
            with self.subTest(workload=workload["id"], variant=workload.get("variant")):
                pilot_dir = pilot_dir_of(workload)
                with open(pilot_dir / "eval_table.json") as f:
                    e2e = json.load(f)["workloads"][0]["e2e"]
                # A pooled query is its tree; its text is client_promql.
                texts = [q.get("promql") or q["client_promql"] for q in e2e["queries"]]
                self.assertEqual(
                    [
                        (t, q["interval_secs"] * 1000)
                        for t, q in zip(texts, e2e["queries"])
                    ],
                    [
                        (q["promql"], q["interval_ms"])
                        for q in workload["queries"]
                        if q["support"] == "common-promql"
                    ],
                )
                # The planner's configs answer every pilot query: by text, or
                # (pooled, an expr-only plan) by each tree's RQEs.
                inference = yaml.safe_load(
                    (pilot_dir / "inference_config.yaml").read_text()
                )
                if workload.get("pooled"):
                    self.assertEqual(
                        set(inference["rqe_configs"]),
                        {r for q in e2e["queries"] for r in q["rqes"]},
                    )
                else:
                    self.assertEqual(
                        sorted(q["query"] for q in inference["queries"]),
                        sorted(texts),
                    )

    def test_every_workload_renders_its_pilot_configs(self):
        for workload in all_workloads():
            with self.subTest(workload=workload["id"], variant=workload.get("variant")):
                pilot_dir = pilot_dir_of(workload)
                files = artifacts.pilot_configs(workload, pilot_dir)
                self.assertEqual(
                    sum(path.suffix == ".yaml" for path in files), 4, sorted(files)
                )


class PilotConfigsTest(unittest.TestCase):
    def test_every_arm_feeds_the_same_data_and_schedule(self):
        workload, files = pilot()
        e2e = yaml.safe_load(files["e2e.yaml"])["experiment_params"]
        vm = yaml.safe_load(files["victoriametrics.yaml"])["experiment_params"]
        raw = yaml.safe_load(files["clickhouse.yaml"])
        mv = yaml.safe_load(files["clickhouse_mv.yaml"])

        self.assertEqual(
            [m["mode"] for m in e2e["experiment"]], ["sketchdb", "baseline"]
        )
        self.assertEqual([m["mode"] for m in vm["experiment"]], ["baseline"])
        self.assertEqual(vm["monitoring"]["tool"], "victoriametrics")
        self.assertEqual(raw["experiment"][0]["mode"], sbw.BASELINE)
        self.assertEqual(mv["experiment"][0]["mode"], sbw.BASELINE_MV)
        for config in (raw, mv):
            self.assertEqual(config["dataset"]["clickhouse_image_tag"], "23.8")
        self.assertIn(
            "prebuilt_output_dir", yaml.safe_load(files["e2e.yaml"])["controller"]
        )

        feeders = [
            e2e["exporters"]["exporter_list"]["paced_feeder"],
            vm["exporters"]["exporter_list"]["paced_feeder"],
            raw["dataset"]["paced_feeder"],
            mv["dataset"]["paced_feeder"],
        ]
        for feeder in feeders:
            self.assertEqual(
                (feeder["workload_table"], feeder["seed"], feeder["metrics"]),
                (str((PILOT_DIR / "eval_table.json").resolve()), 862, ["data"]),
            )
            # Runs stop feeders when their queries are done; the duration
            # only bounds a hung run, past a slow arm's lagging queries.
            self.assertEqual(feeder["duration_secs"], 1210 + 10 * 600)
        # The longest lookback (20 min) plus the freshness delay, then 10 min.
        schedules = [
            (g["repetition_delay_ms"], g["client_options"])
            for config in (e2e, vm, raw, mv)
            for g in config["query_groups"]
        ]
        self.assertEqual({ms for ms, _ in schedules}, {1_000, 20_000, 30_000})
        for ms, options in schedules:
            self.assertEqual(options["starting_delay"], 1210)
            self.assertEqual(options["query_time_offset"], 10)
            self.assertEqual(options["repetitions"], 600_000 // ms)
            self.assertTrue(options["align_to_interval"])

        pilot_sql = [
            q["clickhouse_sql"]
            for q in workload["queries"]
            if q["support"] == "common-promql"
        ]
        raw_sql = "".join(files[f"interval_{t}s_baseline.sql"] for t in (1, 20, 30))
        self.assertTrue(all(sql in raw_sql for sql in pilot_sql))

    def test_pooled_queries_reach_asapquery_as_trees_and_prometheus_as_text(self):
        workload = workloads.generate_workloads(seed=862, pooled=True)[0]
        files = artifacts.pilot_configs(workload, pilot_dir_of(workload))
        by_name = {p.name: c for p, c in files.items()}
        e2e = yaml.safe_load(by_name["e2e.yaml"])
        side = json.loads(by_name[artifacts.RQE_EXPRS_FILE])
        texts = [
            q for g in e2e["experiment_params"]["query_groups"] for q in g["queries"]
        ]
        self.assertEqual(e2e["query_engine"]["frontend"], "rqe")
        self.assertTrue(os.path.isabs(e2e["query_engine"]["rqe_exprs_file"]))
        self.assertEqual(set(side), set(texts))
        # The pooled quantile: one leaf at its grouping, under the avg-of-series
        # stand-in text Prometheus runs.
        stand_in = "quantile by (region) (0.95, quantile_over_time(0.95, data[100s]))"
        self.assertEqual(side[stand_in]["rqe"]["grouping"], ["region"])
        self.assertNotIn(
            "query_engine",
            yaml.safe_load(by_name["victoriametrics.yaml"]),
        )
        self.assertEqual({p.parent.name for p in files}, {"workload-01"})
        self.assertEqual({p.parent.parent.name for p in files}, {"seed-862-pooled"})

    def test_two_dashboards_repeat_every_group_on_the_same_plan(self):
        workload = workloads.generate_workloads(seed=862)[0]

        def configs(dashboards):
            files = artifacts.pilot_configs(workload, PILOT_DIR, dashboards)
            # The SQL files are the same, under the variant's directory.
            return {
                p.name: yaml.safe_load(c.replace("workload-01-dash2/", "workload-01/"))
                for p, c in files.items()
                if p.suffix == ".yaml"
            }, {p.parent.name for p in files}

        one, _ = configs(1)
        two, dirs = configs(2)
        self.assertEqual(dirs, {"workload-01-dash2"})
        for name, key in (
            ("e2e.yaml", "id"),
            ("clickhouse.yaml", "name"),
            ("clickhouse_mv.yaml", "name"),
        ):
            with self.subTest(config=name):
                single, double = one[name], two[name]
                params = (
                    (single["experiment_params"], double["experiment_params"])
                    if name == "e2e.yaml"
                    else (single, double)
                )
                groups, copies = (p.pop("query_groups") for p in params)
                # Same plan, feeder and data; only the groups repeat.
                self.assertEqual(double, single)
                # Each copy is its own group, so the client keeps its latencies apart.
                self.assertEqual(len({g[key] for g in copies}), 2 * len(groups))
                for g in groups + copies:
                    del g[key]
                self.assertEqual(copies, 2 * groups)


class PilotConfigsPreflightTest(unittest.TestCase):
    """Each config through Hydra and the checks its runner makes before it
    starts any service, from a copy of the config directory."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.cwd = os.getcwd()
        shutil.copytree(
            Path(artifacts.EXPERIMENTS_DIR) / "config", Path(self.tmp.name) / "config"
        )
        workload = workloads.generate_workloads(seed=862)[0]
        for path, content in artifacts.pilot_configs(workload, PILOT_DIR).items():
            (Path(self.tmp.name) / path).parent.mkdir(parents=True, exist_ok=True)
            (Path(self.tmp.name) / path).write_text(content)
        # Configs name their SQL relative to the runners' directory.
        os.chdir(self.tmp.name)

    def tearDown(self):
        os.chdir(self.cwd)
        self.tmp.cleanup()

    def compose(self, name):
        with initialize_config_dir(
            version_base=None, config_dir=os.path.join(self.tmp.name, "config")
        ):
            return compose(
                "config",
                overrides=[
                    f"experiment_type=random_workload/seed-862/workload-01/{name}"
                ],
            )

    def test_e2e_and_victoriametrics_configs_validate(self):
        for name, modes in (
            ("e2e", ["sketchdb", "baseline"]),
            ("victoriametrics", ["baseline"]),
        ):
            with self.subTest(config=name):
                cfg = self.compose(name)
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
                    forward_unsupported_queries=cfg.streaming.forward_unsupported_queries,
                    auto_discover_labels=cfg.controller.auto_discover_labels,
                )
                got, _ = experiment_config.generate_controller_client_configs(
                    params, os.path.join(self.tmp.name, name)
                )
                self.assertEqual(got, modes)

    def test_clickhouse_configs_validate(self):
        for name, mode in (
            ("clickhouse", sbw.BASELINE),
            ("clickhouse_mv", sbw.BASELINE_MV),
        ):
            with self.subTest(config=name):
                params = self.compose(name).experiment_params
                experiment_config.validate_experiment_config(params)
                out = os.path.join(self.tmp.name, name)
                servers = {s.name: s.url for s in params.servers}
                modes = experiment_config.generate_clickhouse_client_configs(
                    query_groups=params.query_groups,
                    local_experiment_dir=out,
                    mode_server_urls={mode: servers["clickhouse"]},
                    clickhouse_database="default",
                )
                self.assertEqual(modes, [mode])
                with open(
                    os.path.join(out, "controller_client_configs", f"{mode}.yaml")
                ) as f:
                    client_config = yaml.safe_load(f)
                align = _check_clickhouse_paced_feeder(
                    OmegaConf.to_container(params.dataset.paced_feeder),
                    modes,
                    client_config,
                    experiment_config.get_minimum_experiment_running_time(params),
                )
                self.assertEqual(align, 60)


@unittest.skipUnless(CLICKHOUSE, "clickhouse binary not found")
class PilotMvMatchesRawTest(unittest.TestCase):
    """The pilot's MV SQL answers what its raw SQL does, in clickhouse-local,
    over 12 series sampled every 100 ms for 22 min."""

    def test_mv_sql_matches_raw_sql(self):
        _, files = pilot()
        start_s, end_s = 1767225600, 1767225600 + 22 * 60
        rng = random.Random(3)
        series = [
            (f"r{r}", f"s{r}{s}", f"j{r}{s}", f"l{r}{s}{i}")
            for r in range(2)
            for s in range(2)
            for i in range(3)
        ]
        with tempfile.TemporaryDirectory() as tmp:
            csv = os.path.join(tmp, "data.csv")
            with open(csv, "w") as f:
                for t in range(start_s * 1000, end_s * 1000, 100):
                    for labels in series:
                        f.write(f"{t},{','.join(labels)},{rng.paretovariate(2.0)!r}\n")
            script = [
                files["init_mv.sql"],
                "INSERT INTO data SELECT fromUnixTimestamp64Milli(ts_ms), region, "
                f"service, job, label_0, value FROM file('{csv}', 'CSV', "
                "'ts_ms Int64, region String, service String, job String, "
                "label_0 String, value Float64');\n",
            ]
            for interval in (1, 20, 30):
                for mode in (sbw.BASELINE, sbw.BASELINE_MV):
                    text = files[f"interval_{interval}s_{mode}.sql"]
                    for i, sql in enumerate(s for s in text.split(";\n") if s.strip()):
                        # Eval times past the 20 min warm-up, on every interval.
                        for end in (end_s - 60, end_s):
                            sql_at = sql.replace(sbw.END, str(end))
                            script.append(
                                f"SELECT '{mode}/{interval}/{i}/{end}' AS qid, * "
                                f"FROM ({sql_at}) FORMAT JSONEachRow;\n"
                            )
            path = os.path.join(tmp, "script.sql")
            with open(path, "w") as f:
                f.write("".join(script))
            out = subprocess.run(
                [CLICKHOUSE, "local", "--queries-file", path],
                check=True,
                capture_output=True,
                text=True,
            ).stdout
        got = {}
        for line in out.splitlines():
            row = json.loads(line)
            mode, qid = row.pop("qid").split("/", 1)
            value = row.pop("value")
            got.setdefault(qid, {}).setdefault(mode, {})[tuple(row.items())] = value
        self.assertEqual(len(got), 7 * 2)
        for qid, by_mode in got.items():
            raw, mv = by_mode[sbw.BASELINE], by_mode[sbw.BASELINE_MV]
            self.assertTrue(raw, qid)
            self.assertEqual(sorted(raw), sorted(mv), qid)
            for key, value in raw.items():
                self.assertTrue(math.isclose(mv[key], value, rel_tol=1e-9), qid)


if __name__ == "__main__":
    unittest.main()
