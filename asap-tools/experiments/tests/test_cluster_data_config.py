"""Regression tests for cluster-data scrape and ingestion configuration."""

import tempfile
import unittest
from types import SimpleNamespace
from pathlib import Path

import yaml
from omegaconf import OmegaConf

import generate_prometheus_config
import generate_victoriametrics_config
from experiment_utils.config import get_prometheus_data_ingestion_interval_ms


EXPERIMENTS_DIR = Path(__file__).resolve().parents[1]


class ClusterDataConfigTest(unittest.TestCase):
    def _generate_config(self, experiment_config):
        args = SimpleNamespace(
            num_nodes=1,
            node_offset=18,
            output_dir=None,
            copy_to_dir=None,
            rule_files=None,
            query_log_file=None,
            prometheus_client_ip="10.10.1.19",
            node_ip_prefix="10.10.1",
            remote_write_url=None,
            remote_write_metric_names=None,
            remote_write_base_port=None,
            parallelism=None,
            scrape_interval="1s",
            evaluation_interval="10s",
            recording_rules_interval="5s",
        )
        with tempfile.TemporaryDirectory() as output_dir:
            args.output_dir = output_dir
            generate_prometheus_config.main(args, experiment_config)
            with open(f"{output_dir}/prometheus.yml") as config_file:
                return yaml.safe_load(config_file)

    def _generate_vmagent_config(self, experiment_config):
        args = SimpleNamespace(
            num_nodes=1,
            node_offset=18,
            output_dir=None,
            node_ip_prefix="10.10.1",
            scrape_interval="1s",
            remote_write_metric_names=None,
        )
        with tempfile.TemporaryDirectory() as output_dir:
            args.output_dir = output_dir
            generate_victoriametrics_config.main(args, experiment_config)
            with open(f"{output_dir}/vmagent_scrape.yml") as config_file:
                return yaml.safe_load(config_file)

    def test_google_uses_global_scrape_interval(self):
        with open(
            EXPERIMENTS_DIR / "config/experiment_type/cluster_data_google.yaml"
        ) as config_file:
            experiment_config = yaml.safe_load(config_file)

        prometheus_config = self._generate_config(experiment_config)
        cde_job = next(
            job
            for job in prometheus_config["scrape_configs"]
            if job["job_name"] == "cluster_data_exporter"
        )

        self.assertEqual(cde_job["scrape_interval"], "1s")
        self.assertEqual(cde_job["scrape_timeout"], "1s")
        self.assertEqual(
            get_prometheus_data_ingestion_interval_ms(
                OmegaConf.create({"scrape_interval": "1s"})
            ),
            1000,
        )

    def test_alibaba_uses_global_scrape_interval(self):
        with open(
            EXPERIMENTS_DIR
            / "config/experiment_type/cluster_data_alibaba_node_2021.yaml"
        ) as config_file:
            experiment_config = yaml.safe_load(config_file)

        prometheus_config = self._generate_config(experiment_config)
        cde_job = next(
            job
            for job in prometheus_config["scrape_configs"]
            if job["job_name"] == "cluster_data_exporter"
        )

        self.assertEqual(cde_job["scrape_interval"], "1s")
        self.assertEqual(
            get_prometheus_data_ingestion_interval_ms(
                OmegaConf.create({"scrape_interval": "1s"})
            ),
            1000,
        )

    def test_cluster_data_does_not_define_scrape_interval(self):
        with open(
            EXPERIMENTS_DIR
            / "config/experiment_type/cluster_data_alibaba_node_2022.yaml"
        ) as config_file:
            experiment_config = yaml.safe_load(config_file)
        cde_config = experiment_config["exporters"]["exporter_list"][
            "cluster_data_exporter"
        ]
        self.assertNotIn("scrape_interval", cde_config)

        prometheus_config = self._generate_config(experiment_config)
        cde_job = next(
            job
            for job in prometheus_config["scrape_configs"]
            if job["job_name"] == "cluster_data_exporter"
        )
        self.assertEqual(cde_job["scrape_interval"], "1s")

    def test_obsolete_cluster_data_interval_fails_loudly(self):
        with open(
            EXPERIMENTS_DIR
            / "config/experiment_type/cluster_data_alibaba_node_2022.yaml"
        ) as config_file:
            experiment_config = yaml.safe_load(config_file)
        experiment_config["exporters"]["exporter_list"]["cluster_data_exporter"][
            "scrape_interval"
        ] = "60s"

        with self.assertRaisesRegex(ValueError, "no longer accepts"):
            self._generate_config(experiment_config)

    def test_victoriametrics_uses_global_interval_and_timeout(self):
        with open(
            EXPERIMENTS_DIR / "config/experiment_type/cluster_data_google.yaml"
        ) as config_file:
            experiment_config = yaml.safe_load(config_file)

        vmagent_config = self._generate_vmagent_config(experiment_config)
        self.assertEqual(vmagent_config["global"]["scrape_interval"], "1s")
        cde_job = next(
            job
            for job in vmagent_config["scrape_configs"]
            if job["job_name"] == "cluster_data_exporter"
        )
        self.assertEqual(cde_job["scrape_timeout"], "1s")

    def test_cluster_data_repetitions_use_one_second_delay(self):
        for config_path in EXPERIMENTS_DIR.glob(
            "config/experiment_type/cluster_data_*.yaml"
        ):
            with self.subTest(config=config_path.name):
                with open(config_path) as config_file:
                    experiment_config = yaml.safe_load(config_file)
                self.assertTrue(experiment_config["query_groups"])
                self.assertTrue(
                    all(
                        group["repetition_delay_ms"] == 1000
                        for group in experiment_config["query_groups"]
                    )
                )


if __name__ == "__main__":
    unittest.main()
