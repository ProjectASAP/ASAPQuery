"""Tests for generate_prometheus_client_compose.py's rqe side-file mounts."""

import os
import tempfile
import unittest

import yaml

import generate_prometheus_client_compose as gen

TEMPLATE = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(gen.__file__))),
    "queriers",
    "prometheus-client",
    "docker-compose.yml.j2",
)


class RqeExprsMountsTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.config_file = os.path.join(self.tmp.name, "sketchdb.yaml")

    def tearDown(self):
        self.tmp.cleanup()

    def write_config(self, servers):
        with open(self.config_file, "w") as f:
            yaml.safe_dump({"servers": servers, "query_groups": []}, f)

    def render(self):
        output = os.path.join(self.tmp.name, "compose.yml")
        gen.generate_compose_file(
            TEMPLATE,
            output,
            container_name="c",
            experiment_output_dir="/out",
            config_file=self.config_file,
            client_output_dir="/out/client",
            client_output_file="client.log",
            prometheus_host="10.0.0.1",
            sketchdb_host="10.0.0.1",
            rqe_exprs_mounts=gen.rqe_exprs_mounts(self.config_file),
        )
        with open(output) as f:
            return yaml.safe_load(f)["services"]["prometheusclient"]["volumes"]

    def test_side_file_keeps_its_place_next_to_the_config(self):
        self.write_config(
            [
                {"name": "prometheus", "url": "http://p"},
                {
                    "name": "sketchdb",
                    "url": "http://s",
                    "protocol": "rqe",
                    "rqe_exprs_file": "rqe_exprs.json",
                },
            ]
        )
        open(os.path.join(self.tmp.name, "rqe_exprs.json"), "w").close()
        self.assertIn(
            f"{self.tmp.name}/rqe_exprs.json:/app/rqe_exprs.json:ro", self.render()
        )

    def test_missing_side_file_of_an_rqe_server_is_an_error(self):
        self.write_config(
            [
                {
                    "name": "sketchdb",
                    "url": "http://s",
                    "protocol": "rqe",
                    "rqe_exprs_file": "rqe_exprs.json",
                }
            ]
        )
        with self.assertRaisesRegex(FileNotFoundError, "rqe server 'sketchdb'"):
            gen.rqe_exprs_mounts(self.config_file)

    def test_no_mount_without_servers(self):
        with open(self.config_file, "w") as f:
            yaml.safe_dump({"query_groups": []}, f)
        self.assertEqual(gen.rqe_exprs_mounts(self.config_file), [])

    def test_no_mount_without_a_config_file(self):
        self.assertEqual(gen.rqe_exprs_mounts(self.config_file), [])

    def test_no_mount_without_rqe_servers(self):
        self.write_config([{"name": "sketchdb", "url": "http://s"}])
        self.assertEqual(gen.rqe_exprs_mounts(self.config_file), [])
        self.assertEqual(len(self.render()), 3)


if __name__ == "__main__":
    unittest.main()
