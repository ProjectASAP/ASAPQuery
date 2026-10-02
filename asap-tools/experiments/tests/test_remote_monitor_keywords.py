"""Tests that remote_monitor.py receives its keyword list as one clean argument."""

import shlex
import unittest

from experiment_utils.services.remote_monitor_service import RemoteMonitorService


class RecordingProvider:
    def __init__(self, remote):
        self.remote = remote
        self.commands = []

    def is_remote(self):
        return self.remote

    def get_home_dir(self):
        return "/home"

    def execute_command(self, **kwargs):
        self.commands.append(kwargs["cmd"])


def keywords_seen_by_monitor(remote):
    provider = RecordingProvider(remote)
    RemoteMonitorService(provider, 0).start(
        controller_client_config="/out/controller_client_configs/baseline.yaml",
        experiment_output_dir="/out/baseline",
        experiment_mode="baseline",
        profile_query_engine=False,
        profile_prometheus_time=None,
        manual_mode=False,
        streaming_engine="precompute",
        query_engine_service=None,
        controller_remote_output_dir="/out/controller_output",
        use_container_prometheus_client=True,
        prometheus_client_parallel=False,
        backend_protocol="prometheus",
        pre_query_wait_seconds=0,
        monitor_interval_seconds=1.0,
        timed_duration=10,
    )
    args = shlex.split(provider.commands[0])
    if remote:
        # The SSH provider wraps the command in one more shell.
        args = shlex.split(" ".join(args))
    return args[args.index("--keywords") + 1]


class KeywordQuotingTest(unittest.TestCase):
    def test_local_provider_gets_unescaped_keywords(self):
        # Escaped quotes meant for SSH used to reach remote_monitor.py
        # verbatim in local mode, so no process matched and no queries ran.
        self.assertEqual(keywords_seen_by_monitor(remote=False), "prometheus.yml")

    def test_remote_provider_keeps_escaped_quotes_for_ssh(self):
        self.assertEqual(keywords_seen_by_monitor(remote=True), "prometheus.yml")


if __name__ == "__main__":
    unittest.main()
