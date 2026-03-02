"""
Kafka service management for experiments.
"""

import os
import time
import subprocess
from typing import List

import constants
from .base import BaseService
from experiment_utils.providers.base import InfrastructureProvider


class KafkaService(BaseService):
    """Service for managing Kafka server lifecycle and topics."""

    def __init__(
        self, provider: InfrastructureProvider, node_offset: int, num_tries: int = 5
    ):
        """
        Initialize Kafka service.

        Args:
            provider: Infrastructure provider for node communication and management
            num_tries: Number of retry attempts when starting Kafka
            node_offset: Starting node index offset
        """
        super().__init__(provider)
        self.num_tries = num_tries
        self.node_offset = node_offset
        self.topics_created = False

    def start(self, **kwargs) -> None:
        """
        Start Kafka server with retry logic.

        Args:
            **kwargs: Additional configuration (currently unused)

        Raises:
            RuntimeError: If Kafka fails to start after all retry attempts
        """
        kafka_config = "./config/kraft/server.properties"
        cmd_dir = os.path.join(self.provider.get_home_dir(), "kafka")
        start_cmd = f"./bin/kafka-server-start.sh {kafka_config} > /dev/null 2>&1 &"
        check_cmd = 'pgrep -f "kafka.server"'
        reset_cmd = f"./bin/kafka-storage.sh format -t \`./bin/kafka-storage.sh random-uuid\` --config {kafka_config}"  # noqa: W605

        tries_remaining = self.num_tries
        while tries_remaining > 0:
            self.provider.execute_command(
                node_idx=self.node_offset,
                cmd=start_cmd,
                cmd_dir=cmd_dir,
                nohup=True,
                popen=False,
            )
            time.sleep(30)

            check_result = self.provider.execute_command(
                node_idx=self.node_offset,
                cmd=check_cmd,
                cmd_dir=None,
                nohup=False,
                popen=False,
                ignore_errors=True,
            )
            assert isinstance(check_result, subprocess.CompletedProcess)
            if check_result.stdout != "":
                return

            # Try to reset kafka storage
            self.provider.execute_command(
                node_idx=self.node_offset,
                cmd=reset_cmd,
                cmd_dir=cmd_dir,
                nohup=False,
                popen=False,
            )
            tries_remaining -= 1
            time.sleep(10)

        raise RuntimeError(f"Kafka failed to start after {self.num_tries} attempts")

    def stop(self, **kwargs) -> None:
        """
        Stop Kafka server.

        Args:
            **kwargs: Additional configuration (currently unused)
        """
        cmd_dir = os.path.join(self.provider.get_home_dir(), "kafka")
        cmd = "./bin/kafka-server-stop.sh > /dev/null 2>&1"
        self.provider.execute_command(
            node_idx=self.node_offset,
            cmd=cmd,
            cmd_dir=cmd_dir,
            nohup=False,
            popen=False,
            ignore_errors=True,
        )
        self.topics_created = False

    def is_healthy(self) -> bool:
        """
        Check if Kafka is healthy by attempting to list topics.

        Returns:
            True if Kafka is responsive, False otherwise
        """
        try:
            cmd = f"./bin/kafka-topics.sh --bootstrap-server {constants.KAFKA_BROKER} --list"
            cmd_dir = os.path.join(self.provider.get_home_dir(), "kafka")
            self.provider.execute_command(
                node_idx=self.node_offset,
                cmd=cmd,
                cmd_dir=cmd_dir,
                nohup=False,
                popen=False,
            )
            return True
        except Exception:
            return False

    def wait_until_ready(self) -> None:
        """Wait until Kafka is ready to accept connections."""
        # kafka-topics blocks until it gets a response from the server
        cmd = (
            f"./bin/kafka-topics.sh --bootstrap-server {constants.KAFKA_BROKER} --list"
        )
        cmd_dir = os.path.join(self.provider.get_home_dir(), "kafka")
        self.provider.execute_command(
            node_idx=self.node_offset,
            cmd=cmd,
            cmd_dir=cmd_dir,
            nohup=False,
            popen=False,
        )

    def create_topics(self, topics: List[str] = None) -> None:
        """
        Create Kafka topics for the experiment.

        Args:
            topics: List of topic names to create. Defaults to standard experiment topics.
        """
        if topics is None:
            topics = [constants.FLINK_INPUT_TOPIC, constants.FLINK_OUTPUT_TOPIC]

        cmds = []
        for topic in topics:
            cmd = f"./bin/kafka-topics.sh --bootstrap-server {constants.KAFKA_BROKER} --create --topic {topic} --partitions 1 --replication-factor 1 --config max.message.bytes=20971520 &"
            cmds.append(cmd)
        cmds.append("wait")

        final_cmd = " ".join(cmds)
        cmd_dir = os.path.join(self.provider.get_home_dir(), "kafka")
        self.provider.execute_command(
            node_idx=self.node_offset,
            cmd=final_cmd,
            cmd_dir=cmd_dir,
            nohup=False,
            popen=False,
        )
        self.topics_created = True

    def delete_topics(self, topics: List[str] = None) -> None:
        """
        Delete Kafka topics.

        Args:
            topics: List of topic names to delete. Defaults to standard experiment topics.
        """
        if topics is None:
            topics = [constants.FLINK_INPUT_TOPIC, constants.FLINK_OUTPUT_TOPIC]

        cmds = []
        for topic in topics:
            cmd = f"./bin/kafka-topics.sh --bootstrap-server {constants.KAFKA_BROKER} --delete --topic {topic} &"
            cmds.append(cmd)
        cmds.append("wait")

        final_cmd = " ".join(cmds)
        cmd_dir = os.path.join(self.provider.get_home_dir(), "kafka")
        self.provider.execute_command(
            node_idx=self.node_offset,
            cmd=final_cmd,
            cmd_dir=cmd_dir,
            nohup=False,
            popen=False,
        )
        self.topics_created = False
