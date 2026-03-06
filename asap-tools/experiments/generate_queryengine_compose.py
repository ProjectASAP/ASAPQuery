#!/usr/bin/env python3
"""
Helper script to generate docker-compose.yml for QueryEngine/QueryEngineRust from Jinja2 template.
This script runs on the remote CloudLab node to generate the compose file.
"""

import argparse
import os
import sys
from jinja2 import Template


def generate_compose_file(
    template_path: str,
    output_path: str,
    queryengine_dir: str,
    container_name: str,
    experiment_output_dir: str,
    controller_remote_output_dir: str,
    kafka_topic: str,
    input_format: str,
    prometheus_scrape_interval: str,
    log_level: str,
    streaming_engine: str,
    query_language: str,
    kafka_host: str,
    prometheus_host: str,
    prometheus_port: int,
    lock_strategy: str,
    http_port: str,
    compress_json: bool = False,
    profile_query_engine: bool = False,
    forward_unsupported_queries: bool = False,
    manual: bool = False,
    kafka_proxy_container_name: str = "sketchdb-kafka-proxy",
    dump_precomputes: bool = False,
):
    """Generate docker-compose.yml from template with provided variables."""

    # Read the Jinja template
    try:
        with open(template_path, "r") as f:
            template_content = f.read()
    except FileNotFoundError:
        print(f"Error: Template file not found at {template_path}")
        sys.exit(1)
    except Exception as e:
        print(f"Error reading template file: {e}")
        sys.exit(1)

    # Prepare template variables
    template_vars = {
        "queryengine_dir": queryengine_dir,
        "container_name": container_name,
        "http_port": http_port,
        "experiment_output_dir": experiment_output_dir,
        "controller_remote_output_dir": controller_remote_output_dir,
        "kafka_topic": kafka_topic,
        "input_format": input_format,
        "prometheus_scrape_interval": prometheus_scrape_interval,
        "log_level": log_level,
        "streaming_engine": streaming_engine,
        "query_language": query_language,
        "lock_strategy": lock_strategy,
        "compress_json": compress_json,
        "profile_query_engine": profile_query_engine,
        "forward_unsupported_queries": forward_unsupported_queries,
        "manual": manual,
        "kafka_host": kafka_host,
        "prometheus_host": prometheus_host,
        "prometheus_port": prometheus_port,
        "kafka_proxy_container_name": kafka_proxy_container_name,
        "dump_precomputes": dump_precomputes,
    }

    # Render the template
    try:
        template = Template(template_content)
        rendered_compose = template.render(**template_vars)
    except Exception as e:
        print(f"Error rendering template: {e}")
        sys.exit(1)

    # Ensure output directory exists
    output_dir = os.path.dirname(output_path)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    # Write rendered compose file
    try:
        with open(output_path, "w") as f:
            f.write(rendered_compose)
        print(f"Docker compose file generated successfully at {output_path}")
    except Exception as e:
        print(f"Error writing compose file: {e}")
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        description="Generate QueryEngine docker-compose.yml from template"
    )

    # Required arguments
    parser.add_argument(
        "--template-path", required=True, help="Path to docker-compose.yml.j2 template"
    )
    parser.add_argument(
        "--output-path", required=True, help="Output path for docker-compose.yml"
    )
    parser.add_argument(
        "--queryengine-dir",
        required=True,
        help="QueryEngine directory path for build context",
    )
    parser.add_argument("--container-name", required=True, help="Container name")
    parser.add_argument(
        "--experiment-output-dir", required=True, help="Experiment output directory"
    )
    parser.add_argument(
        "--controller-remote-output-dir",
        required=True,
        help="Controller output directory",
    )
    parser.add_argument("--kafka-topic", required=True, help="Kafka topic name")
    parser.add_argument(
        "--input-format", required=True, choices=["json", "byte"], help="Input format"
    )
    parser.add_argument(
        "--prometheus-scrape-interval", required=True, help="Prometheus scrape interval"
    )
    parser.add_argument("--log-level", required=True, help="Log level")
    parser.add_argument(
        "--streaming-engine",
        required=True,
        choices=["flink", "arroyo"],
        help="Streaming engine",
    )
    parser.add_argument(
        "--query-language",
        required=True,
        choices=["SQL", "PROMQL"],
        help="Query language (SQL or PROMQL)",
    )
    parser.add_argument(
        "--lock-strategy",
        required=True,
        choices=["global", "per-key"],
        help="Lock strategy for SimpleMapStore",
    )

    # Optional arguments
    parser.add_argument(
        "--compress-json", action="store_true", help="Enable JSON compression"
    )
    parser.add_argument(
        "--profile-query-engine", action="store_true", help="Enable profiling"
    )
    parser.add_argument(
        "--forward-unsupported-queries",
        action="store_true",
        help="Forward unsupported queries",
    )
    parser.add_argument("--manual", action="store_true", help="Manual mode")
    parser.add_argument("--kafka-host", required=True, help="Kafka host IP")
    parser.add_argument("--prometheus-host", required=True, help="Prometheus host IP")
    parser.add_argument(
        "--prometheus-port",
        type=int,
        required=True,
        help="Prometheus server port (9090 for Prometheus, 8428 for VictoriaMetrics)",
    )
    parser.add_argument(
        "--kafka-proxy-container-name",
        default="sketchdb-kafka-proxy",
        help="Kafka proxy container name",
    )
    parser.add_argument("--http-port", required=True, help="HTTP port")
    parser.add_argument(
        "--dump-precomputes", action="store_true", help="Dump precomputes"
    )

    args = parser.parse_args()

    generate_compose_file(
        template_path=args.template_path,
        output_path=args.output_path,
        queryengine_dir=args.queryengine_dir,
        container_name=args.container_name,
        experiment_output_dir=args.experiment_output_dir,
        controller_remote_output_dir=args.controller_remote_output_dir,
        kafka_topic=args.kafka_topic,
        input_format=args.input_format,
        prometheus_scrape_interval=args.prometheus_scrape_interval,
        log_level=args.log_level,
        streaming_engine=args.streaming_engine,
        query_language=args.query_language,
        lock_strategy=args.lock_strategy,
        http_port=args.http_port,
        compress_json=args.compress_json,
        profile_query_engine=args.profile_query_engine,
        forward_unsupported_queries=args.forward_unsupported_queries,
        manual=args.manual,
        kafka_host=args.kafka_host,
        prometheus_host=args.prometheus_host,
        prometheus_port=args.prometheus_port,
        kafka_proxy_container_name=args.kafka_proxy_container_name,
        dump_precomputes=args.dump_precomputes,
    )


if __name__ == "__main__":
    main()
