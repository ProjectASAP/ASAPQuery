#!/usr/bin/env python3
"""
Helper script to generate docker-compose.yml from Jinja2 template.
Processes the docker-compose.yml.j2 template with command line arguments.
"""

import argparse
import os
import sys
from jinja2 import Template


def generate_compose_file(template_path: str, output_path: str, **template_vars):
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
        description="Generate docker-compose.yml from Jinja2 template"
    )

    # Required arguments
    parser.add_argument(
        "--template-path",
        default="docker-compose.yml.j2",
        help="Path to docker-compose.yml.j2 template (default: docker-compose.yml.j2)",
    )
    parser.add_argument(
        "--compose-output-path",
        help="Output path for docker-compose.yml (default: docker-compose.yml)",
    )

    # Template variables based on docker-compose.yml.j2
    parser.add_argument(
        "--prometheusclient-dir",
        required=True,
        help="PrometheusClient directory path for build context",
    )
    parser.add_argument(
        "--container-name",
        default="sketchdb-prometheusclient",
        help="Container name (default: sketchdb-prometheusclient)",
    )
    parser.add_argument(
        "--experiment-output-dir",
        required=True,
        help="Experiment output directory to mount",
    )
    parser.add_argument(
        "--config-file",
        required=True,
        help="Config file path (default: /app/prometheus_client_config.yaml)",
    )
    parser.add_argument(
        "--client-output-dir",
        required=True,
        help="Output directory path (default: /app/outputs/prometheus_client/)",
    )
    parser.add_argument(
        "--client-output-file",
        required=True,
        help="Output file path (default: /app/outputs/prometheus_client/prometheus_client_output.log)",
    )
    parser.add_argument(
        "--server-for-alignment",
        default="sketchdb",
        help="Server for alignment (default: sketchdb)",
    )
    parser.add_argument(
        "--prometheus-host",
        required=True,
        help="Prometheus host IP",
    )
    parser.add_argument(
        "--sketchdb-host",
        required=True,
        help="SketchDB host IP",
    )

    # Optional boolean flags
    parser.add_argument(
        "--align-query-time", action="store_true", help="Enable query time alignment"
    )
    parser.add_argument("--dry-run", action="store_true", help="Enable dry run mode")
    parser.add_argument(
        "--compare-results", action="store_true", help="Enable result comparison"
    )
    parser.add_argument(
        "--parallel", action="store_true", help="Enable parallel execution"
    )

    # Optional arguments that can be None
    parser.add_argument("--result-output-file", help="Result output file path")
    parser.add_argument(
        "--query-engine-config-file",
        help="Query engine config file path",
        type=str,
    )
    parser.add_argument(
        "--profile-query-engine-pid", type=int, help="Query engine PID for profiling"
    )
    parser.add_argument("--profile-prometheus-time", help="Prometheus profiling time")
    parser.add_argument(
        "--latency-exporter-socket-addr", help="Latency exporter socket address"
    )

    args = parser.parse_args()

    # Prepare template variables
    template_vars = {
        "prometheusclient_dir": args.prometheusclient_dir,
        "container_name": args.container_name,
        "experiment_output_dir": args.experiment_output_dir,
        "config_file": args.config_file,
        "client_output_dir": args.client_output_dir,
        "client_output_file": args.client_output_file,
        "server_for_alignment": args.server_for_alignment,
        "prometheus_host": args.prometheus_host,
        "sketchdb_host": args.sketchdb_host,
        "align_query_time": args.align_query_time,
        "dry_run": args.dry_run,
        "compare_results": args.compare_results,
        "parallel": args.parallel,
    }

    # Only add optional args if they have values
    if args.result_output_file is not None:
        template_vars["result_output_file"] = args.result_output_file
    if args.query_engine_config_file is not None:
        template_vars["query_engine_config_file"] = args.query_engine_config_file
    if args.profile_query_engine_pid is not None:
        template_vars["query_engine_pid"] = args.profile_query_engine_pid
    if args.profile_prometheus_time is not None:
        template_vars["profile_prometheus_time"] = args.profile_prometheus_time
    if args.latency_exporter_socket_addr is not None:
        template_vars["latency_exporter_socket_addr"] = (
            args.latency_exporter_socket_addr
        )

    generate_compose_file(
        template_path=args.template_path,
        output_path=args.compose_output_path,
        **template_vars,
    )


if __name__ == "__main__":
    main()
