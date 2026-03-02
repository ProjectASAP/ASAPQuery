"""
Helper script to generate docker-compose.yml for ArroyoSketch from Jinja2 template.
"""

import argparse
import os
import sys
from jinja2 import Template


def generate_compose_file(
    template_path: str,
    output_path: str,
    arroyosketch_dir: str,
    container_name: str,
    controller_output_dir: str,
    arroyosketch_output_dir: str,
    prometheus_base_port: int,
    prometheus_path: str,
    prometheus_bind_ip: str,
    parallelism: int,
    output_kafka_topic: str,
    output_format: str,
    pipeline_name: str,
    arroyo_url: str,
    bootstrap_servers: str,
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
        "arroyosketch_dir": arroyosketch_dir,
        "container_name": container_name,
        "controller_output_dir": controller_output_dir,
        "arroyosketch_output_dir": arroyosketch_output_dir,
        "prometheus_base_port": prometheus_base_port,
        "prometheus_path": prometheus_path,
        "prometheus_bind_ip": prometheus_bind_ip,
        "parallelism": parallelism,
        "output_kafka_topic": output_kafka_topic,
        "output_format": output_format,
        "pipeline_name": pipeline_name,
        "arroyo_url": arroyo_url,
        "bootstrap_servers": bootstrap_servers,
    }

    # Render the template
    try:
        template = Template(template_content)
        rendered_compose = template.render(**template_vars)
    except Exception as e:
        print(f"Error rendering template: {e}")
        sys.exit(1)

    # Ensure output directory exists
    output_dir_path = os.path.dirname(output_path)
    if output_dir_path:
        os.makedirs(output_dir_path, exist_ok=True)

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
        description="Generate ArroyoSketch docker-compose.yml from template"
    )

    # Required arguments
    parser.add_argument(
        "--template-path", required=True, help="Path to docker-compose.yml.j2 template"
    )
    parser.add_argument(
        "--compose-output-path",
        required=True,
        help="Output path for docker-compose.yml",
    )
    parser.add_argument(
        "--arroyosketch-dir",
        required=True,
        help="ArroyoSketch directory path for build context",
    )
    parser.add_argument(
        "--container-name",
        default="sketchdb-arroyosketch",
        help="Container name (default: sketchdb-arroyosketch)",
    )
    parser.add_argument(
        "--controller-output-dir",
        required=True,
        help="Controller output directory (shared volume for config)",
    )
    parser.add_argument(
        "--arroyosketch-output-dir",
        required=True,
        help="ArroyoSketch output directory for pipeline_id.txt",
    )
    parser.add_argument(
        "--prometheus-base-port",
        type=int,
        default=9091,
        help="Prometheus remote write base port (default: 9091)",
    )
    parser.add_argument(
        "--prometheus-path",
        default="/receive",
        help="Prometheus remote write path (default: /receive)",
    )
    parser.add_argument(
        "--prometheus-bind-ip",
        default="0.0.0.0",
        help="Prometheus remote write bind IP (default: 0.0.0.0)",
    )
    parser.add_argument(
        "--parallelism",
        type=int,
        default=1,
        help="Pipeline parallelism (default: 1)",
    )
    parser.add_argument(
        "--output-kafka-topic",
        default="flink_output",
        help="Output Kafka topic (default: flink_output)",
    )
    parser.add_argument(
        "--output-format",
        default="json",
        choices=["json", "byte"],
        help="Output format (default: json)",
    )
    parser.add_argument(
        "--pipeline-name",
        required=True,
        help="Pipeline name (usually experiment name)",
    )
    parser.add_argument(
        "--arroyo-url",
        default="http://arroyo:5115/api/v1",
        help="Arroyo API URL (default: http://arroyo:5115/api/v1)",
    )
    parser.add_argument(
        "--bootstrap-servers",
        default="kafka:9092",
        help="Kafka bootstrap servers (default: kafka:9092)",
    )

    args = parser.parse_args()

    generate_compose_file(
        template_path=args.template_path,
        output_path=args.compose_output_path,
        arroyosketch_dir=args.arroyosketch_dir,
        container_name=args.container_name,
        controller_output_dir=args.controller_output_dir,
        arroyosketch_output_dir=args.arroyosketch_output_dir,
        prometheus_base_port=args.prometheus_base_port,
        prometheus_path=args.prometheus_path,
        prometheus_bind_ip=args.prometheus_bind_ip,
        parallelism=args.parallelism,
        output_kafka_topic=args.output_kafka_topic,
        output_format=args.output_format,
        pipeline_name=args.pipeline_name,
        arroyo_url=args.arroyo_url,
        bootstrap_servers=args.bootstrap_servers,
    )


if __name__ == "__main__":
    main()
