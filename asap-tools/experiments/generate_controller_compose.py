"""
Helper script to generate docker-compose.yml for Controller from Jinja2 template.
"""

import argparse
import os
import sys
from jinja2 import Template


def generate_compose_file(
    template_path: str,
    output_path: str,
    controller_dir: str,
    container_name: str,
    input_config_path: str,
    output_dir: str,
    prometheus_scrape_interval: int,
    streaming_engine: str,
    punting: bool,
    prometheus_url: str,
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
        "controller_dir": controller_dir,
        "container_name": container_name,
        "input_config_path": input_config_path,
        "output_dir": output_dir,
        "prometheus_scrape_interval": prometheus_scrape_interval,
        "streaming_engine": streaming_engine,
        "punting": punting,
        "prometheus_url": prometheus_url,
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
        description="Generate Controller docker-compose.yml from template"
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
        "--controller-dir",
        required=True,
        help="Controller directory path for build context",
    )
    parser.add_argument(
        "--container-name",
        default="sketchdb-controller",
        help="Container name (default: sketchdb-controller)",
    )
    parser.add_argument(
        "--input-config-path",
        required=True,
        help="Path to input configuration YAML file",
    )
    parser.add_argument(
        "--controller-output-dir",
        required=True,
        help="Output directory for generated configs",
    )
    parser.add_argument(
        "--prometheus-scrape-interval",
        type=int,
        required=True,
        help="Prometheus scrape interval in seconds",
    )
    parser.add_argument(
        "--streaming-engine",
        required=True,
        choices=["flink", "arroyo"],
        help="Streaming engine",
    )
    parser.add_argument(
        "--punting",
        action="store_true",
        help="Enable query punting based on performance heuristics",
    )
    parser.add_argument(
        "--prometheus-url",
        required=True,
        help="Base URL of the Prometheus instance for metric label inference (e.g. http://localhost:9090)",
    )

    args = parser.parse_args()

    generate_compose_file(
        template_path=args.template_path,
        output_path=args.compose_output_path,
        controller_dir=args.controller_dir,
        container_name=args.container_name,
        input_config_path=args.input_config_path,
        output_dir=args.controller_output_dir,
        prometheus_scrape_interval=args.prometheus_scrape_interval,
        streaming_engine=args.streaming_engine,
        punting=args.punting,
        prometheus_url=args.prometheus_url,
    )


if __name__ == "__main__":
    main()
