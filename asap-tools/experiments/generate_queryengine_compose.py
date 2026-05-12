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
    log_level: str,
    http_port: str,
    manual: bool = False,
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
        "log_level": log_level,
        "experiment_output_dir": experiment_output_dir,
        "controller_remote_output_dir": controller_remote_output_dir,
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
    parser.add_argument("--log-level", required=True, help="Log level")
    parser.add_argument("--http-port", required=True, help="HTTP port")
    parser.add_argument("--manual", action="store_true", help="Manual mode")

    args = parser.parse_args()

    generate_compose_file(
        template_path=args.template_path,
        output_path=args.output_path,
        queryengine_dir=args.queryengine_dir,
        container_name=args.container_name,
        experiment_output_dir=args.experiment_output_dir,
        controller_remote_output_dir=args.controller_remote_output_dir,
        log_level=args.log_level,
        http_port=args.http_port,
        manual=args.manual,
    )


if __name__ == "__main__":
    main()
