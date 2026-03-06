"""
Script to generate docker-compose.yml files from Jinja2 template for fake_exporter.

This script takes command line arguments corresponding to the variables in the
docker-compose.yml.j2 template and renders the final docker-compose.yml file.
"""

import argparse
import sys
from pathlib import Path
from jinja2 import Environment, FileSystemLoader, TemplateNotFound


def render_template(
    template_path: Path, output_path: Path, template_vars: dict
) -> None:
    """Render Jinja2 template with provided variables."""
    try:
        # Set up Jinja2 environment
        env = Environment(loader=FileSystemLoader(template_path.parent))
        template = env.get_template(template_path.name)

        # Render template with variables
        rendered_content = template.render(**template_vars)

        # Write to output file
        with open(output_path, "w") as f:
            f.write(rendered_content)

    except TemplateNotFound:
        print(f"Error: Template file not found: {template_path}")
        sys.exit(1)
    except Exception as e:
        print(f"Error rendering template: {e}")
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        description="Generate docker-compose.yml from Jinja2 template for fake_exporter",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Example:
  python generate_fake_exporter_compose.py \\
    --fake-exporter-dir . \\
    --port 8080 \\
    --output-dir /app/output \\
    --valuescale 1000 \\
    --dataset test_dataset \\
    --num-labels 10 \\
    --num-values-per-label 100 \\
    --metric-type gauge \\
    --experiment-output-dir ./output \\
    --container-name my-fake-exporter
        """,
    )

    # Required arguments based on template variables
    parser.add_argument(
        "--fake-exporter-dir",
        required=True,
        help="Directory containing the fake_exporter Dockerfile (context for Docker build)",
    )
    parser.add_argument(
        "--port", type=int, required=True, help="Port number for the HTTP server"
    )
    parser.add_argument(
        "--valuescale",
        type=int,
        required=True,
        help="Scale factor for generated metric values",
    )
    parser.add_argument("--dataset", required=True, help="Dataset name or identifier")
    parser.add_argument(
        "--num-labels",
        type=int,
        required=True,
        help="Number of labels to generate for metrics",
    )
    parser.add_argument(
        "--num-values-per-label",
        type=int,
        required=True,
        help="Number of values to generate per label",
    )
    parser.add_argument(
        "--metric-type",
        required=True,
        help="Type of metric to generate (e.g., gauge, counter)",
    )

    parser.add_argument(
        "--template-path",
        required=True,
        help="Template file name (default: docker-compose.yml.j2)",
    )
    # Optional arguments
    parser.add_argument(
        "--container-name",
        default="sketchdb-fake-exporter",
        help="Docker container name (defaults to 'sketchdb-fake-exporter')",
    )
    parser.add_argument(
        "--exporter-output-dir",
        help="Output directory path to mount as a volume inthe container. This argument is only required for the python fake exporter",
    )
    parser.add_argument(
        "--experiment-output-dir",
        help="Host directory to mount as output volume. This argument is only required for the python fake exporter",
    )
    parser.add_argument(
        "--compose-output-path",
        default="docker-compose.yml",
        help="Output file name (default: docker-compose.yml)",
    )

    args = parser.parse_args()

    # Prepare template variables
    template_vars = {
        "fake_exporter_dir": args.fake_exporter_dir,
        "port": args.port,
        "output_dir": args.exporter_output_dir,
        "valuescale": args.valuescale,
        "dataset": args.dataset,
        "num_labels": args.num_labels,
        "num_values_per_label": args.num_values_per_label,
        "metric_type": args.metric_type,
        "experiment_output_dir": args.experiment_output_dir,
    }

    # Only include container_name if provided, so Jinja2 default filter can work
    if args.container_name:
        template_vars["container_name"] = args.container_name

    # Set up file paths
    script_dir = Path(__file__).parent
    template_path = script_dir / args.template_path
    output_path = args.compose_output_path

    # Check if template file exists
    if not template_path.exists():
        print(f"Error: Template file not found: {template_path}")
        sys.exit(1)

    # Render template
    render_template(template_path, output_path, template_vars)

    print(f"Generated {output_path} from {template_path}")


if __name__ == "__main__":
    main()
