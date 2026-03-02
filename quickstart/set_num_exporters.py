#!/usr/bin/env python3
"""Script to set the number of fake exporters in the quickstart demo."""

import argparse
import re
import sys


def update_docker_compose(file_path: str, num_exporters: int) -> None:
    """Update docker-compose.yml with the specified number of exporters."""
    with open(file_path, "r") as f:
        lines = f.readlines()

    # Find the start and end of the DATA GENERATORS section
    start_idx = None
    end_idx = None

    for i, line in enumerate(lines):
        if "# DATA GENERATORS" in line:
            # Look backwards to find the first hash separator line
            start_idx = i
            while start_idx > 0 and lines[start_idx - 1].strip().startswith("#"):
                start_idx -= 1
        elif (
            start_idx is not None
            and line.strip()
            and not line.startswith(" ")
            and not line.startswith("#")
        ):
            # Found the start of next section (non-indented, non-comment line)
            end_idx = i
            break

    if start_idx is None:
        print("ERROR: Could not find DATA GENERATORS section in docker-compose.yml")
        sys.exit(1)

    # If we didn't find an end, assume it goes to the end of file
    if end_idx is None:
        end_idx = len(lines)

    # Update the comment with the new number
    header_line = f"  # DATA GENERATORS ({num_exporters} fake exporters for demo)\n"

    # Generate new exporter services
    exporter_template = """  fake-exporter-{idx}:
    build:
      context: ../PrometheusExporters/fake_exporter/fake_exporter_rust/fake_exporter
    container_name: asap-fake-exporter-{idx}
    hostname: fake-exporter-{idx}
    networks:
      - asap-network
    expose:
      - "{port}"
    command:
      - "--port={port}"
      - "--valuescale=10000"
      - "--dataset=uniform"
      - "--num-labels=3"
      - "--num-values-per-label=20"
      - "--metric-type=gauge"
    restart: no
"""

    new_section = [
        "  #############################################################################\n",
        header_line,
        "  #############################################################################\n",
        "\n",
    ]

    for i in range(num_exporters):
        port = 50000 + i
        new_section.append(exporter_template.format(idx=i, port=port))
        if i < num_exporters - 1:
            new_section.append("\n")

    # Replace the old section with the new one
    new_lines = lines[:start_idx] + new_section + lines[end_idx:]

    with open(file_path, "w") as f:
        f.writelines(new_lines)

    print(f"✓ Updated docker-compose.yml: {num_exporters} exporters")


def update_prometheus_config(file_path: str, num_exporters: int) -> None:
    """Update prometheus.yml with the specified number of exporter targets."""
    with open(file_path, "r") as f:
        content = f.read()

    # Find and replace the targets section
    # Match from "- targets:" to the next blank line or end of indented block
    pattern = r"(      - targets:\n)((?:          - \'fake-exporter-\d+:\d+\'\n?)*)"

    # Generate new targets
    targets = []
    for i in range(num_exporters):
        port = 50000 + i
        targets.append(f"          - 'fake-exporter-{i}:{port}'")

    new_targets = "\n".join(targets) + "\n"

    # Replace the targets
    new_content = re.sub(
        pattern,
        (
            f'{targets[0] if targets else ""}' + "\n" + "\n".join(targets[1:]) + "\n"
            if len(targets) > 1
            else new_targets
        ),
        content,
        flags=re.MULTILINE,
    )

    # Better approach - find and replace directly
    match = re.search(pattern, content, re.MULTILINE)
    if match:
        new_content = content[: match.start(2)] + new_targets + content[match.end(2) :]
    else:
        print("ERROR: Could not find targets section in prometheus.yml")
        sys.exit(1)

    with open(file_path, "w") as f:
        f.write(new_content)

    print(f"✓ Updated prometheus.yml: {num_exporters} scrape targets")


def main():
    parser = argparse.ArgumentParser(
        description="Set the number of fake exporters in the quickstart demo"
    )
    parser.add_argument(
        "num_exporters", type=int, help="Number of fake exporters to configure (1-50)"
    )
    parser.add_argument(
        "--docker-compose",
        default="docker-compose.yml",
        help="Path to docker-compose.yml (default: docker-compose.yml)",
    )
    parser.add_argument(
        "--prometheus-config",
        default="config/prometheus.yml",
        help="Path to prometheus.yml (default: config/prometheus.yml)",
    )

    args = parser.parse_args()

    if args.num_exporters < 1 or args.num_exporters > 50:
        print("ERROR: Number of exporters must be between 1 and 50")
        sys.exit(1)

    print(f"\nConfiguring quickstart with {args.num_exporters} fake exporters...\n")

    update_docker_compose(args.docker_compose, args.num_exporters)
    update_prometheus_config(args.prometheus_config, args.num_exporters)

    print(f"\n✓ Successfully configured {args.num_exporters} exporters!")


if __name__ == "__main__":
    main()
