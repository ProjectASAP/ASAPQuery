#!/usr/bin/env python3
"""Script to set the data cardinality for fake exporters in the quickstart demo."""

import argparse
import re
import sys


def update_docker_compose(
    file_path: str, num_labels: int, num_values_per_label: int
) -> None:
    """Update docker-compose.yml with the specified cardinality settings."""
    with open(file_path, "r") as f:
        content = f.read()

    # Replace all --num-labels=N occurrences
    content, num_labels_count = re.subn(
        r'("--num-labels=)\d+(")',
        rf"\g<1>{num_labels}\g<2>",
        content,
    )

    # Replace all --num-values-per-label=N[,N,...] occurrences
    # Build comma-separated value string matching the number of labels
    values_str = ",".join([str(num_values_per_label)] * num_labels)
    content, num_values_count = re.subn(
        r'("--num-values-per-label=)[\d,]+(")',
        rf"\g<1>{values_str}\g<2>",
        content,
    )

    if num_labels_count == 0 or num_values_count == 0:
        print("ERROR: Could not find exporter configuration in docker-compose.yml")
        sys.exit(1)

    with open(file_path, "w") as f:
        f.write(content)

    total_cardinality = num_values_per_label**num_labels
    print("Updated docker-compose.yml:")
    print(f"  --num-labels={num_labels}")
    print(f"  --num-values-per-label={num_values_per_label}")
    print(f"  Cardinality per exporter: {total_cardinality:,}")


def main():
    parser = argparse.ArgumentParser(
        description="Set the data cardinality for fake exporters in the quickstart demo"
    )
    parser.add_argument(
        "num_labels",
        type=int,
        help="Number of label dimensions (e.g., 3)",
    )
    parser.add_argument(
        "num_values_per_label",
        type=int,
        help="Number of unique values per label (e.g., 20)",
    )
    parser.add_argument(
        "--docker-compose",
        default="docker-compose.yml",
        help="Path to docker-compose.yml (default: docker-compose.yml)",
    )

    args = parser.parse_args()

    if args.num_labels < 1 or args.num_labels > 10:
        print("ERROR: Number of labels must be between 1 and 10")
        sys.exit(1)

    if args.num_values_per_label < 1 or args.num_values_per_label > 1000:
        print("ERROR: Number of values per label must be between 1 and 1000")
        sys.exit(1)

    total_cardinality = args.num_values_per_label**args.num_labels
    print("\nConfiguring fake exporters with cardinality settings...")
    print(f"  Labels: {args.num_labels}")
    print(f"  Values per label: {args.num_values_per_label}")
    print(f"  Total cardinality per exporter: {total_cardinality:,}\n")

    update_docker_compose(
        args.docker_compose, args.num_labels, args.num_values_per_label
    )

    print("\nSuccessfully updated exporter cardinality settings!")


if __name__ == "__main__":
    main()
