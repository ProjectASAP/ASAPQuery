"""
Script to generate docker-compose.yml files from frame templates based on data provider configuration.

This script takes a data provider (google or alibaba) and provider-specific arguments,
then generates a docker-compose.yml file by copying and modifying the appropriate frame file.
"""

import argparse
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

# Valid values from Rust enums (CLI format with hyphens)
VALID_GOOGLE_METRICS = [
    "mean-cpu-usage-rate",
    "canonical-memory-usage",
    "assigned-memory-usage",
    "unmapped-page-cache-memory-usage",
    "total-page-cache-memory-usage",
    "max-memory-usage",
    "mean-disk-io-time",
    "mean-local-disk-space-used",
    "max-cpu-usage",
    "max-disk-io-time",
    "cycles-per-instruction",
    "memory-accesses-per-instruction",
    "sample-portion",
    "sampled-cpu-usage",
]

VALID_ALIBABA_DATA_TYPES = ["node", "msresource"]
VALID_ALIBABA_DATA_YEARS = [2021, 2022]


def validate_google_metrics(metrics: List[str]) -> None:
    """Validate that all provided Google metrics are valid."""
    invalid_metrics = [m for m in metrics if m not in VALID_GOOGLE_METRICS]
    if invalid_metrics:
        print(f"Error: Invalid Google metrics: {', '.join(invalid_metrics)}")
        print(f"Valid metrics: {', '.join(VALID_GOOGLE_METRICS)}")
        sys.exit(1)


def validate_alibaba_args(data_type: str, data_year: int) -> None:
    """Validate Alibaba data type and year arguments."""
    if data_type not in VALID_ALIBABA_DATA_TYPES:
        print(f"Error: Invalid data type: {data_type}")
        print(f"Valid data types: {', '.join(VALID_ALIBABA_DATA_TYPES)}")
        sys.exit(1)

    if data_year not in VALID_ALIBABA_DATA_YEARS:
        print(f"Error: Invalid data year: {data_year}")
        print(f"Valid years: {', '.join(map(str, VALID_ALIBABA_DATA_YEARS))}")
        sys.exit(1)


def get_frame_file_path(
    provider: str, data_type: Optional[str] = None, data_year: Optional[int] = None
) -> Path:
    """Get the path to the appropriate frame file based on provider and arguments."""
    frames_dir = Path("docker_compose_frames")

    if provider == "google":
        return frames_dir / "google-docker-compose.yml"
    elif provider == "alibaba":
        return frames_dir / f"alibaba-{data_type}-{data_year}-docker-compose.yml"
    else:
        raise ValueError(f"Unknown provider: {provider}")


def load_yaml_file(file_path: Path) -> Dict[str, Any]:
    """Load YAML file and return parsed content."""
    with open(file_path, "r") as f:
        return yaml.safe_load(f)


def save_yaml_file(file_path: Path, data: Dict[str, Any]) -> None:
    """Save data to YAML file."""
    with open(file_path, "w") as f:
        yaml.dump(data, f, default_flow_style=False, sort_keys=False)


def update_command_arg(command: List[str], arg_name: str, new_value: str) -> List[str]:
    """Update a command line argument in the command list."""
    updated_command = []
    i = 0
    while i < len(command):
        if command[i] == arg_name:
            updated_command.append(command[i])
            if i + 1 < len(command):
                updated_command.append(new_value)
                i += 2
            else:
                updated_command.append(new_value)
                i += 1
        elif command[i].startswith(f"{arg_name}="):
            updated_command.append(f"{arg_name}={new_value}")
            i += 1
        else:
            updated_command.append(command[i])
            i += 1
    return updated_command


def generate_google_compose(
    metrics: List[str], port: Optional[int], input_dir: Optional[str]
) -> None:
    """Generate docker-compose.yml for Google provider."""
    frame_file = get_frame_file_path("google")
    output_file = Path("docker-compose.yml")

    # Load frame file
    compose_data = load_yaml_file(frame_file)

    # Update metrics
    metrics_str = ",".join(metrics)
    service = compose_data["services"]["cluster-data-exporter"]
    command = service["command"]

    # Find and update metrics argument
    for i, arg in enumerate(command):
        if arg.startswith("--metrics="):
            command[i] = f"--metrics={metrics_str}"
            break

    # Update optional arguments if provided
    if port is not None:
        # Update port mapping
        service["ports"] = [f"{port}:{port}"]
        # Update port in command
        command = update_command_arg(command, "--port", str(port))
        service["command"] = command

    if input_dir is not None:
        # Update volume mapping
        service["volumes"] = [f"{input_dir}:/data:ro"]

    # Save updated compose file
    save_yaml_file(output_file, compose_data)


def generate_alibaba_compose(
    data_type: str,
    data_year: int,
    port: Optional[int],
    input_dir: Optional[str],
    speedup: Optional[int],
) -> None:
    """Generate docker-compose.yml for Alibaba provider."""
    frame_file = get_frame_file_path("alibaba", data_type, data_year)
    output_file = Path("docker-compose.yml")

    # Load frame file
    compose_data = load_yaml_file(frame_file)

    service = compose_data["services"]["cluster-data-exporter"]
    command = service["command"]

    # Update optional arguments if provided
    if port is not None:
        # Update port mapping
        service["ports"] = [f"{port}:{port}"]
        # Update port in command
        command = update_command_arg(command, "--port", str(port))
        service["command"] = command

    if input_dir is not None:
        # Update volume mapping
        service["volumes"] = [f"{input_dir}:/data:ro"]

    # Add speedup if specified
    if speedup is not None:
        if "--speedup" not in " ".join(command):
            command.append(f"--speedup={speedup}")
        else:
            command = update_command_arg(command, "--speedup", str(speedup))
        service["command"] = command

    # Save updated compose file
    save_yaml_file(output_file, compose_data)


def main():
    parser = argparse.ArgumentParser(
        description="Generate docker-compose.yml from frame files based on data provider configuration",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Google provider with specific metrics
  python scripts/generate_docker_compose.py google --metrics mean_cpu_usage_rate,max_cpu_usage --port 8080

  # Alibaba provider with node data from 2021
  python scripts/generate_docker_compose.py alibaba --data-type node --data-year 2021 --port 8080

  # With custom input directory
  python scripts/generate_docker_compose.py google --metrics canonical_memory_usage --input-dir /path/to/data
        """,
    )

    parser.add_argument("provider", choices=["google", "alibaba"], help="Data provider")
    parser.add_argument("--port", type=int, help="Port number for the HTTP server")
    parser.add_argument("--input-dir", "--input-directory", help="Input directory path")

    # Google-specific arguments
    google_group = parser.add_argument_group("Google provider arguments")
    google_group.add_argument(
        "--metrics", type=str, help="Comma-separated list of metrics to export"
    )

    # Alibaba-specific arguments
    alibaba_group = parser.add_argument_group("Alibaba provider arguments")
    alibaba_group.add_argument(
        "--data-type", choices=VALID_ALIBABA_DATA_TYPES, help="Type of data to export"
    )
    alibaba_group.add_argument(
        "--data-year",
        type=int,
        choices=VALID_ALIBABA_DATA_YEARS,
        help="Year of the dataset",
    )
    alibaba_group.add_argument(
        "--speedup",
        type=int,
        help="Speedup factor for faster-than-realtime export (1=real-time, 10=10x faster)",
    )

    args = parser.parse_args()

    # Validate provider-specific required arguments
    if args.provider == "google":
        if not args.metrics:
            parser.error("Google provider requires --metrics argument")
        metrics_list = [m.strip() for m in args.metrics.split(",")]
        validate_google_metrics(metrics_list)
        generate_google_compose(metrics_list, args.port, args.input_dir)

    elif args.provider == "alibaba":
        if not args.data_type:
            parser.error("Alibaba provider requires --data-type argument")
        if not args.data_year:
            parser.error("Alibaba provider requires --data-year argument")
        validate_alibaba_args(args.data_type, args.data_year)
        generate_alibaba_compose(
            args.data_type, args.data_year, args.port, args.input_dir, args.speedup
        )

    print(f"Generated docker-compose.yml for {args.provider} provider")


if __name__ == "__main__":
    main()
