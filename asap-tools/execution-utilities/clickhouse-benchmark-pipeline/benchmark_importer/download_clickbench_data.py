#!/usr/bin/env python3
"""
ClickBench Data Downloader

Downloads the official ClickBench dataset (hits.json.gz).

Usage:
    python download_data.py --output-dir ./data

    # Specify output file directly:
    python download_data.py --output-file /path/to/hits.json.gz
"""

import argparse
import os
import sys
import urllib.request


def load_config():
    """Load configuration from config.env file. All values must be defined there."""
    config = {}
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)

    # Load config (environment variables take precedence)
    config_file = os.path.join(project_root, "config.env")
    if os.path.exists(config_file):
        with open(config_file) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, value = line.split("=", 1)
                    if key not in os.environ:
                        config[key] = value

    # Store project root for path resolution
    config["_PROJECT_ROOT"] = project_root
    return config


_config = load_config()


def get_config(key):
    """Get config value from environment or config.env. Raises error if not found."""
    if key in os.environ:
        return os.environ[key]
    if key in _config:
        return _config[key]
    raise ValueError(f"Configuration '{key}' not found. Please set it in config.env")


# Get values from config (no hardcoded fallbacks)
CLICKBENCH_URL = get_config("CLICKBENCH_URL")
CLICKBENCH_FILENAME = get_config("CLICKBENCH_FILENAME")
CLICKBENCH_DATA_DIR = get_config("CLICKBENCH_DATA_DIR")


def download_clickbench_data(output_path: str) -> str:
    """Download ClickBench dataset if not already present."""
    if os.path.exists(output_path):
        print(f"Using existing file: {output_path}")
        return output_path

    print(f"Downloading ClickBench dataset from {CLICKBENCH_URL}")
    print("This is a large file (~14GB compressed, ~100M rows). Please wait...")

    request = urllib.request.Request(
        CLICKBENCH_URL,
        headers={"User-Agent": "Mozilla/5.0 (compatible; ClickBench-Importer/1.0)"},
    )

    try:
        with urllib.request.urlopen(request) as response:
            total_size = int(response.headers.get("Content-Length", 0))
            downloaded = 0
            last_percent = -1
            block_size = 8192 * 128  # 1MB blocks

            with open(output_path, "wb") as out_file:
                while True:
                    block = response.read(block_size)
                    if not block:
                        break
                    out_file.write(block)
                    downloaded += len(block)

                    if total_size > 0:
                        percent = downloaded * 100 // total_size
                        if percent != last_percent:
                            last_percent = percent
                            downloaded_mb = downloaded / (1024 * 1024)
                            total_mb = total_size / (1024 * 1024)
                            sys.stdout.write(
                                f"\rProgress: {percent}% ({downloaded_mb:.1f}/{total_mb:.1f} MB)"
                            )
                            sys.stdout.flush()

        print("\nDownload complete!")
        return output_path

    except urllib.error.HTTPError as e:
        print(f"\nDownload failed: HTTP {e.code} - {e.reason}")
        print("You can manually download the file and use --output-file option:")
        print(f"  wget {CLICKBENCH_URL}")
        print(f"  curl -L -o {CLICKBENCH_FILENAME} {CLICKBENCH_URL}")
        raise


def main():
    # Compute default output dir from config
    default_output_dir = os.path.join(_config["_PROJECT_ROOT"], CLICKBENCH_DATA_DIR)

    parser = argparse.ArgumentParser(
        description="Download ClickBench dataset",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--output-file",
        help=f"Path to save {CLICKBENCH_FILENAME} (overrides --output-dir)",
    )
    parser.add_argument(
        "--output-dir",
        default=default_output_dir,
        help=f"Directory to download data to (default from config: {CLICKBENCH_DATA_DIR})",
    )

    args = parser.parse_args()

    if args.output_file:
        output_path = args.output_file
        os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    else:
        os.makedirs(args.output_dir, exist_ok=True)
        output_path = os.path.join(args.output_dir, CLICKBENCH_FILENAME)

    download_clickbench_data(output_path)


if __name__ == "__main__":
    main()
