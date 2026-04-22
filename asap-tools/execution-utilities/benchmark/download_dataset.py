#!/usr/bin/env python3
"""
Unified dataset downloader for the ASAP benchmark pipeline.

Supports ClickBench (hits.json.gz), H2O groupby (G1_1e7_1e2_0_0.csv),
or any custom HTTP URL.

Usage:
    python download_dataset.py --dataset clickbench --output-dir ./data
    python download_dataset.py --dataset h2o --output-dir ./data
    python download_dataset.py --dataset custom --custom-url https://... --output-dir ./data
"""

import argparse
import os
import sys
import urllib.request

CLICKBENCH_URL = "https://datasets.clickhouse.com/hits_compatible/hits.json.gz"
CLICKBENCH_FILENAME = "hits.json.gz"

H2O_FILE_ID = "15SVQjQ2QehzYDLoDonio4aP7xqdMiNyi"
H2O_FILENAME = "G1_1e7_1e2_0_0.csv"


def _http_download(url: str, output_path: str) -> str:
    """Download a file via HTTP with progress reporting."""
    print(f"Downloading from {url}")
    request = urllib.request.Request(
        url, headers={"User-Agent": "Mozilla/5.0 (compatible; ASAP-Benchmark/1.0)"}
    )
    try:
        with urllib.request.urlopen(request) as response:
            total_size = int(response.headers.get("Content-Length", 0))
            downloaded = 0
            last_percent = -1
            block_size = 8192 * 128  # ~1 MB blocks

            with open(output_path, "wb") as f:
                while True:
                    block = response.read(block_size)
                    if not block:
                        break
                    f.write(block)
                    downloaded += len(block)
                    if total_size > 0:
                        percent = downloaded * 100 // total_size
                        if percent != last_percent:
                            last_percent = percent
                            mb = downloaded / (1024 * 1024)
                            total_mb = total_size / (1024 * 1024)
                            sys.stdout.write(
                                f"\rProgress: {percent}% ({mb:.1f}/{total_mb:.1f} MB)"
                            )
                            sys.stdout.flush()

        print("\nDownload complete!")
        return output_path

    except urllib.error.HTTPError as e:
        print(f"\nDownload failed: HTTP {e.code} - {e.reason}")
        raise


def download_clickbench(output_path: str, force: bool = False) -> str:
    """Download hits.json.gz from ClickHouse datasets CDN."""
    if not force and os.path.exists(output_path):
        print(f"Using existing file: {output_path}")
        return output_path
    print("Downloading ClickBench dataset (~14 GB compressed). Please wait...")
    return _http_download(CLICKBENCH_URL, output_path)


def download_h2o(output_path: str, force: bool = False) -> str:
    """Download H2O groupby CSV (~300 MB) from Google Drive via gdown."""
    if (
        not force
        and os.path.exists(output_path)
        and os.path.getsize(output_path) > 100 * 1024 * 1024
    ):
        print(f"Using existing file: {output_path}")
        return output_path

    try:
        import gdown
    except ImportError:
        print("Installing gdown...")
        import subprocess

        subprocess.check_call([sys.executable, "-m", "pip", "install", "gdown"])
        import gdown

    print(f"Downloading H2O dataset via gdown (ID: {H2O_FILE_ID})...")
    url = f"https://drive.google.com/uc?id={H2O_FILE_ID}"
    gdown.download(url, output_path, quiet=False)
    return output_path


def download_custom(url: str, output_path: str, force: bool = False) -> str:
    """Download a dataset from an arbitrary HTTP URL."""
    if not force and os.path.exists(output_path):
        print(f"Using existing file: {output_path}")
        return output_path
    return _http_download(url, output_path)


def main():
    parser = argparse.ArgumentParser(
        description="Download benchmark datasets",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--dataset",
        choices=["clickbench", "h2o", "custom"],
        required=True,
        help="Dataset to download",
    )
    parser.add_argument(
        "--output-dir",
        required=True,
        help="Directory to save the downloaded file",
    )
    parser.add_argument(
        "--output-file",
        default=None,
        help="Exact output file path (overrides --output-dir)",
    )
    parser.add_argument(
        "--custom-url",
        default=None,
        help="URL to download (required when --dataset custom)",
    )
    parser.add_argument(
        "--force-redownload",
        action="store_true",
        help="Re-download even if the file already exists",
    )
    args = parser.parse_args()

    if args.dataset == "custom" and not args.custom_url:
        parser.error("--custom-url is required when --dataset custom")

    os.makedirs(args.output_dir, exist_ok=True)

    if args.output_file:
        output_path = args.output_file
        os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
    elif args.dataset == "clickbench":
        output_path = os.path.join(args.output_dir, CLICKBENCH_FILENAME)
    elif args.dataset == "h2o":
        output_path = os.path.join(args.output_dir, H2O_FILENAME)
    else:
        filename = args.custom_url.rstrip("/").split("/")[-1] or "data"
        output_path = os.path.join(args.output_dir, filename)

    if args.dataset == "clickbench":
        download_clickbench(output_path, force=args.force_redownload)
    elif args.dataset == "h2o":
        download_h2o(output_path, force=args.force_redownload)
    else:
        download_custom(args.custom_url, output_path, force=args.force_redownload)

    print(f"Dataset saved to: {output_path}")


if __name__ == "__main__":
    main()
