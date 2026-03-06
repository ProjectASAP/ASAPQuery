#!/usr/bin/env python3
import sys
import os
import argparse

# Check for gdown dependency
try:
    import gdown
except ImportError:
    # This should be handled by the shell script, but just in case:
    print("Error: 'gdown' is missing. Installing it now...")
    import subprocess

    subprocess.check_call([sys.executable, "-m", "pip", "install", "gdown"])
    import gdown

# H2O GroupBy Dataset ID
FILE_ID = "15SVQjQ2QehzYDLoDonio4aP7xqdMiNyi"
DEFAULT_FILENAME = "G1_1e7_1e2_0_0.csv"


def main():
    parser = argparse.ArgumentParser(description="Download H2O Benchmark Data")
    parser.add_argument(
        "--output-dir", required=True, help="Directory to save the file"
    )
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)
    output_path = os.path.join(args.output_dir, DEFAULT_FILENAME)

    # Simple check to avoid redownloading if it looks valid (>100MB)
    if os.path.exists(output_path) and os.path.getsize(output_path) > 100 * 1024 * 1024:
        print(f"File {output_path} already exists. Skipping download.")
        return

    print(f"Downloading H2O dataset (ID: {FILE_ID}) using gdown...")

    # gdown automatically handles the 'virus scan' warning and cookies
    url = f"https://drive.google.com/uc?id={FILE_ID}"
    gdown.download(url, output_path, quiet=False)


if __name__ == "__main__":
    main()
