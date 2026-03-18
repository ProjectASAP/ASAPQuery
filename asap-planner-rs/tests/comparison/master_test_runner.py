#!/usr/bin/env python3
"""Run all comparison tests between Python and Rust planner."""
import os
import sys
import json
import shutil
import subprocess

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))
RUST_BINARY = os.path.join(REPO_ROOT, "target", "release", "asap-planner")
COMPARATOR = os.path.join(os.path.dirname(__file__), "comparator.py")
TEST_CASES_PATH = os.path.join(os.path.dirname(__file__), "test_cases.json")
COMPARISON_DIR = os.path.dirname(__file__)
TEST_OUTPUTS_DIR = os.path.join(os.path.dirname(__file__), "test_outputs")


def run_python_planner(tc: dict, output_dir: str) -> bool:
    planner_script = os.path.join(REPO_ROOT, "asap-planner", "main_controller.py")
    args = [
        sys.executable,
        planner_script,
        "--input_config",
        os.path.join(COMPARISON_DIR, tc["input_config"]),
        "--output_dir",
        output_dir,
        "--prometheus_scrape_interval",
        str(tc["prometheus_scrape_interval"]),
        "--streaming_engine",
        tc["streaming_engine"],
    ]
    if tc.get("enable_punting"):
        args.append("--enable-punting")
    if tc.get("range_duration", 0) > 0:
        args += ["--range-duration", str(tc["range_duration"])]
    if tc.get("step", 0) > 0:
        args += ["--step", str(tc["step"])]

    env = os.environ.copy()
    py_utils = os.path.join(
        REPO_ROOT, "asap-common", "dependencies", "py", "promql_utilities"
    )
    planner_path = os.path.join(REPO_ROOT, "asap-planner")
    env["PYTHONPATH"] = f"{py_utils}:{planner_path}:{env.get('PYTHONPATH', '')}"

    result = subprocess.run(args, env=env, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"  Python planner failed:\n{result.stderr}")
        return False
    return True


def run_rust_planner(tc: dict, output_dir: str) -> bool:
    args = [
        RUST_BINARY,
        "--input_config",
        os.path.join(COMPARISON_DIR, tc["input_config"]),
        "--output_dir",
        output_dir,
        "--prometheus_scrape_interval",
        str(tc["prometheus_scrape_interval"]),
        "--streaming_engine",
        tc["streaming_engine"],
    ]
    if tc.get("enable_punting"):
        args.append("--enable-punting")
    if tc.get("range_duration", 0) > 0:
        args += ["--range-duration", str(tc["range_duration"])]
    if tc.get("step", 0) > 0:
        args += ["--step", str(tc["step"])]

    result = subprocess.run(args, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"  Rust planner failed:\n{result.stderr}")
        return False
    return True


def main():
    # Build Rust binary
    print("Building Rust planner...")
    build = subprocess.run(
        ["cargo", "build", "-p", "asap_planner", "--release"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )
    if build.returncode != 0:
        print(f"Build failed:\n{build.stderr}")
        sys.exit(1)
    print("Build OK\n")

    with open(TEST_CASES_PATH) as f:
        test_cases = json.load(f)["test_cases"]

    passed = 0
    failed = 0

    # Clear and recreate test_outputs dir
    if os.path.exists(TEST_OUTPUTS_DIR):
        shutil.rmtree(TEST_OUTPUTS_DIR)
    os.makedirs(TEST_OUTPUTS_DIR)

    for tc in test_cases:
        print(f"Test: {tc['id']}")
        tc_out = os.path.join(TEST_OUTPUTS_DIR, tc["id"])
        py_dir = os.path.join(tc_out, "python")
        rs_dir = os.path.join(tc_out, "rust")
        os.makedirs(py_dir)
        os.makedirs(rs_dir)

        if not run_python_planner(tc, py_dir):
            print("  [FAIL] Python planner error")
            failed += 1
            continue
        if not run_rust_planner(tc, rs_dir):
            print("  [FAIL] Rust planner error")
            failed += 1
            continue

        result = subprocess.run(
            [
                sys.executable,
                COMPARATOR,
                os.path.join(py_dir, "streaming_config.yaml"),
                os.path.join(py_dir, "inference_config.yaml"),
                os.path.join(rs_dir, "streaming_config.yaml"),
                os.path.join(rs_dir, "inference_config.yaml"),
            ],
            capture_output=True,
            text=True,
        )
        if result.returncode == 0:
            print("  [PASS]")
            passed += 1
        else:
            print("  [FAIL]")
            print(result.stdout)
            failed += 1

    print(f"\nResults: {passed}/{passed + failed} passed")
    if failed > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
