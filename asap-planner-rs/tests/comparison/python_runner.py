#!/usr/bin/env python3
"""Run the Python planner for a test case and return output paths."""
import os
import sys
import subprocess
import tempfile
import json

def run_python_planner(test_case: dict, output_dir: str) -> tuple[str, str]:
    """Run Python planner and return (streaming_config_path, inference_config_path)."""
    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))
    planner_script = os.path.join(repo_root, "asap-planner", "main_controller.py")

    args = [
        sys.executable, planner_script,
        "--input_config", os.path.join(os.path.dirname(__file__), test_case["input_config"]),
        "--output_dir", output_dir,
        "--prometheus_scrape_interval", str(test_case["prometheus_scrape_interval"]),
        "--streaming_engine", test_case["streaming_engine"],
    ]
    if test_case.get("enable_punting"):
        args.append("--enable-punting")
    if test_case.get("range_duration", 0) > 0:
        args += ["--range-duration", str(test_case["range_duration"])]
    if test_case.get("step", 0) > 0:
        args += ["--step", str(test_case["step"])]

    env = os.environ.copy()
    python_utils_path = os.path.join(repo_root, "asap-common", "dependencies", "py", "promql_utilities")
    planner_path = os.path.join(repo_root, "asap-planner")
    env["PYTHONPATH"] = f"{python_utils_path}:{planner_path}:{env.get('PYTHONPATH', '')}"

    result = subprocess.run(args, env=env, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"Python planner failed:\n{result.stderr}")

    return (
        os.path.join(output_dir, "streaming_config.yaml"),
        os.path.join(output_dir, "inference_config.yaml"),
    )


if __name__ == "__main__":
    test_cases_path = os.path.join(os.path.dirname(__file__), "test_cases.json")
    with open(test_cases_path) as f:
        test_cases = json.load(f)["test_cases"]

    for tc in test_cases:
        with tempfile.TemporaryDirectory() as tmp:
            streaming, inference = run_python_planner(tc, tmp)
            print(f"[OK] {tc['id']}: {streaming}, {inference}")
