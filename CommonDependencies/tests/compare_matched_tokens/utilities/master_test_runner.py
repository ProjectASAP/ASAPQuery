#!/usr/bin/env python3

import os
import sys
import subprocess
import json
from pathlib import Path
from datetime import datetime


class MasterTestRunner:
    def __init__(self, base_dir: str):
        self.base_dir = Path(base_dir).resolve()
        self.test_data_file = self.base_dir / "test_data" / "promql_queries.json"
        self.python_dir = self.base_dir / "python_tests"
        self.rust_dir = self.base_dir / "rust_tests"
        self.comparison_dir = self.base_dir / "comparison_tests"

    def run_all_tests(self):
        """Run the complete test suite: Python, Rust, and comparison"""

        print("🚀 Starting Cross-Language PromQL Pattern Testing")
        print("=" * 60)

        if not self.test_data_file.exists():
            print(f"❌ Test data file not found: {self.test_data_file}")
            return False

        # Run Python tests
        print("\n📍 Step 1: Running Python tests...")
        python_success = self._run_python_tests()

        # Run Rust tests
        print("\n📍 Step 2: Running Rust tests...")
        rust_success = self._run_rust_tests()

        # Compare results
        if python_success and rust_success:
            print("\n📍 Step 3: Comparing results...")
            self._compare_results()
        else:
            print("\n⚠️  Skipping comparison due to test failures")

        print(f"\n✅ Test suite completed at {datetime.now()}")
        return python_success and rust_success

    def _run_python_tests(self) -> bool:
        """Run Python test suite"""
        try:
            os.chdir(self.python_dir)

            cmd = [sys.executable, "test_runner.py", str(self.test_data_file)]

            print(f"Running: {' '.join(cmd)}")
            result = subprocess.run(cmd, capture_output=True, text=True)

            print("Python test output:")
            print(result.stdout)
            if result.stderr:
                print("Python test errors:")
                print(result.stderr)

            return result.returncode == 0

        except Exception as e:
            print(f"❌ Error running Python tests: {e}")
            return False
        finally:
            os.chdir(self.base_dir)

    def _run_rust_tests(self) -> bool:
        """Run Rust test suite"""
        try:
            os.chdir(self.rust_dir)

            # Build the Rust project first
            print("Building Rust test runner...")
            build_result = subprocess.run(
                ["cargo", "build", "--release"], capture_output=True, text=True
            )

            if build_result.returncode != 0:
                print("❌ Rust build failed:")
                print(build_result.stderr)
                return False

            # Run the tests
            cmd = ["cargo", "run", "--release", "--", str(self.test_data_file)]

            print(f"Running: {' '.join(cmd)}")
            result = subprocess.run(cmd, capture_output=True, text=True)

            print("Rust test output:")
            print(result.stdout)
            if result.stderr:
                print("Rust test errors:")
                print(result.stderr)

            return result.returncode == 0

        except Exception as e:
            print(f"❌ Error running Rust tests: {e}")
            return False
        finally:
            os.chdir(self.base_dir)

    def _compare_results(self):
        """Compare Python and Rust test results"""
        try:
            python_results = self.python_dir / "python_test_results.json"
            rust_results = self.rust_dir / "rust_test_results.json"

            if not python_results.exists():
                print("❌ Python results file not found")
                return

            if not rust_results.exists():
                print("❌ Rust results file not found")
                return

            os.chdir(self.comparison_dir)

            cmd = [
                sys.executable,
                "result_comparator.py",
                str(python_results),
                str(rust_results),
            ]

            print(f"Running: {' '.join(cmd)}")
            result = subprocess.run(cmd, capture_output=True, text=True)

            print("Comparison output:")
            print(result.stdout)
            if result.stderr:
                print("Comparison errors:")
                print(result.stderr)

        except Exception as e:
            print(f"❌ Error comparing results: {e}")
        finally:
            os.chdir(self.base_dir)

    def generate_test_summary(self):
        """Generate a comprehensive test summary"""
        summary = {
            "timestamp": datetime.utcnow().isoformat(),
            "test_data_file": str(self.test_data_file),
            "files_generated": [],
        }

        # Collect generated files
        for results_file in [
            self.python_dir / "python_test_results.json",
            self.rust_dir / "rust_test_results.json",
            self.comparison_dir / "comparison_report.json",
        ]:
            if results_file.exists():
                summary["files_generated"].append(str(results_file))

        summary_file = self.base_dir / "test_summary.json"
        with open(summary_file, "w") as f:
            json.dump(summary, f, indent=2)

        print(f"📊 Test summary written to: {summary_file}")


def main():
    script_dir = Path(__file__).parent.parent
    runner = MasterTestRunner(str(script_dir))

    success = runner.run_all_tests()
    runner.generate_test_summary()

    if success:
        print("\n🎉 All tests completed successfully!")
        sys.exit(0)
    else:
        print("\n💥 Some tests failed. Check the output above.")
        sys.exit(1)


if __name__ == "__main__":
    main()
