"""Compare serialized pattern JSON files from Python and Rust generators.

Exits with code 0 if equivalent, 1 otherwise.
"""

import json
import os
import sys


def load(path):
    with open(path, "r") as f:
        return json.load(f)


def normalize(value):
    """Normalize pattern structures for comparison: sort keys in dicts and recursively apply."""
    if isinstance(value, dict):
        return {k: normalize(value[k]) for k in sorted(value.keys())}
    if isinstance(value, list):
        return [normalize(v) for v in value]
    return value


def main():
    base = os.path.dirname(__file__)
    out_dir = os.path.join(base, "out")
    py_path = os.path.join(out_dir, "python_patterns.json")
    rs_path = os.path.join(out_dir, "rust_patterns.json")

    if not os.path.exists(py_path) or not os.path.exists(rs_path):
        print(
            "Missing generated pattern files. Run python_generate_patterns.py and rust generator."
        )
        sys.exit(2)

    py = load(py_path)
    rs = load(rs_path)

    py_n = normalize(py)
    rs_n = normalize(rs)

    if py_n == rs_n:
        print("Patterns match")
        sys.exit(0)
    else:
        print("Patterns differ")
        print("--- Python patterns ---")
        print(json.dumps(py_n, indent=2))
        print("--- Rust patterns ---")
        print(json.dumps(rs_n, indent=2))
        sys.exit(1)


if __name__ == "__main__":
    main()
