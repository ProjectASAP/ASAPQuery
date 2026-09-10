import csv
import gzip
import importlib.util
import json
import tempfile
import unittest
from pathlib import Path


WRANGLER_PATH = Path(__file__).parents[1] / "dataset_wrangler.py"
SPEC = importlib.util.spec_from_file_location("dataset_wrangler", WRANGLER_PATH)
WRANGLER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(WRANGLER)


def task_usage_row(start, end, job="job", task="task", machine="machine"):
    row = [""] * 20
    row[0], row[1] = str(start), str(end)
    row[2], row[3], row[4] = job, task, machine
    row[18] = "0"
    return row


class DatasetWranglerTest(unittest.TestCase):
    def write_source(self, directory, rows):
        source = directory / "source.csv.gz"
        with gzip.open(source, "wt", newline="") as output:
            writer = csv.writer(output, lineterminator="\n")
            writer.writerows(rows)
        return source

    def spec(self):
        return {
            "source_file": "source.csv.gz",
            "source_time_range_us": [100, 200],
            "exported_metric": "test_metric",
            "grouping_labels": ["job_id", "task_index", "machine_id"],
            "rebase_to_offset": True,
        }

    def test_materializes_canonical_rebased_csv_and_manifest(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            self.write_source(directory, [task_usage_row(100, 110)])
            output = directory / "output"

            manifest = WRANGLER.materialize(self.spec(), directory, output)

            with gzip.open(output / "source.csv.gz", "rt", newline="") as result:
                line = result.read()
            self.assertTrue(line.startswith("600000000,600000010,job,task,machine"))
            self.assertTrue(line.endswith("\n"))
            self.assertNotIn("\r\n", line)
            self.assertEqual(manifest["records_loaded"], 1)
            self.assertEqual(manifest["grouping_state_count"], 1)
            persisted = json.loads((output / "scenario_manifest.json").read_text())
            self.assertEqual(
                persisted["output_payload_sha256"], manifest["output_payload_sha256"]
            )

    def test_rejects_duplicate_exported_samples(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            self.write_source(
                directory, [task_usage_row(100, 110), task_usage_row(100, 111)]
            )

            with self.assertRaisesRegex(ValueError, "duplicate exported sample"):
                WRANGLER.materialize(self.spec(), directory, directory / "output")


if __name__ == "__main__":
    unittest.main()
