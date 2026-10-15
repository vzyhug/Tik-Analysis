"""Behavior checks for the ViTIED CSV bridge and Silver null/ID rules."""

import importlib.util
import io
import sys
import unittest
from pathlib import Path


PATH = Path(__file__).resolve().parents[1] / "jobs/spark/03_vitied_silver.py"
SPEC = importlib.util.spec_from_file_location("vitied_silver", PATH)
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


class SilverRules(unittest.TestCase):
    def test_multiline_csv_preserves_both_records(self):
        one = ["anna", "7123456789012345678", "https://www.tiktok.com/@anna/video/7123456789012345678",
               "2025-01-01 10:00:00+07:00", "2025-01-02 10:00:00+07:00", "line 1\nline 2",
               "Cafe\u0301", "", "", "", "['Food']", "1", "2", "3", "Share", "4"]
        two = one.copy()
        two[1] = "7123456789012345679"
        two[2] = "https://www.tiktok.com/@anna/photo/7123456789012345679"
        buffer = io.StringIO(newline="")
        import csv
        writer = csv.writer(buffer)
        writer.writerow(MODULE.RAW_COLUMNS)
        writer.writerow(one)
        writer.writerow(two)
        parsed = list(MODULE.csv_records(("/tmp/fixture.csv", buffer.getvalue().encode())))
        self.assertEqual(2, len(parsed))
        self.assertEqual("line 1\nline 2", parsed[0][2]["vid_caption"])
        normalized = MODULE.normalize(parsed[0])
        self.assertEqual("7123456789012345678", normalized[3])
        self.assertIsNone(normalized[20])  # Share is missing, not zero.
        self.assertEqual(["café"], normalized[10])
        self.assertIsNone(normalized[24])  # No validation error.

    def test_metric_and_unicode_hashtag(self):
        self.assertEqual(1200000, MODULE.metric("1.2M"))
        self.assertEqual(22200, MODULE.metric("22.2K"))
        self.assertIsNone(MODULE.metric("Share"))
        self.assertIsNone(MODULE.metric(""))


if __name__ == "__main__":
    unittest.main()
