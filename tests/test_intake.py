"""Behavior checks for the G01 inventory and raw copy guard."""

import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from scripts import intake


class IntakeTests(unittest.TestCase):
    def test_kol_reference_reconciliation(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "kol.csv"
            path.write_text("username,category_id,category_name\n@Alice,1,Food\n"
                            "alice,2,Travel\nbob,1,Food\n", encoding="utf-8")
            result = intake.reconcile_kol(path, {"alice"}, {"alice"})
            self.assertEqual(result["rows"], 3)
            self.assertEqual(result["unique_usernames"], 2)
            self.assertEqual(result["unique_categories"], 2)
            self.assertEqual(result["usernames_without_source_record"], ["bob"])

    def test_jsonl_and_long_id(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "rows.jsonl"
            path.write_text('{"id":"99999999999999999999999999"}\n', encoding="utf-8")
            fmt, rows = intake.read_records(path)
            self.assertEqual(fmt, "jsonl")
            self.assertEqual(rows[0]["id"], "99999999999999999999999999")

    def test_inventory_counts_conflicts_and_never_overwrites_raw(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source, raw, reports = root / "source", root / "data/raw/apify", root / "reports"
            source.mkdir()
            for pack in range(1, 9):
                rows = [{"id": str(10**25 + pack), "input": f"u{pack}",
                         "text": "", "hashtags": [], "playCount": 0,
                         "isAd": False, "isSponsored": None}]
                if pack == 1:
                    rows += [dict(rows[0]), {**rows[0], "text": "changed"},
                             {"error": "missing", "input": "gone"},
                             {"authorMeta": {"name": "profile"}, "input": "profile"}]
                (source / f"test_PACK{pack}.json").write_text(json.dumps(rows), encoding="utf-8")
            with patch.object(intake, "ROOT", root):
                result = intake.inventory(source, raw, reports)
                self.assertEqual(result["totals"]["records"], 12)
                self.assertEqual(result["totals"]["unique_video_ids"], 8)
                self.assertEqual(result["totals"]["exact_duplicate"], 1)
                self.assertEqual(result["totals"]["conflict"], 1)
                self.assertEqual(result["totals"]["source_error"], 1)
                self.assertEqual(result["totals"]["profile_only"], 1)
                self.assertEqual(result["missing_fields"].get("playCount", 0), 0)
                self.assertEqual(result["null_fields"]["isSponsored"], 10)
                self.assertEqual(result["null_fields"].get("isAd", 0), 0)
                self.assertEqual(result["totals"]["confirmed_empty_hashtags"], 10)
                intake.inventory(source, raw, reports)
                (raw / "test_PACK1.json").write_text("altered", encoding="utf-8")
                with self.assertRaisesRegex(ValueError, "refusing overwrite"):
                    intake.inventory(source, raw, reports)


if __name__ == "__main__":
    unittest.main()
