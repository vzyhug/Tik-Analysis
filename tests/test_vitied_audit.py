import csv
import tempfile
import unittest
from pathlib import Path

from scripts.audit_vitied import audit_file


class ViTIEDAuditTests(unittest.TestCase):
    def test_snapshots_and_source_id_conflict_are_reported_without_repair(self):
        columns = ["vid_id", "vid_url", "vid_postTime", "vid_scrapeTime", "topic", "vid_caption", "music_id"]
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "streaming_data.csv"
            with path.open("w", encoding="utf-8", newline="") as source:
                writer = csv.DictWriter(source, fieldnames=columns)
                writer.writeheader()
                first = dict(vid_id="1234567890123456000", vid_url="https://www.tiktok.com/@x/video/1234567890123456789",
                             vid_postTime="2025-01-01 10:00:00+07:00", vid_scrapeTime="2025-01-02 10:00:00+07:00",
                             topic="['Fashion']", vid_caption="áo đẹp", music_id="1.23e+18")
                writer.writerow(first)
                writer.writerow({**first, "vid_scrapeTime": "2025-01-03 10:00:00+07:00"})
                writer.writerow(first)
            result = audit_file(path)
            self.assertEqual(result["row_count"], 3)
            self.assertEqual(result["distinct_raw_video_ids"], 1)
            self.assertEqual(result["distinct_raw_video_observation_keys"], 2)
            self.assertEqual(result["issues"]["media_id_url_mismatch"], 3)
            self.assertEqual(result["issues"]["duplicate_video_observation"], 1)
            self.assertEqual(result["issues"]["non_integer_music_id_text"], 3)


if __name__ == "__main__":
    unittest.main()
