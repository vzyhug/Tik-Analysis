import unittest

from scripts.intake_vitied import analyze, categories_from_source, media_from_url


def source_row(number, raw_id, url, observed="2025-06-25 10:00:00+07:00", caption="nội dung"):
    values = {
        "user_name": "creator", "vid_id": raw_id, "vid_url": url,
        "vid_postTime": "2025-06-24 10:00:00+07:00", "vid_scrapeTime": observed,
        "vid_caption": caption, "vid_hashtags": "tag", "music_id": "1.23e+18",
        "topic": "['Beauty']", "vid_duration": "10", "music_title": "sound",
        "music_authorName": "author", "vid_nview": "1.2K", "vid_nlike": "12",
        "vid_ncomment": "1", "vid_nshare": "0", "vid_nsave": "0",
    }
    return {"source_file": "streaming_data.csv", "source_row": number, "values": values}


class ViTIEDIntakeTests(unittest.TestCase):
    def test_video_photo_url_validation(self):
        self.assertEqual(media_from_url("https://www.tiktok.com/@x/photo/1234567890123456789"),
                         ("1234567890123456789", "photo", "x"))
        self.assertEqual(media_from_url("https://evil.example/@x/video/1234567890123456789"),
                         (None, None, None))
        self.assertEqual(categories_from_source("['Beauty', 'Fashion']"),
                         (["beauty", "fashion"], None))
        self.assertEqual(categories_from_source("[nan]"), (None, None))

    def test_reconcile_rounded_ids_and_quarantine_bad_observations(self):
        video_url = "https://www.tiktok.com/@creator/video/1234567890123456789"
        photo_url = "https://www.tiktok.com/@creator/photo/1234567890123456999"
        rows = [
            source_row(2, "1234567890123456000", video_url),
            source_row(3, "1234567890123456000", video_url, "2025-06-26 10:00:00+07:00"),
            source_row(4, "1234567890123456000", video_url),
            source_row(5, "1234567890123456000", video_url, caption="khác"),
            source_row(6, "1234567890123457000", photo_url),
            source_row(7, "1234567890123458000", "https://evil.example/@creator/video/1234567890123458999"),
        ]
        report, identities, rejected = analyze(rows, [])
        self.assertEqual(report["counts"]["source_rows"], 6)
        self.assertEqual(report["counts"]["accepted_observation_rows"], 3)
        self.assertEqual(report["counts"]["quarantine_rows"], 3)
        self.assertEqual(report["counts"]["canonical_video_posts"], 1)
        self.assertEqual(report["counts"]["canonical_photo_posts"], 1)
        self.assertEqual({item["canonical_post_id"] for item in identities},
                         {"1234567890123456789", "1234567890123456999"})
        self.assertEqual({item["reason"] for item in rejected},
                         {"duplicate_observation", "conflicting_observation", "no_trusted_media_url_id"})

    def test_ambiguous_raw_id_rejects_both_mappings(self):
        rows = [
            source_row(2, "1234567890123456000", "https://tiktok.com/@creator/video/1234567890123456789"),
            source_row(3, "1234567890123456000", "https://tiktok.com/@creator/video/1234567890123456790",
                       "2025-06-26 10:00:00+07:00"),
        ]
        report, identities, rejected = analyze(rows, [])
        self.assertEqual(report["identity"]["raw_ids_mapping_to_multiple_url_ids"], 1)
        self.assertEqual(report["counts"]["accepted_observation_rows"], 0)
        self.assertEqual(len(identities), 0)
        self.assertEqual(len(rejected), 2)


if __name__ == "__main__":
    unittest.main()
