"""G05 split groups exact repeated captions before fitting features."""

import importlib.util
import unittest
from pathlib import Path


PATH = Path(__file__).resolve().parents[1] / "scripts/prepare_g05_features.py"
SPEC = importlib.util.spec_from_file_location("g05_features", PATH)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class G05Preparation(unittest.TestCase):
    def test_shared_caption_keeps_authors_in_one_split(self):
        caption = "Cùng một nội dung bài đăng được sao chép nguyên văn"
        rows = [
            {"author_username": "author_a", "caption": caption},
            {"author_username": "author_b", "caption": caption},
            {"author_username": "author_c", "caption": "Một nội dung hoàn toàn khác để kiểm tra"},
        ]
        groups = MODULE.author_groups(rows)
        self.assertEqual(groups["author_a"], groups["author_b"])
        self.assertEqual(MODULE.author_split(groups["author_a"]), MODULE.author_split(groups["author_b"]))

    def test_empty_content_and_unicode_tag(self):
        self.assertFalse(MODULE.content_text({"caption": None, "hashtags": None}).strip())
        self.assertIn("tag_café", MODULE.content_text({"caption": "", "hashtags": ["cafe\u0301"]}))


if __name__ == "__main__":
    unittest.main()
