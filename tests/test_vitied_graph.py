"""Graph facts must have exact, reusable keys and an auditable projection."""

import importlib.util
import unittest
from pathlib import Path


SPEC = importlib.util.spec_from_file_location(
    "vitied_graph", Path(__file__).resolve().parents[1] / "graph/load_vitied.py"
)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def fixture():
    return {
        "posts": [
            {"post_id": "7123456789012345678", "media_type": "video", "observation_count": 2},
            {"post_id": "7123456789012345679", "media_type": "photo", "observation_count": 1},
            {"post_id": "7123456789012345680", "media_type": "video", "observation_count": 1},
        ],
        "hashtags": [{"tag_key": "ẩmthực"}],
        "post_hashtags": [
            {"post_id": "7123456789012345678", "tag_key": "ẩmthực"},
            {"post_id": "7123456789012345679", "tag_key": "ẩmthực"},
        ],
        "sounds": [], "post_sounds": [],
    }


class GraphRules(unittest.TestCase):
    def test_projection_counts_unique_posts_and_isolates(self):
        tables = fixture()
        MODULE.validate_tables(tables)
        self.assertEqual({"component_count": 2, "largest_component_nodes": 3,
                          "largest_component_posts": 2, "isolated_posts": 1}, MODULE.connectivity(tables))

    def test_duplicate_or_orphan_edge_rejected(self):
        tables = fixture()
        tables["post_hashtags"].append(tables["post_hashtags"][0].copy())
        with self.assertRaisesRegex(ValueError, "Duplicate or orphan"):
            MODULE.validate_tables(tables)
        tables["post_hashtags"].pop()
        tables["post_hashtags"][0]["post_id"] = "7123456789012345681"
        with self.assertRaisesRegex(ValueError, "Duplicate or orphan"):
            MODULE.validate_tables(tables)

    def test_sound_requires_verified_id_and_one_per_post(self):
        tables = fixture()
        tables["sounds"] = [{"sound_id": "1.23457E+18"}]
        with self.assertRaisesRegex(ValueError, "Sound ID"):
            MODULE.validate_tables(tables)
        tables["sounds"] = [{"sound_id": "7123456789012345682"}, {"sound_id": "7123456789012345683"}]
        tables["post_sounds"] = [
            {"post_id": "7123456789012345678", "sound_id": "7123456789012345682"},
            {"post_id": "7123456789012345678", "sound_id": "7123456789012345683"},
        ]
        with self.assertRaisesRegex(ValueError, "Multiple Sound"):
            MODULE.validate_tables(tables)


if __name__ == "__main__":
    unittest.main()
