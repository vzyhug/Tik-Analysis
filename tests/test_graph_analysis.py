import math
import unittest

from apps.web_dashboard.graph_analysis import _closeness_centrality, score_hashtag_network


class HashtagLinkAnalysisTests(unittest.TestCase):
    def test_common_tag_does_not_gain_weight_from_raw_frequency(self):
        result = score_hashtag_network(
            {"xuhuong": 1000, "alpha": 40, "beta": 40},
            [{"source": "alpha", "target": "xuhuong", "shared_posts": 40, "shared_creators": 30},
             {"source": "alpha", "target": "beta", "shared_posts": 20, "shared_creators": 20}],
            1000,
        )
        edges = {(edge["source"], edge["target"]) for edge in result["edges"]}
        self.assertNotIn(("alpha", "xuhuong"), edges)
        self.assertIn(("alpha", "beta"), edges)
        self.assertEqual(result["suppressed_pairs"], 1)

    def test_tag_used_in_more_than_five_percent_of_posts_is_kept_out_of_scoring(self):
        result = score_hashtag_network(
            {"xuhuong": 180, "alpha": 30, "beta": 30},
            [{"source": "alpha", "target": "xuhuong", "shared_posts": 20, "shared_creators": 20},
             {"source": "alpha", "target": "beta", "shared_posts": 15, "shared_creators": 10}],
            1000,
        )
        self.assertEqual(result["generic_tags_suppressed"], 1)
        self.assertEqual({edge["source"] for edge in result["edges"]} | {edge["target"] for edge in result["edges"]},
                         {"alpha", "beta"})

    def test_articlerank_and_closeness_score_surface_connector(self):
        tags = ["hub", "a", "b", "c", "d"]
        pairs = [{"source": "hub", "target": tag, "shared_posts": 50, "shared_creators": 10}
                 for tag in tags[1:]]
        result = score_hashtag_network({tag: 30 for tag in tags}, pairs, 1000)
        by_tag = {row["tag"]: row for row in result["rankings"]}
        self.assertTrue(all(row["articlerank"] > 0 for row in result["rankings"]))
        self.assertGreater(by_tag["hub"]["closeness"], max(by_tag[tag]["closeness"] for tag in tags[1:]))
        self.assertTrue(all(math.isfinite(row["articlerank"]) for row in result["rankings"]))

    def test_support_threshold_suppresses_small_pairs(self):
        result = score_hashtag_network(
            {"a": 100, "b": 100},
            [{"source": "a", "target": "b", "shared_posts": 2, "shared_creators": 10}],
            1000,
        )
        self.assertEqual(result["scored_pairs"], 0)
        self.assertEqual(result["suppressed_pairs"], 1)

    def test_closeness_handles_disconnected_nodes(self):
        scores = _closeness_centrality({"a": [("b", 1.0)], "b": [("a", 1.0)], "isolated": []})
        self.assertAlmostEqual(scores["a"], 0.5)
        self.assertAlmostEqual(scores["b"], 0.5)
        self.assertEqual(scores["isolated"], 0.0)


if __name__ == "__main__":
    unittest.main()
