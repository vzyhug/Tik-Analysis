from __future__ import annotations

import unittest
from datetime import date, timedelta

from apps.web_dashboard.main import build_trend_analysis


class DashboardTrendTests(unittest.TestCase):
    def test_projection_conserves_share_and_detects_broadening_topic_cohort(self) -> None:
        posts = []
        assignments = {}
        first_week = date(2025, 1, 6)
        for week_index in range(20):
            week = first_week + timedelta(days=7 * week_index)
            counts = (4, 1) if week_index >= 16 else (2, 3)
            for cluster, count in enumerate(counts):
                for i in range(count):
                    post_id = f"{week_index}-{cluster}-{i}"
                    posts.append({"post_id": post_id, "published_date": (week + timedelta(days=i)).isoformat(),
                                  "author_username": f"creator-{cluster}-{i}"})
                    assignments[post_id] = cluster

        period_end = first_week + timedelta(days=7 * 19 + 6)
        analysis = build_trend_analysis(posts, assignments,
                                        [{"cluster": 0, "caption_keywords": [{"term": "ẩm thực"}]},
                                         {"cluster": 1, "caption_keywords": []}],
                                        first_week, period_end)

        self.assertTrue(analysis["ready"])
        self.assertEqual(analysis["history_weeks"], 20)
        self.assertEqual(analysis["backtest"]["n_folds"], 3)
        self.assertEqual(len(analysis["forecast_candidates"]), 12)
        shares_by_week: dict[str, float] = {}
        for row in analysis["forecast"]:
            shares_by_week[row["period"]] = shares_by_week.get(row["period"], 0.0) + row["share"]
        self.assertTrue(all(abs(value - 1.0) < 1e-9 for value in shares_by_week.values()))
        cluster_zero = next(row for row in analysis["momentum"] if row["cluster"] == 0)
        self.assertGreater(cluster_zero["share_change_pp"], 0)
        self.assertEqual(cluster_zero["creators_recent"], 4)

    def test_short_history_returns_explicit_unavailable_state(self) -> None:
        start = date(2025, 1, 6)
        posts = [{"post_id": str(i), "published_date": (start + timedelta(days=7 * i)).isoformat()}
                 for i in range(8)]
        analysis = build_trend_analysis(posts, {str(i): 0 for i in range(8)},
                                        [{"cluster": 0, "caption_keywords": []}],
                                        start, start + timedelta(days=7 * 7 + 6))
        self.assertFalse(analysis["ready"])
        self.assertIn("12 tuần", analysis["reason"])


if __name__ == "__main__":
    unittest.main()
