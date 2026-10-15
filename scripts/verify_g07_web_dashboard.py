"""Verify the web dashboard against the current Neo4j ViTIED graph and G05 model."""

from __future__ import annotations

import json
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
import time
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
BASE_URL = "http://127.0.0.1:8787"


def get_json(path: str) -> tuple[int, Any, str | None]:
    request = urllib.request.Request(BASE_URL + path, headers={"Accept": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            return response.status, json.load(response), response.headers.get("Cache-Control")
    except urllib.error.HTTPError as exc:
        return exc.code, json.load(exc), exc.headers.get("Cache-Control")


def check(condition: bool, message: str, checks: list[dict[str, Any]]) -> None:
    checks.append({"name": message, "passed": bool(condition)})
    if not condition:
        raise AssertionError(message)


def main() -> None:
    graph = json.loads((ROOT / "outputs/quality/g04_graph_load.json").read_text(encoding="utf-8"))
    g01 = json.loads((ROOT / "outputs/quality/vitied_g01.json").read_text(encoding="utf-8"))
    checks: list[dict[str, Any]] = []
    status, health, _ = 0, {}, None
    for attempt in range(20):
        try:
            status, health, _ = get_json("/api/health")
            if status == 200 and health.get("status") == "ok":
                break
        except OSError:
            pass
        if attempt < 19:
            time.sleep(1)
    check(status == 200 and health.get("source") == "Neo4j ViTIED graph", "Health endpoint confirms Neo4j graph source", checks)
    check(int(health.get("posts", 0)) == graph["expected"]["posts"]
          and int(health.get("observations", 0)) == graph["expected"]["observations"],
          "Live Neo4j Post/Observation nodes reconcile to graph load", checks)

    status, meta, cache = get_json("/api/meta")
    check(status == 200 and "vitied-g04-v2" in meta["publication"]["publication_id"], "Graph metadata endpoint identifies active version", checks)
    check(cache == "no-store", "API responses are not cached", checks)
    start, end = meta["period"]["min_date"], meta["period"]["max_date"]

    def dashboard(media: str, day_from: str = start, day_to: str = end) -> dict[str, Any]:
        query = urllib.parse.urlencode({"from": day_from, "to": day_to, "media_type": media})
        code, payload, _ = get_json("/api/dashboard?" + query)
        check(code == 200, f"Neo4j dashboard query ({media}, {day_from}..{day_to})", checks)
        return payload

    all_data = dashboard("all")
    video, photo = dashboard("video"), dashboard("photo")
    check(all_data["kpis"]["posts"] == graph["expected"]["posts"], "All-media cohort reconciles to unique graph Posts", checks)
    check(all_data["kpis"]["observation_count"] == graph["expected"]["observations"], "Observation timeline reconciles to graph", checks)
    check(video["kpis"]["posts"] == g01["counts"]["canonical_video_posts"]
          and photo["kpis"]["posts"] == g01["counts"]["canonical_photo_posts"],
          "Video/photo filters reconcile to G01", checks)
    check(sum(row["posts"] for row in all_data["posts_by_week"]) == all_data["kpis"]["posts"],
          "Weekly content timeline reconciles to graph Post count", checks)
    check(sum(row["posts"] for row in all_data["posts_by_week_media"]) == all_data["kpis"]["posts"],
          "Media-by-week report reconciles to cohort", checks)
    check(sum(row["snapshots"] for row in all_data["snapshots_by_day"]) == all_data["kpis"]["observation_count"],
          "Snapshot chart reconciles to observation count", checks)
    check(all_data["latest_metrics"]["posts"] == all_data["kpis"]["observed_posts"],
          "Latest observation metrics have one row per observed Post", checks)
    check(sum(row["posts"] for row in all_data["cluster_engagement"]) == 8254,
          "Caption-only DL4J cohort assignments reconcile", checks)
    check(len(all_data["cluster_engagement"]) == 8 and len(all_data["creator_metrics"]) <= 12,
          "Dashboard returns cluster performance and bounded creator comparison", checks)
    check(all(row["median_like_per_view"] is None or 0 <= row["median_like_per_view"] <= 1
              for row in all_data["cluster_engagement"]), "Cluster like/view rates are valid proportions", checks)
    trend = all_data["future_trends"]
    check(trend.get("ready") and trend["horizon_weeks"] == 4 and len(trend["forecast"]) == 36,
          "Four-week caption-cluster share projection is available", checks)
    weekly_forecast_sums: dict[str, float] = {}
    for item in trend["forecast"]:
        weekly_forecast_sums[item["period"]] = weekly_forecast_sums.get(item["period"], 0.0) + item["share"]
    check(all(abs(total - 1.0) < 1e-8 for total in weekly_forecast_sums.values()),
          "Forecast cluster shares plus unassigned cohort sum to one", checks)
    check(trend["backtest"]["n_folds"] >= 3 and trend["backtest"]["status"] in {"better_than_flat", "not_better_than_flat"},
          "Forecast method has at least three chronological backtest windows", checks)
    check(trend["method_used"] == ("damped trend" if trend["backtest"]["status"] == "better_than_flat" else "flat-share baseline"),
          "Dashboard serves the backtest winner instead of an unvalidated trend model", checks)
    check("not strict out-of-time validation" in trend["backtest_scope"],
          "Forecast output discloses cluster-fit leakage limitation", checks)
    check(trend["source_end"] == end and trend["history_weeks"] >= 12,
          "Trend projection discloses source end and has at least 12 consecutive weeks", checks)

    query = urllib.parse.urlencode({"from": start, "to": end, "rank_by": "articlerank", "display_limit": 40})
    network_code, network, _ = get_json("/api/graph-analysis?" + query)
    check(network_code == 200 and network.get("summary", {}).get("hashtags", 0) > 10000,
          "Global hashtag network covers the graph population", checks)
    check(len(network.get("nodes", [])) <= 40 and network["summary"]["candidate_pairs"] > network["summary"]["scored_pairs"],
          "Link analysis bounds the visible network and suppresses weak/common pairs", checks)
    node_ids = {node["id"] for node in network.get("nodes", [])}
    check(all(edge["source"] in node_ids and edge["target"] in node_ids
              and edge["shared_posts"] >= 3 and edge["shared_creators"] >= 2 and edge["weight"] > 0
              for edge in network.get("edges", [])),
          "Displayed links pass support, creator, and positive-weight rules", checks)
    closeness_query = urllib.parse.urlencode({"from": start, "to": end, "rank_by": "closeness", "display_limit": 40})
    closeness_code, closeness_network, _ = get_json("/api/graph-analysis?" + closeness_query)
    check(closeness_code == 200 and closeness_network.get("rank_by") == "closeness"
          and closeness_network["summary"]["hashtags"] == network["summary"]["hashtags"]
          and all(item["closeness"] > 0 for item in closeness_network["closeness_rankings"])
          and all(closeness_network["closeness_rankings"][i]["closeness"] >= closeness_network["closeness_rankings"][i + 1]["closeness"]
                  for i in range(len(closeness_network["closeness_rankings"]) - 1)),
          "Closeness ranking uses the same full graph population", checks)

    latest_day = dashboard("all", end, end)
    check(latest_day["kpis"]["posts"] == 0 and latest_day["kpis"]["observation_count"] > 0,
          "Published-date cohort and observation-date timeline remain distinct", checks)
    reversed_query = urllib.parse.urlencode({"from": end, "to": start, "media_type": "all"})
    code, _, _ = get_json("/api/dashboard?" + reversed_query)
    check(code == 422, "Reversed date range is rejected", checks)

    model_code, model_report, _ = get_json("/api/model-analysis")
    check(model_code == 200 and model_report["passed"], "Caption-only DL4J analysis artifact is served", checks)
    check(model_report["feature_version"] == "vitied-g05-caption-idf512-v1"
          and model_report.get("feature_mode") == "caption_only"
          and model_report.get("descriptor_version") == "caption-terms-v2-stop-unaccented-filler"
          and all("caption_keywords" in item and "enriched_hashtags" not in item for item in model_report["clusters"]),
          "DL4J dashboard uses caption-only features and filtered caption descriptors", checks)
    with urllib.request.urlopen(BASE_URL + "/", timeout=8) as response:
        page = response.read().decode("utf-8")
    with urllib.request.urlopen(BASE_URL + "/static/app.js", timeout=8) as response:
        js_source = response.read().decode("utf-8")
    customer_ui = page + js_source
    check('class="report-sidebar"' in page and 'class="report-nav"' in page
          and all(f'data-view="{name}"' in page for name in ("overview", "momentum", "projection", "engagement", "groups", "network")),
          "Customer dashboard has sidebar navigation to six dedicated report views", checks)
    check('data-report="momentum"' in page and 'data-report="projection"' in page
          and 'data-report="engagement"' in page and 'data-report="groups"' in page
          and "độ phủ creator" in page.casefold(),
          "Report sections separate momentum, projection, engagement, and group comparison", checks)
    check('data-report="network"' in page and 'id="network-stage"' in page
          and 'id="network-rank"' in page and 'id="articlerank-table"' in page
          and 'id="closeness-table"' in page and "ArticleRank" in page and "Closeness Centrality" in page
          and "Betweenness" not in page + js_source
          and '/api/graph-analysis?' in js_source and 'id="network-hashtag"' not in page,
          "Network report ranks the global hashtag graph with ArticleRank and closeness centrality scores", checks)
    with urllib.request.urlopen(BASE_URL + "/static/styles.css", timeout=8) as response:
        styles = response.read().decode("utf-8")
    check("Be+Vietnam+Pro" in page and "Segoe UI" in styles and "workspace-header h1{font-size:29px" in styles,
          "Vietnamese-first typography and larger report hierarchy are served", checks)
    check('id="media"' not in page and "media_type" not in page and "post_id" not in customer_ui
          and "Post ID" not in page and "Video và ảnh theo tuần" not in page,
          "Customer UI hides media-type controls and raw post identifiers", checks)
    check("DL4J" not in page and "embedding" not in page and "Cụm 0" not in customer_ui,
          "Customer UI hides model diagnostics and machine cluster identifiers", checks)
    check(all(term not in customer_ui.casefold() for term in ("chưa phải xu hướng", "không phải cam kết", "chưa được con người xác nhận", "không đại diện")),
          "Customer reports use concise reporting language without defensive caveats", checks)
    check("PostgreSQL" not in customer_ui and "top hashtag" not in customer_ui.casefold()
          and "cặp hashtag" not in customer_ui.casefold(),
          "Customer UI avoids database labels and hashtag popularity reports", checks)

    report = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "graph_version": graph["graph_version"],
        "base_url": BASE_URL,
        "checks": checks,
        "api_results": {"posts": all_data["kpis"]["posts"], "observations": all_data["kpis"]["observation_count"],
                        "video_posts": video["kpis"]["posts"], "photo_posts": photo["kpis"]["posts"],
                        "clusters": len(all_data["cluster_engagement"]), "creators": len(all_data["creator_metrics"]),
                        "weekly_periods": len(all_data["posts_by_week"]), "observation_days": len(all_data["snapshots_by_day"]),
                        "hashtags": network["summary"]["hashtags"], "candidate_pairs": network["summary"]["candidate_pairs"],
                        "scored_pairs": network["summary"]["scored_pairs"],
                        "generic_tags_suppressed": network["summary"]["generic_tags_suppressed"],
                        "suppressed_pairs": network["summary"]["suppressed_pairs"],
                        "articlerank_leader": network["summary"]["articlerank_leader"],
                        "closeness_leader": network["summary"]["closeness_leader"],
                        "date_range": {"from": start, "to": end}},
        "passed": all(item["passed"] for item in checks),
        "visual_browser_review": "not performed",
    }
    output = ROOT / "outputs/quality/g07_web_dashboard.json"
    output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"passed": report["passed"], "checks": len(checks), "output": str(output.relative_to(ROOT))}, ensure_ascii=False))


if __name__ == "__main__":
    main()
