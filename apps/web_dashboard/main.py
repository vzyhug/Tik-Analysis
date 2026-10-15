from __future__ import annotations

import base64
import heapq
import json
import math
import os
import random
import statistics
import time
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

try:
    from .graph_analysis import score_hashtag_network
except ImportError:  # Uvicorn loads this file as top-level module `main` in Docker.
    from graph_analysis import score_hashtag_network


APP_DIR = Path(__file__).resolve().parent
NEO4J_HTTP_URL = os.getenv("NEO4J_HTTP_URL", "http://neo4j:7474").rstrip("/")
NEO4J_AUTH_FILE = Path(os.getenv("NEO4J_AUTH_FILE", "/run/secrets/neo4j_auth"))
MODEL_ANALYSIS_FILE = APP_DIR / "model_analysis.json"
DATASET_ID = "vitied"
TIMEZONE = "Asia/Ho_Chi_Minh"
GRAPH_CACHE_TTL_SECONDS = 20
app = FastAPI(title="ViTIED Topic Analysis", docs_url=None, redoc_url=None)
app.mount("/static", StaticFiles(directory=APP_DIR / "static"), name="static")
_graph_cache: tuple[float, list[dict[str, Any]]] | None = None
_link_analysis_cache: dict[str, tuple[float, dict[str, Any]]] = {}


@app.middleware("http")
async def disable_api_cache(request: Any, call_next: Any) -> Any:
    response = await call_next(request)
    if request.url.path.startswith("/api/"):
        response.headers["Cache-Control"] = "no-store"
    return response


def neo4j_auth() -> str:
    try:
        raw = NEO4J_AUTH_FILE.read_text(encoding="utf-8").strip()
    except OSError as exc:
        raise RuntimeError("Neo4j auth secret is unavailable") from exc
    if "/" not in raw:
        raise RuntimeError("Neo4j auth secret must contain username/password")
    return "Basic " + base64.b64encode(raw.replace("/", ":", 1).encode("utf-8")).decode("ascii")


def cypher(statement: str, parameters: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    payload = json.dumps({"statements": [{"statement": statement, "parameters": parameters or {},
                                           "resultDataContents": ["row"]}]}, ensure_ascii=False).encode("utf-8")
    request = Request(f"{NEO4J_HTTP_URL}/db/neo4j/tx/commit", data=payload, headers={
        "Authorization": neo4j_auth(), "Content-Type": "application/json", "Accept": "application/json"})
    try:
        with urlopen(request, timeout=45) as response:
            result = json.load(response)
    except (HTTPError, URLError, TimeoutError, OSError) as exc:
        raise RuntimeError("Neo4j query unavailable") from exc
    if result.get("errors"):
        raise RuntimeError("Neo4j query failed: " + json.dumps(result["errors"], ensure_ascii=False))
    result_set = result.get("results", [{}])[0]
    columns = result_set.get("columns", [])
    return [dict(zip(columns, item.get("row", []))) for item in result_set.get("data", [])]


def model_payload() -> dict[str, Any]:
    try:
        payload = json.loads(MODEL_ANALYSIS_FILE.read_text(encoding="utf-8"))
        if not payload.get("passed"):
            raise ValueError("model analysis did not pass")
        return payload
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        raise HTTPException(status_code=503, detail="DL4J analysis artifact is unavailable or invalid.") from exc


def graph_posts(force: bool = False) -> list[dict[str, Any]]:
    global _graph_cache
    now = time.monotonic()
    if not force and _graph_cache and now - _graph_cache[0] < GRAPH_CACHE_TTL_SECONDS:
        return _graph_cache[1]
    statement = """
    MATCH (p:Post {datasetId:$dataset_id})
    OPTIONAL MATCH (p)-[:HAS_OBSERVATION]->(o:Observation {datasetId:$dataset_id})
    WITH p, [snapshot IN collect(o) WHERE snapshot IS NOT NULL | {
      observed_at_utc: toString(snapshot.observedAtUtc),
      observed_date: toString(date(snapshot.observedAtUtc + duration('PT7H'))),
      view_count: snapshot.viewCount, like_count: snapshot.likeCount,
      comment_count: snapshot.commentCount, share_count: snapshot.shareCount,
      save_count: snapshot.saveCount, caption: snapshot.caption
    }] AS observations
    RETURN p.postId AS post_id, p.authorUsername AS author_username,
      p.mediaType AS media_type,
      toString(date(p.publishedMinuteUtc + duration('PT7H'))) AS published_date,
      observations ORDER BY post_id
    """
    rows = cypher(statement, {"dataset_id": DATASET_ID})
    for row in rows:
        row["observations"] = sorted(row.get("observations") or [], key=lambda snap: snap["observed_at_utc"])
    _graph_cache = (now, rows)
    return rows


def average(values: list[int | float | None]) -> float | None:
    valid = [float(value) for value in values if value is not None]
    return sum(valid) / len(valid) if valid else None


def median(values: list[int | float | None]) -> float | None:
    valid = [float(value) for value in values if value is not None]
    return statistics.median(valid) if valid else None


def date_in_range(value: str, start: date, end: date) -> bool:
    parsed = date.fromisoformat(value)
    return start <= parsed <= end


def start_of_week(value: str) -> str:
    current = date.fromisoformat(value)
    return (current - timedelta(days=current.weekday())).isoformat()


def _linear_trend_forecast(series_by_cluster: dict[int, list[float]], horizon: int = 4) -> list[dict[int, float]]:
    """Damped linear projection of recent topic shares; proportions are renormalized each step."""
    forecasts: list[dict[int, float]] = []
    phi = 0.65
    for step in range(1, horizon + 1):
        raw: dict[int, float] = {}
        for cluster, series in series_by_cluster.items():
            recent = series[-8:]
            if not recent:
                raw[cluster] = 0.0
                continue
            mean_x = (len(recent) - 1) / 2
            mean_y = sum(recent) / len(recent)
            denom = sum((x - mean_x) ** 2 for x in range(len(recent)))
            slope = sum((x - mean_x) * (y - mean_y) for x, y in enumerate(recent)) / denom if denom else 0.0
            damped_steps = sum(phi ** offset for offset in range(1, step + 1))
            raw[cluster] = max(0.0, recent[-1] + slope * damped_steps)
        total = sum(raw.values())
        forecasts.append({cluster: value / total for cluster, value in raw.items()} if total else raw)
    return forecasts


def _flat_forecast(series_by_cluster: dict[int, list[float]], horizon: int = 4) -> list[dict[int, float]]:
    raw = {cluster: (sum(series[-4:]) / max(1, len(series[-4:]))) for cluster, series in series_by_cluster.items()}
    total = sum(raw.values())
    flat = {cluster: value / total for cluster, value in raw.items()} if total else raw
    return [dict(flat) for _ in range(horizon)]


def build_trend_analysis(posts: list[dict[str, Any]], assignments: dict[str, int],
                         clusters: list[dict[str, Any]], start: date, end: date,
                         horizon: int = 4) -> dict[str, Any]:
    """Forecast sampled caption-cluster share, with walk-forward comparison to a flat baseline."""
    selected = [p for p in posts if date_in_range(p["published_date"], start, end)]
    if not selected:
        return {"ready": False, "reason": "Không có bài trong khoảng ngày đã chọn."}
    by_week: dict[str, dict[int, list[dict[str, Any]]]] = defaultdict(lambda: defaultdict(list))
    for post in selected:
        week = start_of_week(post["published_date"])
        cluster = assignments.get(post["post_id"], -1)
        by_week[week][cluster].append(post)
    last_week = max(by_week)
    weeks = []
    cursor = date.fromisoformat(min(by_week))
    stop = date.fromisoformat(last_week)
    while cursor <= stop:
        weeks.append(cursor.isoformat())
        cursor += timedelta(days=7)
    group_ids = sorted(set(assignments.values()) | {-1})
    cluster_profiles = {int(item["cluster"]): item for item in clusters}
    shares: dict[int, list[float]] = {cluster: [] for cluster in group_ids}
    counts_by_week: dict[str, dict[int, int]] = {}
    for week in weeks:
        groups = by_week.get(week, {})
        total = sum(len(rows) for rows in groups.values())
        counts_by_week[week] = {cluster: len(groups.get(cluster, [])) for cluster in group_ids}
        for cluster in group_ids:
            shares[cluster].append(counts_by_week[week][cluster] / total if total else 0.0)

    # Exclude the last incomplete calendar week from training and signal comparisons.
    complete_n = len(weeks) - (1 if date.fromisoformat(last_week) + timedelta(days=6) > end else 0)
    complete_weeks = weeks[:complete_n]
    complete_shares = {cluster: values[:complete_n] for cluster, values in shares.items()}
    contiguous_start = max(0, complete_n - 1)
    while contiguous_start > 0 and sum(counts_by_week[complete_weeks[contiguous_start - 1]].values()) > 0:
        contiguous_start -= 1
    active_weeks = complete_weeks[contiguous_start:]
    active_shares = {cluster: values[contiguous_start:] for cluster, values in complete_shares.items()}
    if len(active_weeks) < 12:
        return {"ready": False, "reason": "Cần tối thiểu 12 tuần liên tục có bài để tạo và kiểm định dự báo.",
                "source_end": max(p["published_date"] for p in selected), "history_weeks": len(active_weeks)}

    recent_dates = active_weeks[-8:]
    recent_slice = {cluster: values[-8:] for cluster, values in active_shares.items()}
    projected = _linear_trend_forecast(recent_slice, horizon)
    flat = _flat_forecast(recent_slice, horizon)
    anchor = date.fromisoformat(active_weeks[-1])
    forecast_weeks = [(anchor + timedelta(days=7 * i)).isoformat() for i in range(1, horizon + 1)]

    # Rolling-origin four-week backtests. Do not claim skill without enough folds and baseline lift.
    folds = []
    n = len(active_weeks)
    first_origin = max(8, n - 28)
    for origin in range(first_origin, n - horizon + 1, horizon):
        train = {cluster: values[:origin] for cluster, values in active_shares.items()}
        pred = _linear_trend_forecast(train, horizon)
        baseline = _flat_forecast(train, horizon)
        model_errors, baseline_errors = [], []
        for step in range(horizon):
            actual_total = sum(counts_by_week[active_weeks[origin + step]].values())
            if not actual_total:
                continue
            actual = {cluster: counts_by_week[active_weeks[origin + step]][cluster] / actual_total for cluster in group_ids}
            model_errors.extend(abs(pred[step].get(cluster, 0.0) - actual.get(cluster, 0.0)) for cluster in group_ids)
            baseline_errors.extend(abs(baseline[step].get(cluster, 0.0) - actual.get(cluster, 0.0)) for cluster in group_ids)
        if model_errors:
            folds.append({"from": active_weeks[origin], "model_mae": sum(model_errors) / len(model_errors),
                          "baseline_mae": sum(baseline_errors) / len(baseline_errors)})
    model_mae = sum(x["model_mae"] for x in folds) / len(folds) if folds else None
    baseline_mae = sum(x["baseline_mae"] for x in folds) / len(folds) if folds else None
    backtest_status = "insufficient" if len(folds) < 3 else ("better_than_flat" if model_mae < baseline_mae else "not_better_than_flat")
    chosen_forecast = projected if backtest_status == "better_than_flat" else flat
    chosen_method = "damped trend" if backtest_status == "better_than_flat" else "flat-share baseline"

    momentum = []
    if len(active_weeks) >= 8:
        for cluster in group_ids:
            values = active_shares[cluster]
            recent = values[-4:]
            previous = values[-8:-4]
            recent_posts = [p for week in active_weeks[-4:] for p in by_week[week].get(cluster, [])]
            previous_posts = [p for week in active_weeks[-8:-4] for p in by_week[week].get(cluster, [])]
            recent_creators = {p.get("author_username") for p in recent_posts if p.get("author_username")}
            previous_creators = {p.get("author_username") for p in previous_posts if p.get("author_username")}
            trend_steps = sum(1 for a, b in zip(previous[-1:] + recent[:-1], recent) if b > a)
            forecast_share = sum(x.get(cluster, 0.0) for x in chosen_forecast) / horizon
            row = {"cluster": cluster, "share_previous": sum(previous) / 4,
                   "share_recent": sum(recent) / 4,
                   "share_change_pp": (sum(recent) - sum(previous)) / 4 * 100,
                   "posts_recent": len(recent_posts), "creators_recent": len(recent_creators),
                   "creators_previous": len(previous_creators), "weeks_share_up": trend_steps,
                   "forecast_share": forecast_share,
                   "label": "Chưa gán cụm" if cluster == -1 else f"Cụm {cluster}",
                   "caption_terms": [] if cluster == -1 else [x["term"] for x in cluster_profiles.get(cluster, {}).get("caption_keywords", [])[:4]]}
            momentum.append(row)
    momentum.sort(key=lambda x: (x["share_change_pp"], x["creators_recent"]), reverse=True)

    history = []
    for index, week in enumerate(active_weeks[-16:]):
        week_index = len(active_weeks) - min(16, len(active_weeks)) + index
        total = sum(counts_by_week[week].values())
        for cluster in group_ids:
            history.append({"period": week, "cluster": cluster,
                            "share": counts_by_week[week][cluster] / total if total else 0.0,
                            "posts": counts_by_week[week][cluster]})
    future = [{"period": week, "cluster": cluster, "share": prediction.get(cluster, 0.0)}
              for week, prediction in zip(forecast_weeks, chosen_forecast) for cluster in group_ids]
    forecast_mae = []
    for step, week in enumerate(forecast_weeks):
        for cluster in group_ids:
            forecast_mae.append({"period": week, "cluster": cluster,
                                 "trend": projected[step].get(cluster, 0.0),
                                 "flat": flat[step].get(cluster, 0.0)})
    sample_end = max((snap.get("observed_date") or p["published_date"])
                     for p in selected for snap in (p.get("observations") or [{}]))
    return {"ready": True, "source_end": sample_end, "anchor_week": active_weeks[-1],
            "forecast_end": forecast_weeks[-1], "horizon_weeks": horizon,
            "history_weeks": len(active_weeks), "signal_weeks": 4,
            "history": history, "forecast": future, "forecast_comparison": forecast_mae,
            "forecast_candidates": forecast_mae,
            "momentum": momentum, "backtest": {"folds": folds, "n_folds": len(folds),
                "trend_mae": model_mae, "baseline_mae": baseline_mae,
                "skill": (1 - model_mae / baseline_mae) if model_mae is not None and baseline_mae else None,
                "status": backtest_status},
            "method": "Damped linear projection on 8-week cluster share; compared with 4-week flat-share baseline",
            "method_used": chosen_method,
            "backtest_scope": "Exploratory conditional backtest: G05 cluster centroids were fit with the existing author-group split across the time range. This is not strict out-of-time validation of topic discovery.",
            "interpretation": "Projection of share of all sampled Posts assigned to each caption cluster; unembedded Posts are an explicit unassigned group. Not total TikTok interest."}


def aggregate_dashboard(start: date, end: date, media_type: str, posts: list[dict[str, Any]],
                       model: dict[str, Any]) -> dict[str, Any]:
    selected = [post for post in posts if date_in_range(post["published_date"], start, end)
                and (media_type == "all" or post["media_type"] == media_type)]
    assignments = {row["post_id"]: row["cluster"] for row in model["assignments"]}
    first_snapshot = {post["post_id"]: (post["observations"][0] if post["observations"] else {}) for post in selected}
    caption_posts = sum(bool((snapshot.get("caption") or "").strip()) for snapshot in first_snapshot.values())

    week_posts: Counter[str] = Counter()
    week_media: Counter[tuple[str, str]] = Counter()
    weekdays: Counter[int] = Counter()
    month_posts: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for post in selected:
        published = post["published_date"]
        week = start_of_week(published)
        week_posts[week] += 1
        week_media[(week, post["media_type"])] += 1
        weekdays[date.fromisoformat(published).isoweekday()] += 1
        month_posts[published[:7]].append(post)
    selected_observations = [snap for post in posts if (media_type == "all" or post["media_type"] == media_type)
                             for snap in post["observations"] if date_in_range(snap["observed_date"], start, end)]
    snapshots_by_day: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for snapshot in selected_observations:
        snapshots_by_day[snapshot["observed_date"]].append(snapshot)
    daily = [{"period": observed_day, "snapshots": len(observations),
              "mean_views": average([x.get("view_count") for x in observations]),
              "mean_likes": average([x.get("like_count") for x in observations]),
              "mean_comments": average([x.get("comment_count") for x in observations]),
              "mean_shares": average([x.get("share_count") for x in observations]),
              "mean_saves": average([x.get("save_count") for x in observations])}
             for observed_day, observations in sorted(snapshots_by_day.items())]
    latest_period: dict[str, dict[str, Any]] = {}
    for post in posts:
        if media_type != "all" and post["media_type"] != media_type:
            continue
        candidates = [snap for snap in post["observations"] if date_in_range(snap["observed_date"], start, end)]
        if candidates:
            latest_period[post["post_id"]] = candidates[-1]
    latest_values = list(latest_period.values())
    top_posts = []
    for post_id, snapshot in sorted(latest_period.items(), key=lambda pair: (pair[1].get("view_count") is not None,
                                                                              pair[1].get("view_count") or -1), reverse=True)[:10]:
        post = next(row for row in posts if row["post_id"] == post_id)
        top_posts.append({"post_id": post_id, "observed_at_utc": snapshot["observed_at_utc"],
                          "media_type": post["media_type"], "view_count": snapshot.get("view_count"),
                          "like_count": snapshot.get("like_count"), "author_username": post["author_username"]})

    coverage = []
    for month, month_rows in sorted(month_posts.items()):
        snapshots = [snap for post in month_rows for snap in post["observations"]]
        coverage.append({"period": month + "-01", "posts": len(month_rows),
                         "caption_posts": sum(bool((first_snapshot[x["post_id"]].get("caption") or "").strip()) for x in month_rows),
                         "observations": len(snapshots),
                         "view_observations": sum(x.get("view_count") is not None for x in snapshots),
                         "like_observations": sum(x.get("like_count") is not None for x in snapshots),
                         "comment_observations": sum(x.get("comment_count") is not None for x in snapshots),
                         "share_observations": sum(x.get("share_count") is not None for x in snapshots),
                         "save_observations": sum(x.get("save_count") is not None for x in snapshots)})

    cluster_posts: dict[int, list[dict[str, Any]]] = defaultdict(list)
    cluster_view_rates: dict[int, list[float]] = defaultdict(list)
    cluster_comment_rates: dict[int, list[float]] = defaultdict(list)
    cluster_medians: dict[int, list[int | None]] = defaultdict(list)
    for post in selected:
        cluster = assignments.get(post["post_id"])
        if cluster is None:
            continue
        cluster_posts[cluster].append(post)
        all_snapshots = post["observations"]
        if all_snapshots:
            last = all_snapshots[-1]
            cluster_medians[cluster].append(last.get("view_count"))
            views = last.get("view_count")
            if views and views > 0:
                if last.get("like_count") is not None:
                    cluster_view_rates[cluster].append(last["like_count"] / views)
                if last.get("comment_count") is not None:
                    cluster_comment_rates[cluster].append(last["comment_count"] / views)
    cluster_metrics = []
    for cluster in sorted(cluster_posts):
        cluster_metrics.append({"cluster": cluster, "posts": len(cluster_posts[cluster]),
                                "median_views": median(cluster_medians[cluster]),
                                "median_like_per_view": median(cluster_view_rates[cluster]),
                                "median_comment_per_view": median(cluster_comment_rates[cluster])})

    creator_rows: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for post in selected:
        creator = post.get("author_username") or "(unknown)"
        last = post["observations"][-1] if post["observations"] else {}
        views = last.get("view_count")
        cluster = assignments.get(post["post_id"])
        creator_rows[creator].append({"views": views,
                                      "like_rate": last["like_count"] / views if views and last.get("like_count") is not None else None,
                                      "cluster": cluster})
    creator_metrics = []
    for creator, rows in creator_rows.items():
        creator_metrics.append({"creator": creator, "posts": len(rows),
                                "clusters": len({x["cluster"] for x in rows if x["cluster"] is not None}),
                                "median_views": median([x["views"] for x in rows]),
                                "median_like_per_view": median([x["like_rate"] for x in rows])})
    creator_metrics.sort(key=lambda row: (-row["posts"], row["creator"]))

    trend = build_trend_analysis(posts, assignments, model["clusters"], start, end)
    if media_type != "all" and trend.get("ready"):
        # The user selected a media segment; rebuild the projection on that population only.
        media_posts = [p for p in posts if p["media_type"] == media_type]
        trend = build_trend_analysis(media_posts, assignments, model["clusters"], start, end)

    return {
        "filters": {"from": start.isoformat(), "to": end.isoformat(), "media_type": media_type},
        "kpis": {"posts": len(selected), "post_period_rows": len(selected),
                 "observation_count": len(selected_observations),
                 "observed_posts": len({snap_post["post_id"] for snap_post in posts
                                          if any(date_in_range(x["observed_date"], start, end) for x in snap_post["observations"])
                                          and (media_type == "all" or snap_post["media_type"] == media_type)}),
                 "mean_views": average([snap.get("view_count") for snap in selected_observations]),
                 "views_observed": sum(snap.get("view_count") is not None for snap in selected_observations),
                 "caption_coverage": caption_posts / len(selected) if selected else None},
        "posts_by_week": [{"period": key, "posts": count} for key, count in sorted(week_posts.items())],
        "posts_by_week_media": [{"period": key[0], "media_type": key[1], "posts": count}
                                for key, count in sorted(week_media.items())],
        "posts_by_weekday": [{"weekday": key, "posts": count} for key, count in sorted(weekdays.items())],
        "snapshots_by_day": daily,
        "latest_metrics": {"posts": len(latest_values), "mean_views": average([x.get("view_count") for x in latest_values]),
                           "median_views": median([x.get("view_count") for x in latest_values]),
                           "mean_likes": average([x.get("like_count") for x in latest_values]),
                           "mean_comments": average([x.get("comment_count") for x in latest_values]),
                           "mean_shares": average([x.get("share_count") for x in latest_values]),
                           "mean_saves": average([x.get("save_count") for x in latest_values])},
        "top_posts_latest_snapshot": top_posts, "coverage_by_month": coverage,
        "cluster_engagement": cluster_metrics, "creator_metrics": creator_metrics[:12],
        "future_trends": trend,
    }


@app.get("/")
def home() -> FileResponse:
    return FileResponse(APP_DIR / "static" / "index.html")


@app.get("/api/health")
def health() -> dict[str, str]:
    try:
        counts = cypher("MATCH (p:Post {datasetId:$id}) WITH count(p) AS posts "
                        "MATCH (o:Observation {datasetId:$id}) RETURN posts, count(o) AS observations", {"id": DATASET_ID})
        if not counts or counts[0]["posts"] == 0:
            raise RuntimeError("ViTIED graph is empty")
        return {"status": "ok", "source": "Neo4j ViTIED graph",
                "posts": str(counts[0]["posts"]), "observations": str(counts[0]["observations"])}
    except Exception as exc:
        raise HTTPException(status_code=503, detail="Neo4j ViTIED graph is unavailable.") from exc


@app.get("/api/model-analysis")
def model_analysis() -> dict[str, Any]:
    return model_payload()


@app.get("/api/meta")
def metadata() -> dict[str, Any]:
    try:
        posts = graph_posts()
        published = [post["published_date"] for post in posts]
        observed = [snap["observed_date"] for post in posts for snap in post["observations"]]
        dates = published + observed
        return {"publication": {"publication_id": "vitied-g04-v2 · Observation graph",
                                 "topic_label_status": "unavailable_no_reviewed_post_labels",
                                 "timezone": TIMEZONE},
                "period": {"min_date": min(dates), "max_date": max(dates)},
                "media_types": sorted({post["media_type"] for post in posts})}
    except Exception as exc:
        raise HTTPException(status_code=503, detail="Không đọc được dữ liệu ViTIED từ Neo4j.") from exc


@app.get("/api/dashboard")
def dashboard(start: date = Query(alias="from"), end: date = Query(alias="to"),
              media_type: str = Query(default="all", pattern="^(all|video|photo)$")) -> dict[str, Any]:
    if start > end:
        raise HTTPException(status_code=422, detail="Ngày bắt đầu phải trước hoặc bằng ngày kết thúc.")
    try:
        return aggregate_dashboard(start, end, media_type, graph_posts(), model_payload())
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=503, detail="Không truy vấn được graph dashboard từ Neo4j.") from exc


@app.get("/api/graph-analysis")
def graph_analysis(start: date = Query(alias="from"), end: date = Query(alias="to"),
                   rank_by: str = Query(default="articlerank", pattern="^(articlerank|closeness)$"),
                   display_limit: int = Query(default=70, ge=30, le=120)) -> dict[str, Any]:
    """Score the global hashtag co-occurrence graph; return a readable central subgraph."""
    if start > end:
        raise HTTPException(status_code=422, detail="Ngày bắt đầu phải trước hoặc bằng ngày kết thúc.")
    cache_key = f"{start.isoformat()}|{end.isoformat()}"
    cached = _link_analysis_cache.get(cache_key)
    if cached and time.monotonic() - cached[0] < 600:
        analysis = cached[1]
    else:
        params = {"dataset_id": DATASET_ID, "from": start.isoformat(), "to": end.isoformat()}
        try:
            total_rows = cypher("""
            MATCH (p:Post {datasetId:$dataset_id})
            WHERE date(p.publishedMinuteUtc + duration('PT7H')) >= date($from)
              AND date(p.publishedMinuteUtc + duration('PT7H')) <= date($to)
            RETURN count(p) AS posts
            """, params)
            total_posts = int(total_rows[0]["posts"]) if total_rows else 0
            if total_posts == 0:
                return {"period": {"from": start.isoformat(), "to": end.isoformat()},
                        "summary": {"posts": 0, "hashtags": 0, "candidate_pairs": 0,
                                    "scored_pairs": 0, "suppressed_pairs": 0},
                        "nodes": [], "edges": [], "rankings": [], "closeness_rankings": []}
            frequency_rows = cypher("""
            MATCH (h:Hashtag {datasetId:$dataset_id})<-[:HAS_HASHTAG]-(p:Post {datasetId:$dataset_id})
            WHERE date(p.publishedMinuteUtc + duration('PT7H')) >= date($from)
              AND date(p.publishedMinuteUtc + duration('PT7H')) <= date($to)
            RETURN h.tag AS tag, count(DISTINCT p) AS posts
            """, params)
            post_counts = {str(row["tag"]): int(row["posts"]) for row in frequency_rows if row.get("tag")}
            pair_rows = cypher("""
            MATCH (p:Post {datasetId:$dataset_id})-[:HAS_HASHTAG]->(a:Hashtag {datasetId:$dataset_id}),
                  (p)-[:HAS_HASHTAG]->(b:Hashtag {datasetId:$dataset_id})
            WHERE a.tagKey < b.tagKey
              AND date(p.publishedMinuteUtc + duration('PT7H')) >= date($from)
              AND date(p.publishedMinuteUtc + duration('PT7H')) <= date($to)
            WITH a.tag AS source, b.tag AS target,
                 count(DISTINCT p) AS shared_posts,
                 count(DISTINCT p.authorUsername) AS shared_creators
            RETURN source, target, shared_posts, shared_creators
            """, params)
            analysis = score_hashtag_network(post_counts, pair_rows, total_posts)
            analysis["total_posts"] = total_posts
            _link_analysis_cache[cache_key] = (time.monotonic(), analysis)
            if len(_link_analysis_cache) > 3:
                oldest = min(_link_analysis_cache, key=lambda key: _link_analysis_cache[key][0])
                _link_analysis_cache.pop(oldest, None)
        except Exception as exc:
            raise HTTPException(status_code=503, detail="Không tính được link analysis trên graph Neo4j.") from exc

    metric = "articlerank" if rank_by == "articlerank" else "closeness"
    linked_rankings = [row for row in analysis["rankings"] if row["links"] > 0]
    ordered = sorted(linked_rankings, key=lambda row: (row[metric], row["strength"]), reverse=True)
    closeness_rankings = sorted((row for row in linked_rankings if row["closeness"] > 0),
                     key=lambda row: (row["closeness"], row["articlerank"]), reverse=True)
    displayed = ordered[:display_limit]
    displayed_tags = {row["tag"] for row in displayed}
    selected_edges = [edge for edge in analysis["edges"]
                      if edge["source"] in displayed_tags and edge["target"] in displayed_tags]
    selected_edges.sort(key=lambda row: row["weight"], reverse=True)
    selected_edges = selected_edges[:240]
    node_ids = {tag: f"tag:{index}" for index, tag in enumerate(sorted(displayed_tags))}
    nodes = [{"id": node_ids[row["tag"]], "tag": row["tag"], "label": "#" + row["tag"],
              "posts": row["posts"], "links": row["links"], "strength": row["strength"],
              "articlerank": row["articlerank"], "closeness": row["closeness"],
              "articlerank_baseline": 0.15 / max(1, len(analysis["rankings"])),
              "score": row[metric]} for row in displayed]
    edges = [{"source": node_ids[row["source"]], "target": node_ids[row["target"]],
              "source_tag": row["source"], "target_tag": row["target"],
              "shared_posts": row["shared_posts"], "shared_creators": row["shared_creators"],
              "lift": row["lift"], "weight": row["weight"]} for row in selected_edges]
    top_rows = sorted(linked_rankings, key=lambda row: (row["articlerank"], row["strength"]), reverse=True)[:20]
    top_closeness_rankings = closeness_rankings[:20]
    return {"period": {"from": start.isoformat(), "to": end.isoformat()}, "rank_by": rank_by,
            "nodes": nodes, "edges": edges, "rankings": top_rows, "closeness_rankings": top_closeness_rankings,
            "summary": {"posts": analysis["total_posts"], "hashtags": len(analysis["rankings"]),
                        "linked_hashtags": sum(row["links"] > 0 for row in analysis["rankings"]),
                        "candidate_pairs": analysis["candidate_pairs"],
                        "scored_pairs": analysis["scored_pairs"],
                        "suppressed_pairs": analysis["suppressed_pairs"],
                        "displayed_nodes": len(nodes), "displayed_edges": len(edges),
                        "min_shared_posts": analysis["min_shared_posts"],
                        "min_shared_creators": analysis["min_shared_creators"],
                        "generic_tags_suppressed": analysis["generic_tags_suppressed"],
                        "max_tag_prevalence": analysis["max_tag_prevalence"],
                        "articlerank_leader": top_rows[0]["tag"] if top_rows else None,
                        "closeness_leader": top_closeness_rankings[0]["tag"] if top_closeness_rankings else None}}
