"""G01 intake for the immutable ViTIED CSV exports.

Produces a source-backed identity manifest, row pointers for rejected records,
and reconciliation metrics. This script never edits raw CSVs or loads Silver.
"""

from __future__ import annotations

import argparse
import ast
import collections
import csv
import hashlib
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import unquote, urlparse


SOURCE_FILES = ("training_data.csv", "streaming_data.csv")
REQUIRED_COLUMNS = {
    "user_name", "vid_id", "vid_url", "vid_postTime", "vid_scrapeTime",
    "vid_caption", "vid_hashtags", "music_id", "topic",
    "vid_nview", "vid_nlike", "vid_ncomment", "vid_nshare", "vid_nsave",
}
MEDIA_PATH = re.compile(r"^/@([^/]+)/(video|photo)/(\d{18,20})/?$")
RAW_ID = re.compile(r"^\d{18,20}$")
ALLOWED_CATEGORIES = {
    "beauty", "fashion", "food", "health", "mom_and_baby", "tech_and_household"
}
METRICS = ("vid_nview", "vid_nlike", "vid_ncomment", "vid_nshare", "vid_nsave")
METRIC_TEXT = re.compile(r"^\d+(?:[.,]\d+)?\s*[KMBkmb]?$", re.ASCII)
SCHEMA_VERSION = "vitied-g01-v1"


def media_from_url(raw_url: str) -> tuple[str | None, str | None, str | None]:
    parsed = urlparse(raw_url.strip())
    host = (parsed.hostname or "").casefold()
    if parsed.scheme not in {"https", "http"} or not (host == "tiktok.com" or host.endswith(".tiktok.com")):
        return None, None, None
    match = MEDIA_PATH.fullmatch(unquote(parsed.path))
    if not match:
        return None, None, None
    return match.group(3), match.group(2), match.group(1).casefold()


def utc_timestamp(value: str) -> str | None:
    try:
        timestamp = datetime.fromisoformat(value.strip())
        if timestamp.tzinfo is None:
            return None
        return timestamp.astimezone(timezone.utc).isoformat()
    except ValueError:
        return None


def categories_from_source(value: str) -> tuple[list[str] | None, str | None]:
    raw = value.strip()
    if not raw or raw.casefold() in {"[nan]", "nan", "null"}:
        return None, None
    try:
        values = ast.literal_eval(raw)
    except (SyntaxError, ValueError):
        return None, "invalid_category_syntax"
    if not isinstance(values, list) or not all(isinstance(item, str) for item in values):
        return None, "invalid_category_type"
    normalized = sorted({item.strip().casefold() for item in values if item.strip()})
    if not set(normalized) <= ALLOWED_CATEGORIES:
        return None, "unknown_category"
    return normalized or None, None


def source_rows(root: Path, manifest: dict) -> tuple[list[dict], list[dict]]:
    source = root / "data/raw/vitied"
    specs = {item["local_path"]: item for item in manifest["files"]}
    rows: list[dict] = []
    files: list[dict] = []
    for filename in SOURCE_FILES:
        path = source / filename
        relative = f"data/raw/vitied/{filename}"
        expected = specs.get(relative)
        if expected is None or not path.is_file():
            raise FileNotFoundError(f"Missing source or manifest entry: {relative}")
        digest = hashlib.sha256()
        with path.open("rb") as handle:
            while block := handle.read(1024 * 1024):
                digest.update(block)
        actual_hash = digest.hexdigest()
        if actual_hash != expected["sha256"] or path.stat().st_size != expected["bytes"]:
            raise ValueError(f"Source checksum/size changed: {relative}")
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            columns = reader.fieldnames or []
            if len(columns) != len(set(columns)) or not REQUIRED_COLUMNS <= set(columns):
                raise ValueError(f"Schema missing/duplicate columns: {relative}")
            count = 0
            for count, row in enumerate(reader, start=1):
                rows.append({"source_file": filename, "source_row": count + 1, "values": row})
            if count != expected["rows"]:
                raise ValueError(f"Source row count changed: {relative}")
        files.append({"source_file": filename, "rows": count, "bytes": path.stat().st_size,
                      "sha256": actual_hash, "columns": columns})
    return rows, files


def row_fingerprint(row: dict) -> str:
    # Derived training-only columns and raw rounded ID do not define an observation.
    fields = {key: row.get(key, "") for key in (
        "user_name", "vid_caption", "vid_postTime", "vid_scrapeTime", "vid_duration",
        "vid_hashtags", "music_id", "music_title", "music_authorName", "topic", *METRICS
    )}
    return hashlib.sha256(json.dumps(fields, sort_keys=True, ensure_ascii=False).encode("utf-8")).hexdigest()


def analyze(rows: list[dict], files: list[dict]) -> tuple[dict, list[dict], list[dict]]:
    counts = collections.Counter()
    by_file = collections.defaultdict(collections.Counter)
    url_by_raw = collections.defaultdict(set)
    raw_by_url = collections.defaultdict(set)
    authors_by_url = collections.defaultdict(set)
    types_by_url = collections.defaultdict(set)
    author_categories = collections.defaultdict(set)
    category_mentions = collections.Counter()
    missing_by_file = collections.defaultdict(collections.Counter)
    metric_formats = collections.defaultdict(collections.Counter)
    id_deltas = []
    validated: list[dict] = []
    rejected: list[dict] = []

    for item in rows:
        row = item["values"]
        filename = item["source_file"]
        tally = by_file[filename]
        counts["source_rows"] += 1
        tally["rows"] += 1
        reasons: list[str] = []
        if None in row or any(value is None for value in row.values()):
            reasons.append("malformed_width")
        for field, value in row.items():
            if field is not None and (value is None or not value.strip() or value.strip().casefold() in {"nan", "[nan]"}):
                missing_by_file[filename][field] += 1
        for field in METRICS:
            value = (row.get(field) or "").strip()
            form = "missing" if not value or value.casefold() == "nan" else (
                "numeric_text" if METRIC_TEXT.fullmatch(value) else (
                    "share_placeholder" if field == "vid_nshare" and value.casefold() == "share" else "unparsed_text"))
            metric_formats[field][form] += 1
        raw = (row.get("vid_id") or "").strip()
        url_id, kind, url_user = media_from_url(row.get("vid_url") or "")
        author = (row.get("user_name") or "").strip().lstrip("@").casefold()
        published = utc_timestamp(row.get("vid_postTime") or "")
        observed = utc_timestamp(row.get("vid_scrapeTime") or "")
        if not url_id:
            reasons.append("no_trusted_media_url_id")
        if raw and not RAW_ID.fullmatch(raw):
            reasons.append("invalid_raw_id_format")
        if not published:
            reasons.append("invalid_published_at")
        if not observed:
            reasons.append("invalid_observed_at")
        if published and observed and published > observed:
            reasons.append("published_after_observed")
        if not author:
            reasons.append("missing_author_username")
        if author and url_user and author != url_user:
            tally["author_url_handle_mismatch"] += 1
            reasons.append("author_url_handle_mismatch")
        categories, category_error = categories_from_source(row.get("topic") or "")
        if category_error:
            tally[category_error] += 1
        elif categories and author:
            author_categories[author].add(tuple(categories))
            category_mentions.update(categories)
        else:
            tally["unknown_creator_category_rows"] += 1
        if not (row.get("vid_caption") or "").strip():
            tally["missing_caption"] += 1
        if not (row.get("vid_hashtags") or "").strip():
            tally["missing_hashtags"] += 1
        music_id = (row.get("music_id") or "").strip()
        if music_id and not RAW_ID.fullmatch(music_id):
            tally["unverified_music_id_rows"] += 1
        if url_id:
            tally[f"url_{kind}_rows"] += 1
            if raw:
                url_by_raw[raw].add(url_id)
                raw_by_url[url_id].add(raw)
                if raw != url_id:
                    tally["raw_url_id_mismatch_rows"] += 1
                    if raw.isdigit():
                        id_deltas.append(abs(int(raw) - int(url_id)))
            authors_by_url[url_id].add(author)
            types_by_url[url_id].add(kind)
        if reasons:
            rejected.append({"source_file": filename, "source_row": item["source_row"],
                             "raw_vid_id": raw, "url_media_id": url_id or "", "reason": "|".join(reasons)})
            for reason in reasons:
                tally[reason] += 1
            continue
        validated.append({"source_file": filename, "source_row": item["source_row"],
                          "raw_vid_id": raw, "canonical_id": url_id, "media_type": kind,
                          "author": author, "published_at_utc": published,
                          "observed_at_utc": observed, "categories": categories,
                          "fingerprint": row_fingerprint(row)})

    ambiguous_raw = {key for key, values in url_by_raw.items() if len(values) > 1}
    ambiguous_url = {key for key, values in raw_by_url.items() if len(values) > 1}
    ambiguous_author = {key for key, values in authors_by_url.items() if len(values) > 1}
    ambiguous_type = {key for key, values in types_by_url.items() if len(values) > 1}
    identity_rows = []
    observations: dict[tuple[str, str], str] = {}
    accepted: list[dict] = []
    for item in validated:
        causes = []
        if item["raw_vid_id"] in ambiguous_raw:
            causes.append("raw_id_maps_to_multiple_url_ids")
        if item["canonical_id"] in ambiguous_url:
            causes.append("url_id_maps_to_multiple_raw_ids")
        if item["canonical_id"] in ambiguous_author:
            causes.append("url_id_multiple_authors")
        if item["canonical_id"] in ambiguous_type:
            causes.append("url_id_multiple_media_types")
        if causes:
            rejected.append({"source_file": item["source_file"], "source_row": item["source_row"],
                             "raw_vid_id": item["raw_vid_id"], "url_media_id": item["canonical_id"],
                             "reason": "|".join(causes)})
            by_file[item["source_file"]]["identity_conflict_rows"] += 1
            continue
        key = item["canonical_id"], item["observed_at_utc"]
        prior = observations.get(key)
        if prior is not None:
            reason = "duplicate_observation" if prior == item["fingerprint"] else "conflicting_observation"
            rejected.append({"source_file": item["source_file"], "source_row": item["source_row"],
                             "raw_vid_id": item["raw_vid_id"], "url_media_id": item["canonical_id"],
                             "reason": reason})
            by_file[item["source_file"]][reason + "_rows"] += 1
            continue
        observations[key] = item["fingerprint"]
        accepted.append(item)

    by_post = collections.defaultdict(list)
    for item in accepted:
        by_post[item["canonical_id"]].append(item)
    for canonical_id, items in sorted(by_post.items()):
        first = items[0]
        identity_rows.append({"canonical_post_id": canonical_id, "media_type": first["media_type"],
                              "raw_vid_id": first["raw_vid_id"], "author_username": first["author"],
                              "id_resolution_status": "url_matches_raw" if first["raw_vid_id"] == canonical_id else "url_selected_raw_mismatch",
                              "source_files": "|".join(sorted({i["source_file"] for i in items})),
                              "observation_count": len(items),
                              "published_minute_utc": first["published_at_utc"][:16] + ":00+00:00",
                              "published_second_variants": len({i["published_at_utc"] for i in items})})

    posts_with_multiple_publish_times = sum(
        len({item["published_at_utc"] for item in items}) > 1 for items in by_post.values()
    )
    posts_with_conflicting_publish_minutes = sum(
        len({item["published_at_utc"][:16] for item in items}) > 1 for items in by_post.values()
    )

    counts.update({"accepted_observation_rows": len(accepted), "quarantine_rows": len(rejected),
                   "canonical_posts": len(by_post),
                   "canonical_video_posts": sum(items[0]["media_type"] == "video" for items in by_post.values()),
                   "canonical_photo_posts": sum(items[0]["media_type"] == "photo" for items in by_post.values()),
                   "unique_authors": len({item["author"] for item in accepted}),
                   "authors_with_multiple_category_sets": sum(len(values) > 1 for values in author_categories.values()),
                   "posts_with_multiple_publish_times": posts_with_multiple_publish_times,
                   "posts_with_conflicting_publish_minutes": posts_with_conflicting_publish_minutes})
    if counts["source_rows"] != counts["accepted_observation_rows"] + counts["quarantine_rows"]:
        raise AssertionError("G01 row accounting failed")
    report = {"schema_version": SCHEMA_VERSION, "generated_at_utc": datetime.now(timezone.utc).isoformat(),
              "source_files": files, "counts": dict(sorted(counts.items())),
              "by_file": {name: dict(sorted(tally.items())) for name, tally in sorted(by_file.items())},
              "missing_by_file": {name: dict(sorted(tally.items())) for name, tally in sorted(missing_by_file.items())},
              "metric_text_formats": {name: dict(sorted(tally.items())) for name, tally in sorted(metric_formats.items())},
              "creator_category_row_mentions": dict(sorted(category_mentions.items())),
              "published_at_utc_range": [min((item["published_at_utc"] for item in accepted), default=None),
                                         max((item["published_at_utc"] for item in accepted), default=None)],
              "observed_at_utc_range": [min((item["observed_at_utc"] for item in accepted), default=None),
                                        max((item["observed_at_utc"] for item in accepted), default=None)],
              "identity": {"raw_ids_mapping_to_multiple_url_ids": len(ambiguous_raw),
                           "url_ids_mapping_to_multiple_raw_ids": len(ambiguous_url),
                           "url_ids_with_multiple_authors": len(ambiguous_author),
                           "url_ids_with_multiple_media_types": len(ambiguous_type),
                           "raw_url_mismatch_absolute_delta_range": [min(id_deltas) if id_deltas else None,
                                                                      max(id_deltas) if id_deltas else None],
                           "canonical_policy": "Validated TikTok /video/ or /photo/ URL ID; one-to-one stable raw-to-URL mapping and author/media type. Preserve raw ID."},
              "category_policy": "Source topic is a creator category, never reviewed post topic; six casefolded values, unknown remains null.",
              "time_policy": "Preserve raw posting timestamps on observations. Canonical post time has minute precision because some source snapshots vary in seconds; any conflict across minutes requires review.",
              "grain": {"identity_csv": "one canonical TikTok post", "quarantine_csv": "one source CSV row",
                        "accepted_observation": "canonical_post_id + observed_at_utc"}}
    return report, identity_rows, rejected


def atomic_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def atomic_csv(path: Path, rows: list[dict], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)
    os.replace(temporary, path)


def main(root: Path) -> dict:
    manifest_path = root / "data/raw/manifests/vitied_source_20261013.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    rows, files = source_rows(root, manifest)
    report, identities, rejected = analyze(rows, files)
    atomic_json(root / "outputs/quality/vitied_g01.json", report)
    atomic_csv(root / "outputs/quality/vitied_id_resolution.csv", identities,
               ["canonical_post_id", "media_type", "raw_vid_id", "author_username",
                "id_resolution_status", "source_files", "observation_count",
                "published_minute_utc", "published_second_variants"])
    atomic_csv(root / "data/quarantine/vitied_g01_pointers.csv", rejected,
               ["source_file", "source_row", "raw_vid_id", "url_media_id", "reason"])
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    print(json.dumps(main(parser.parse_args().root), ensure_ascii=False, indent=2))
