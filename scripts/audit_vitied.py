"""Read-only source audit for the public ViTIED CSV exports.

This does not infer or repair TikTok IDs. It records source-level evidence before
the ViTIED adapter, Silver tables, or graph loader are allowed to consume data.
"""

from __future__ import annotations

import argparse
import collections
import csv
import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path


FILES = ("training_data.csv", "streaming_data.csv")
REQUIRED = {"vid_id", "vid_url", "vid_postTime", "vid_scrapeTime", "topic"}
URL_ID = re.compile(r"/(video|photo)/(\d+)(?:[/?#]|$)")


def audit_file(path: Path) -> dict:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    with path.open("r", encoding="utf-8-sig", newline="") as source:
        reader = csv.DictReader(source)
        columns = reader.fieldnames or []
        if len(columns) != len(set(columns)) or not REQUIRED <= set(columns):
            raise ValueError(f"Invalid/duplicate columns in {path.name}")
        counts = collections.Counter()
        topics = collections.Counter()
        videos: set[str] = set()
        authors: set[str] = set()
        observations: dict[tuple[str, str], str] = {}
        urls_by_raw_id = collections.defaultdict(set)
        raw_ids_by_url_id = collections.defaultdict(set)
        authors_by_raw_id = collections.defaultdict(set)
        for row in reader:
            counts["rows"] += 1
            if None in row or any(value is None for value in row.values()):
                counts["malformed_width"] += 1
                continue
            video_id = row["vid_id"].strip()
            author = row.get("user_name", "").strip()
            observed = row["vid_scrapeTime"].strip()
            if video_id:
                videos.add(video_id)
            else:
                counts["missing_video_id"] += 1
            if author:
                authors.add(author.casefold())
                if video_id:
                    authors_by_raw_id[video_id].add(author.casefold())
            if "vid_caption" in row and not row["vid_caption"].strip():
                counts["missing_caption"] += 1
            if not row.get("vid_hashtags", "").strip():
                counts["missing_hashtags"] += 1
            if not observed:
                counts["missing_observed_at"] += 1
            else:
                try:
                    parsed = datetime.fromisoformat(observed)
                    if parsed.tzinfo is None:
                        counts["naive_observed_at"] += 1
                except ValueError:
                    counts["invalid_observed_at"] += 1
            raw_topic = row["topic"].strip()
            topics[raw_topic or "<missing>"] += 1
            match = URL_ID.search(row["vid_url"])
            if not match:
                counts["missing_url_media_id"] += 1
            elif video_id:
                counts[f"url_{match.group(1)}_rows"] += 1
                urls_by_raw_id[video_id].add(match.group(2))
                raw_ids_by_url_id[match.group(2)].add(video_id)
                if video_id != match.group(2):
                    counts["media_id_url_mismatch"] += 1
            if video_id and ("e+" in video_id.lower() or "." in video_id):
                counts["non_integer_video_id_text"] += 1
            music_id = row.get("music_id", "").strip()
            if music_id and ("e+" in music_id.lower() or "." in music_id):
                counts["non_integer_music_id_text"] += 1
            if video_id and observed:
                key = video_id, observed
                fingerprint = hashlib.sha256(json.dumps(row, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()
                previous = observations.get(key)
                if previous is None:
                    observations[key] = fingerprint
                elif previous != fingerprint:
                    counts["conflicting_video_observation"] += 1
                else:
                    counts["duplicate_video_observation"] += 1
        return {
            "source_file": path.name,
            "bytes": path.stat().st_size,
            "sha256": digest.hexdigest(),
            "columns": columns,
            "row_count": counts["rows"],
            "distinct_raw_video_ids": len(videos),
            "distinct_usernames": len(authors),
            "distinct_raw_video_observation_keys": len(observations),
            "raw_ids_with_multiple_url_ids": sum(len(values) > 1 for values in urls_by_raw_id.values()),
            "url_ids_with_multiple_raw_ids": sum(len(values) > 1 for values in raw_ids_by_url_id.values()),
            "raw_ids_with_multiple_authors": sum(len(values) > 1 for values in authors_by_raw_id.values()),
            "issues": {key: value for key, value in sorted(counts.items()) if key != "rows"},
            "raw_topic_values": dict(sorted(topics.items())),
        }


def compare_sources(paths: list[Path]) -> dict:
    by_file = {}
    for path in paths:
        raw_ids, url_ids, author_topics = set(), set(), collections.defaultdict(set)
        with path.open("r", encoding="utf-8-sig", newline="") as source:
            for row in csv.DictReader(source):
                if None in row or any(value is None for value in row.values()):
                    continue
                raw_id = row["vid_id"].strip()
                if raw_id:
                    raw_ids.add(raw_id)
                match = URL_ID.search(row["vid_url"])
                if match:
                    url_ids.add(match.group(2))
                author = row.get("user_name", "").strip().casefold()
                topic = row["topic"].strip().casefold()
                if author and topic and topic != "[nan]":
                    author_topics[author].add(topic)
        by_file[path.name] = {
            "raw_ids": raw_ids,
            "url_ids": url_ids,
            "authors_with_multiple_raw_topic_values": sum(len(values) > 1 for values in author_topics.values()),
            "authors_with_nonempty_topic": len(author_topics),
        }
    result = {name: {key: value for key, value in item.items() if not isinstance(value, set)}
              for name, item in by_file.items()}
    if len(paths) == 2:
        a, b = (by_file[path.name] for path in paths)
        result["between_files"] = {
            "raw_video_id_overlap": len(a["raw_ids"] & b["raw_ids"]),
            "url_media_id_overlap": len(a["url_ids"] & b["url_ids"]),
            "raw_video_id_union": len(a["raw_ids"] | b["raw_ids"]),
            "url_media_id_union": len(a["url_ids"] | b["url_ids"]),
        }
    return result


def main(root: Path) -> dict:
    source = root / "data/raw/vitied"
    files = [source / name for name in FILES]
    present = [path for path in files if path.is_file()]
    if not present:
        raise FileNotFoundError("No ViTIED CSV has been downloaded")
    report = {
        "audited_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_repository": "https://github.com/dthn-anna/ViTIED",
        "audited_files": [audit_file(path) for path in present],
        "cross_file": compare_sources(present),
        "missing_files": [path.name for path in files if not path.is_file()],
        "scope": "Source audit only; no ID repair, topic validation, Silver load, or model training",
    }
    destination = root / "outputs/quality/vitied_source_audit.json"
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    print(json.dumps(main(parser.parse_args().root), ensure_ascii=False, indent=2))
