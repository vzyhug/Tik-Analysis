"""Create a deterministic manual-review queue without assigning Topic labels."""

from __future__ import annotations

from collections import defaultdict
import csv
from datetime import datetime, timezone
from hashlib import sha256
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PACKS = ROOT / "data/raw/apify"
REFERENCE = ROOT / "data/reference/kol.csv"
OUTPUT = ROOT / "data/labels/review_queue.csv"
REPORT = ROOT / "outputs/quality/g05_review_queue.json"
SAMPLE_PER_CATEGORY = 4
SEED = "g05-manual-review-v1"


def username_key(value: str) -> str:
    return value.strip().lstrip("@").casefold()


def main() -> None:
    categories = defaultdict(set)
    with REFERENCE.open("r", encoding="utf-8-sig", newline="") as stream:
        for row in csv.DictReader(stream):
            categories[username_key(row["username"])].add(row["category_id"])
    candidates = defaultdict(list)
    for path in sorted(PACKS.glob("*.json")):
        for record in json.loads(path.read_text(encoding="utf-8-sig")):
            if not isinstance(record, dict) or record.get("id") is None:
                continue
            text = record.get("text") or ""
            tags = [item.get("name") for item in (record.get("hashtags") or [])
                    if isinstance(item, dict) and item.get("name")]
            if not text.strip() and not tags:
                continue
            video_id = str(record["id"])
            input_username = username_key(record.get("input") or "")
            published = record.get("createTime")
            published_utc = (datetime.fromtimestamp(published, timezone.utc).isoformat()
                             if isinstance(published, (int, float)) and not isinstance(published, bool) else "")
            row = {"video_id": video_id, "author_username": (record.get("authorMeta") or {}).get("name") or "",
                   "input_username": input_username, "published_at_utc": published_utc,
                   "text": text, "hashtags": " | ".join(tags), "source_file": path.name,
                   "category_ids_reference": " | ".join(sorted(categories[input_username])),
                   "review_status": "unreviewed", "label_ids": "", "reviewer": "",
                   "reviewed_at_utc": "", "review_note": ""}
            for category_id in categories[input_username]:
                candidates[category_id].append(row)
    chosen = {}
    per_category = {}
    for category_id in sorted(set().union(*categories.values())):
        ordered = sorted(candidates[category_id], key=lambda row: sha256(
            f'{SEED}:{category_id}:{row["video_id"]}'.encode()).hexdigest())
        selected = 0
        for row in ordered:
            if row["video_id"] in chosen:
                continue
            chosen[row["video_id"]] = row
            selected += 1
            if selected == SAMPLE_PER_CATEGORY:
                break
        per_category[category_id] = {"eligible_videos": len(ordered), "selected": selected}
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    if OUTPUT.exists():
        with OUTPUT.open("r", encoding="utf-8-sig", newline="") as stream:
            previous = list(csv.DictReader(stream))
        if any(row.get("review_status") != "unreviewed" or row.get("label_ids") or
               row.get("reviewer") or row.get("reviewed_at_utc") or row.get("review_note")
               for row in previous):
            raise ValueError(f"Review queue contains human work; refusing overwrite: {OUTPUT}")
    fields = ["video_id", "author_username", "input_username", "published_at_utc", "text",
              "hashtags", "source_file", "category_ids_reference", "review_status", "label_ids",
              "reviewer", "reviewed_at_utc", "review_note"]
    with OUTPUT.open("w", encoding="utf-8-sig", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        writer.writerows(chosen.values())
    result = {"schema_version": "g05-review-queue-v1", "seed": SEED,
              "sample_per_category": SAMPLE_PER_CATEGORY, "videos_to_review": len(chosen),
              "reference_category_coverage": per_category,
              "label_policy": "All selected videos are unreviewed; KOL categories are sampling strata only"}
    REPORT.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"videos_to_review": len(chosen), "categories": len(per_category)}, sort_keys=True))


if __name__ == "__main__":
    main()
