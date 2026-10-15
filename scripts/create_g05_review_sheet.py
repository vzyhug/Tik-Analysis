"""Create an unreviewed, editable Post-level topic review sheet."""

from __future__ import annotations

import argparse
import csv
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
OUTPUT_COLUMNS = ["model_version", "cluster", "sample_type", "post_id", "caption_first_snapshot",
                  "hashtags_first_snapshot", "source_file", "source_row", "fits_cluster",
                  "reviewed_topic_ids", "reviewer", "reviewed_at_utc", "notes"]


def run(source: Path, output: Path, model_version: str) -> int:
    if output.exists():
        raise FileExistsError(f"Review sheet already exists; do not overwrite manual work: {output}")
    with source.open(encoding="utf-8-sig", newline="") as stream:
        rows = list(csv.DictReader(stream))
    if not rows or len({row["post_id"] for row in rows}) != len(rows):
        raise ValueError("Review sample has no rows or duplicated Post IDs")
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", encoding="utf-8-sig", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=OUTPUT_COLUMNS)
        writer.writeheader()
        for row in rows:
            writer.writerow({**{name: row.get(name, "") for name in OUTPUT_COLUMNS},
                             "model_version": model_version,
                             "fits_cluster": "", "reviewed_topic_ids": "", "reviewer": "",
                             "reviewed_at_utc": "", "notes": ""})
    return len(rows)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--output", type=Path, default=ROOT / "data/labels/vitied_g05_review.csv")
    parser.add_argument("--model-version", default="vitied-g05-ae64-v2")
    args = parser.parse_args()
    print(f"unreviewed_rows={run(args.source, args.output, args.model_version)}")
