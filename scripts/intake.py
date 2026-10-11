"""G01: inventory immutable Apify exports using only the Python standard library."""

from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from datetime import datetime, timezone
from hashlib import sha256
import json
from pathlib import Path
import re
import shutil
import sys
import tempfile
import unicodedata


ROOT = Path(__file__).resolve().parents[1]
PACK_RE = re.compile(r"PACK([1-8])(?=\.|$)", re.IGNORECASE)
FIELDS = (
    "text", "createTime", "createTimeISO", "authorMeta.id", "authorMeta.name",
    "musicMeta.musicId", "hashtags", "effectStickers", "locationMeta.locationId",
    "playCount", "diggCount", "shareCount", "commentCount", "collectCount",
    "repostCount", "isAd", "isSponsored", "hasTikTokShopProduct",
)


def digest(path: Path) -> str:
    h = sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def value_at(record: dict, dotted: str):
    value = record
    for part in dotted.split("."):
        if not isinstance(value, dict) or part not in value:
            return None
        value = value[part]
    return value


def canonical_hash(record: dict) -> str:
    encoded = json.dumps(record, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return sha256(encoded.encode("utf-8")).hexdigest()


def read_records(path: Path) -> tuple[str, list]:
    with path.open("r", encoding="utf-8-sig") as stream:
        first = stream.read(1)
        stream.seek(0)
        if first == "[":
            data = json.load(stream)
            if not isinstance(data, list):
                raise ValueError(f"Expected JSON array: {path}")
            return "json_array", data
        return "jsonl", [json.loads(line) for line in stream if line.strip()]


def utc_time(record: dict) -> str | None:
    raw = record.get("createTime")
    if isinstance(raw, (int, float)) and not isinstance(raw, bool):
        try:
            return datetime.fromtimestamp(raw, timezone.utc).isoformat()
        except (OverflowError, OSError, ValueError):
            return None
    return None


def tag_key(value: str) -> str:
    return unicodedata.normalize("NFC", value).strip().lstrip("#").casefold()


def write_json(path: Path, content: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    staged = path.with_suffix(path.suffix + ".tmp")
    staged.write_text(json.dumps(content, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    staged.replace(path)


def inventory(source: Path, raw: Path, reports: Path) -> dict:
    source_files = sorted(source.glob("*.json"))
    by_pack = defaultdict(list)
    for path in source_files:
        match = PACK_RE.search(path.name)
        if match:
            by_pack[int(match.group(1))].append(path)
    if any(len(by_pack[i]) != 1 for i in range(1, 9)):
        raise ValueError("Expected exactly one source JSON for each PACK1..PACK8")
    if len(source_files) != 8:
        raise ValueError("Source directory contains extra JSON exports; select exactly eight PACK files")

    raw.mkdir(parents=True, exist_ok=True)
    files = []
    counts = Counter()
    field_missing = Counter()
    field_null = Counter()
    schema = defaultdict(Counter)
    video_seen = {}
    pair_seen = {}
    duplicate_pointers = []
    conflict_pointers = []
    rejected = []
    authors, author_usernames, input_usernames = set(), set(), set()
    sounds, hashtags, effects, locations = set(), set(), set(), set()
    published = []
    for pack in range(1, 9):
        origin = by_pack[pack][0]
        target = raw / origin.name
        source_hash = digest(origin)
        if target.exists():
            if digest(target) != source_hash:
                raise ValueError(f"Raw destination differs; refusing overwrite: {target}")
        else:
            with tempfile.NamedTemporaryFile(dir=raw, prefix=".intake-", delete=False) as staged:
                staged_path = Path(staged.name)
                with origin.open("rb") as source_stream:
                    shutil.copyfileobj(source_stream, staged)
            try:
                if digest(staged_path) != source_hash:
                    raise ValueError(f"Copy checksum mismatch: {origin}")
                staged_path.replace(target)
            finally:
                staged_path.unlink(missing_ok=True)
        fmt, records = read_records(target)
        local = Counter()
        for index, record in enumerate(records):
            pointer = {"source_file": target.name, "record_index": index, "collection_id": f"PACK{pack}"}
            counts["records"] += 1
            local["records"] += 1
            if isinstance(record, dict) and isinstance(record.get("input"), str):
                input_usernames.add(record["input"].strip().casefold())
            if not isinstance(record, dict):
                reason = "not_object"
            elif record.get("error") is not None:
                reason = "source_error"
            elif record.get("id") is None:
                reason = "profile_only" if "authorMeta" in record else "missing_video_id"
            elif isinstance(record.get("id"), bool) or not isinstance(record.get("id"), (str, int)):
                reason = "invalid_video_id"
            else:
                reason = None
            if reason:
                counts[reason] += 1
                local[reason] += 1
                rejected.append({**pointer, "reason": reason})
                continue
            video_id = str(record["id"])
            row_hash = canonical_hash(record)
            pair = (video_id, f"PACK{pack}")
            if pair in pair_seen:
                kind = "exact_duplicate" if pair_seen[pair][0] == row_hash else "conflict"
                counts[kind] += 1
                local[kind] += 1
                (duplicate_pointers if kind == "exact_duplicate" else conflict_pointers).append(
                    {**pointer, "video_id": video_id, "first": pair_seen[pair][1]}
                )
            else:
                pair_seen[pair] = (row_hash, pointer)
            if video_id in video_seen:
                counts["repeated_video_id"] += 1
            else:
                video_seen[video_id] = pointer
            counts["video_records"] += 1
            local["video_records"] += 1
            if not record.get("text"):
                counts["empty_or_missing_text"] += 1
            if record.get("hashtags") == []:
                counts["confirmed_empty_hashtags"] += 1
            if record.get("hashtags") is None:
                counts["unavailable_hashtags"] += 1
            for field in FIELDS:
                current = record
                present = True
                for part in field.split("."):
                    if not isinstance(current, dict) or part not in current:
                        present = False
                        break
                    current = current[part]
                if not present:
                    field_missing[field] += 1
                elif current is None:
                    field_null[field] += 1
            schema[f"PACK{pack}"][",".join(sorted(record))] += 1
            author = value_at(record, "authorMeta.id")
            if author is not None:
                authors.add(str(author))
            username = value_at(record, "authorMeta.name")
            if isinstance(username, str):
                author_usernames.add(username.strip().casefold())
            sound = value_at(record, "musicMeta.musicId")
            if sound is not None:
                sounds.add(str(sound))
            location = value_at(record, "locationMeta.locationId")
            if location is not None:
                locations.add(str(location))
            for tag in record.get("hashtags") or []:
                if isinstance(tag, dict) and isinstance(tag.get("name"), str):
                    key = tag_key(tag["name"])
                    if key:
                        hashtags.add(key)
            for effect in record.get("effectStickers") or []:
                if isinstance(effect, dict) and effect.get("ID") is not None:
                    effects.add(str(effect["ID"]))
            moment = utc_time(record)
            if moment:
                published.append(moment)
        files.append({"pack": f"PACK{pack}", "source_file": origin.name,
                      "source_path": origin.relative_to(ROOT).as_posix(),
                      "raw_path": target.relative_to(ROOT).as_posix(),
                      "sha256": source_hash, "size_bytes": target.stat().st_size,
                      "format": fmt, "counts": dict(sorted(local.items()))})

    result = {
        "manifest_version": 1,
        "schema_version": "apify-profile-v1",
        "collection_id_policy": "PACK is an export identifier; independent observation times are unverified",
        "observed_at": None,
        "files": files,
        "totals": {**dict(sorted(counts.items())), "unique_video_ids": len(video_seen),
                   "unique_author_ids_in_video_records": len(authors),
                   "unique_author_usernames_in_video_records": len(author_usernames),
                   "unique_input_usernames_in_all_records": len(input_usernames),
                   "unique_sound_ids": len(sounds), "unique_hashtag_keys": len(hashtags),
                   "unique_effect_ids": len(effects), "unique_location_ids": len(locations)},
        "published_at_utc": {"min": min(published) if published else None,
                             "max": max(published) if published else None,
                             "unparseable_or_missing": counts["video_records"] - len(published)},
        "missing_fields": dict(sorted(field_missing.items())),
        "null_fields": dict(sorted(field_null.items())),
        "schema_variants": {pack: [{"fields": key.split(","), "count": n}
                                   for key, n in variants.most_common()]
                            for pack, variants in sorted(schema.items())},
        "exact_duplicates": duplicate_pointers,
        "conflicts": conflict_pointers,
        "quarantine": rejected,
    }
    write_json(ROOT / "data/raw/manifests/apify_g01.json", result)
    write_json(reports / "g01_inventory.json", result)
    write_json(ROOT / "data/quarantine/g01_pointers.json",
               {"schema_version": "g01-quarantine-v1", "records": rejected,
                "conflicts": conflict_pointers})
    report = ["# G01 — Kiểm kê nguồn Apify", "", "Nguồn: 8 PACK trong `data/raw/apify/`; checksum SHA-256 trong manifest.",
              "`observed_at` chưa biết; timestamp trong tên file không được dùng làm thời gian quan sát.", "",
              "| PACK | Records | Video records | Source errors | Profile only | SHA-256 |",
              "|---|---:|---:|---:|---:|---|"]
    for item in files:
        c = item["counts"]
        report.append(f'| {item["pack"]} | {c.get("records", 0)} | {c.get("video_records", 0)} | {c.get("source_error", 0)} | {c.get("profile_only", 0)} | `{item["sha256"]}` |')
    report += ["", "## Tổng", ""]
    for key, count in result["totals"].items():
        report.append(f"- {key}: {count}")
    report += ["", f'- published_at_utc: {result["published_at_utc"]["min"]} đến {result["published_at_utc"]["max"]}',
               "", "## Trường thiếu và null trên video records", "",
               "| Field | Absent | Null |", "|---|---:|---:|"]
    for field in FIELDS:
        report.append(f"| `{field}` | {field_missing[field]} | {field_null[field]} |")
    report += ["", "Chi tiết schema từng PACK, pointer quarantine, duplicate và conflict nằm trong `g01_inventory.json`.",
               "`source_error`/`profile_only` không là Video. Duplicates/conflicts cùng video_id + collection_id được báo, không âm thầm chọn bản cuối.",
               "Dữ liệu này là mẫu KOL đã chọn, không đại diện toàn TikTok. Danh sách 578 KOL đầu vào chưa có file `kol.csv` để đối soát."]
    path = reports / "g01_inventory.md"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(report) + "\n", encoding="utf-8")
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=ROOT / "crawled_data")
    parser.add_argument("--raw", type=Path, default=ROOT / "data/raw/apify")
    parser.add_argument("--reports", type=Path, default=ROOT / "outputs/quality")
    args = parser.parse_args()
    result = inventory(args.source.resolve(), args.raw.resolve(), args.reports.resolve())
    print(json.dumps(result["totals"], ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
