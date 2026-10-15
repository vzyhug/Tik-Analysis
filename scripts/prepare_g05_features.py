"""Fit train-only TF-IDF and persist author-disjoint ViTIED G05 features."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import unicodedata
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

import joblib
import numpy as np
from sklearn.feature_extraction.text import HashingVectorizer, TfidfTransformer
from sklearn.pipeline import Pipeline


ROOT = Path(__file__).resolve().parents[1]
FEATURE_VERSIONS = {"caption_hashtag": "vitied-g05-hashidf512-v3",
                    "caption_only": "vitied-g05-caption-idf512-v1"}
SPLIT_SEED = "vitied-author-split-v1"


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def author_split(author: str) -> str:
    bucket = int(hashlib.sha256(f"{SPLIT_SEED}:{author}".encode()).hexdigest()[:8], 16) % 100
    return "train" if bucket < 70 else "validation" if bucket < 85 else "test"


def content_text(row: dict, feature_mode: str = "caption_hashtag") -> str:
    caption = unicodedata.normalize("NFC", (row.get("caption") or "").strip())
    if feature_mode == "caption_only":
        return caption
    tags = row.get("hashtags") or []
    tag_tokens = ["tag_" + re.sub(r"\s+", "_", unicodedata.normalize("NFC", tag).strip()) for tag in tags if tag.strip()]
    return caption + " " + " ".join(tag_tokens)


def author_groups(rows: list[dict]) -> dict[str, str]:
    """Keep authors sharing an exact substantial caption in the same split."""
    parents = {row["author_username"]: row["author_username"] for row in rows}

    def root(author: str) -> str:
        while parents[author] != author:
            parents[author] = parents[parents[author]]
            author = parents[author]
        return author

    by_caption = defaultdict(set)
    for row in rows:
        caption = re.sub(r"\s+", " ", unicodedata.normalize("NFC", row.get("caption") or "").casefold()).strip()
        if len(caption) >= 20:
            by_caption[caption].add(row["author_username"])
    for authors in by_caption.values():
        ordered = sorted(authors)
        for author in ordered[1:]:
            left, right = root(ordered[0]), root(author)
            parents[right] = left
    members = defaultdict(list)
    for author in parents:
        members[root(author)].append(author)
    representative = {author: min(group) for group in members.values() for author in group}
    return representative


def run(content_dir: Path, output: Path, feature_mode: str = "caption_hashtag") -> dict:
    if feature_mode not in FEATURE_VERSIONS:
        raise ValueError(f"Unsupported feature mode: {feature_mode}")
    source = json.loads((content_dir / "manifest.json").read_text(encoding="utf-8"))
    if not source["passed"] or source["feature_source_version"] != "vitied-g05-content-v1":
        raise ValueError("Invalid G05 content source")
    actual = sorted((content_dir / "posts").glob("part-*.json"))
    if {path.name for path in actual} != set(source["checksums"]):
        raise ValueError("G05 content part file set changed")
    rows = []
    for path in actual:
        if sha256(path) != source["checksums"][path.name]:
            raise ValueError(f"G05 content checksum changed: {path}")
        with path.open(encoding="utf-8") as stream:
            rows.extend(json.loads(line) for line in stream if line.strip())
    if len(rows) != source["posts"] or len({row["post_id"] for row in rows}) != len(rows):
        raise ValueError("G05 content Post grain changed")
    if output.exists():
        raise FileExistsError(output)
    stage = Path(str(output) + ".__staging__")
    stage.mkdir(parents=True, exist_ok=False)
    rows.sort(key=lambda row: row["post_id"])
    group_by_author = author_groups(rows)
    split_manifest = []
    usable = defaultdict(list)
    for row in rows:
        author = row["author_username"]
        if not author:
            raise ValueError("Missing author for leakage-safe split")
        split = author_split(group_by_author[author])
        text = content_text(row, feature_mode)
        has_content = bool(text.strip())
        split_manifest.append({"post_id": row["post_id"], "author_username": author,
                               "split_group": group_by_author[author],
                               "split": split, "usable": has_content,
                               "observed_at_utc": row["observed_at_utc"],
                               "source_file": row["source_file"], "source_row": row["source_row"]})
        if has_content:
            usable[split].append((row["post_id"], text))
    authors = {split: {row["author_username"] for row in split_manifest if row["split"] == split}
               for split in ("train", "validation", "test")}
    if any(authors[left] & authors[right] for left, right in (("train", "validation"), ("train", "test"), ("validation", "test"))):
        raise ValueError("Author leakage across splits")
    if any(not usable[split] for split in ("train", "validation", "test")):
        raise ValueError("A split has no usable content")
    vectorizer = Pipeline([
        ("hash", HashingVectorizer(n_features=512, alternate_sign=False, norm=None,
                                    token_pattern=r"(?u)\b\w+\b", dtype=np.float32)),
        ("tfidf", TfidfTransformer(norm="l2", sublinear_tf=True)),
    ])
    train_texts = [text for _, text in usable["train"]]
    vectorizer.fit(train_texts)
    dimension = 512
    files = {}
    manifest_by_id = {row["post_id"]: row for row in split_manifest}
    vector_empty = 0
    for split in ("train", "validation", "test"):
        items = usable[split]
        vectors = vectorizer.transform([text for _, text in items]).toarray()
        kept_indices = [index for index, vector in enumerate(vectors) if np.any(vector)]
        kept_set = set(kept_indices)
        vector_empty += len(items) - len(kept_indices)
        for index, (post_id, _) in enumerate(items):
            if index not in kept_set:
                manifest_by_id[post_id]["usable"] = False
        items = [items[index] for index in kept_indices]
        vectors = vectors[kept_indices]
        usable[split] = items
        name = f"{split}.tsv"
        shard_streams = [(stage / f"train_worker_{index}.tsv").open("w", encoding="utf-8", newline="\n")
                         for index in (1, 2)] if split == "train" else []
        with (stage / name).open("w", encoding="utf-8", newline="\n") as stream:
            for index, ((post_id, _), vector) in enumerate(zip(items, vectors)):
                line = post_id + "\t" + ",".join(f"{float(value):.8g}" for value in vector) + "\n"
                stream.write(line)
                if shard_streams:
                    shard_streams[index % 2].write(line)
        for shard in shard_streams:
            shard.close()
        files[name] = sha256(stage / name)
    files.update({name: sha256(stage / name) for name in ("train_worker_1.tsv", "train_worker_2.tsv")})
    with (stage / "split_manifest.jsonl").open("w", encoding="utf-8", newline="\n") as stream:
        for row in split_manifest:
            stream.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
    joblib.dump(vectorizer, stage / "tfidf.joblib")
    contract = {"method": "HashingVectorizer + train-only TfidfTransformer",
                "feature_mode": feature_mode,
                "source_text": "caption only" if feature_mode == "caption_only" else "caption plus hashtags",
                "n_features": dimension,
                "alternate_sign": False, "token_pattern": r"(?u)\b\w+\b", "normalization": "L2"}
    (stage / "feature_contract.json").write_text(json.dumps(contract, ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    files.update({name: sha256(stage / name) for name in ("split_manifest.jsonl", "tfidf.joblib", "feature_contract.json")})
    counts = Counter(row["split"] for row in split_manifest)
    usable_counts = {split: len(usable[split]) for split in ("train", "validation", "test")}
    source_empty_count = source["missing_first_snapshot"]["no_caption"] if feature_mode == "caption_only" else source["missing_first_snapshot"]["neither"]
    manifest = {"feature_version": FEATURE_VERSIONS[feature_mode], "feature_mode": feature_mode,
                "source_feature_version": source["feature_source_version"],
                "dataset_version": source["dataset_version"], "source_posts": len(rows),
                "split_seed": SPLIT_SEED, "split_counts": dict(counts), "usable_counts": usable_counts,
                "author_group_count": len(set(group_by_author.values())),
                "abstain_count": len(rows) - sum(usable_counts.values()),
                "source_empty_count": source_empty_count,
                "vector_empty_count": vector_empty,
                "author_counts": {split: len(values) for split, values in authors.items()},
                "input_dimension": dimension, "normalization": "hashing 512 + IDF fitted on train only, L2",
                "content_policy": ("earliest observed snapshot caption only" if feature_mode == "caption_only"
                                   else "earliest observed snapshot, same-row caption and hashtags"),
                "created_at_utc": datetime.now(timezone.utc).isoformat(), "checksums": files,
                "passed": sum(counts.values()) == source["posts"] and
                len(rows) - sum(usable_counts.values()) == source_empty_count + vector_empty}
    (stage / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    stage.rename(output)
    quality_payload = json.dumps({**manifest, "directory": str(output)}, ensure_ascii=False, indent=2) + "\n"
    quality = ROOT / "outputs/quality" / f"g05_features_{manifest['feature_version']}.json"
    if quality.exists():
        raise FileExistsError(quality)
    quality.write_text(quality_payload, encoding="utf-8")
    (ROOT / "outputs/quality/g05_features.json").write_text(quality_payload, encoding="utf-8")
    print(quality.read_text(encoding="utf-8"))
    return manifest


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--content", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--feature-mode", choices=sorted(FEATURE_VERSIONS), default="caption_hashtag")
    arguments = parser.parse_args()
    run(arguments.content, arguments.output, arguments.feature_mode)
