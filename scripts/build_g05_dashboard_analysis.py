"""Create an immutable, source-checked analysis payload for the DL4J dashboard report."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
from sklearn.cluster import KMeans
from sklearn.decomposition import TruncatedSVD
from sklearn.metrics import adjusted_rand_score, silhouette_score


ROOT = Path(__file__).resolve().parents[1]
TIMEZONE = ZoneInfo("Asia/Ho_Chi_Minh")


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read_vectors(path: Path, dimension: int) -> tuple[list[str], np.ndarray]:
    ids: list[str] = []
    vectors: list[list[float]] = []
    with path.open(encoding="utf-8") as stream:
        for line in stream:
            post_id, values = line.rstrip("\n").split("\t", 1)
            vector = np.fromstring(values, sep=",", dtype=np.float32)
            if vector.size != dimension:
                raise ValueError(f"Unexpected vector dimension in {path}")
            ids.append(post_id)
            vectors.append(vector.tolist())
    if not ids or len(ids) != len(set(ids)):
        raise ValueError(f"Empty or duplicate Post IDs in {path}")
    return ids, np.asarray(vectors, dtype=np.float32)


def run(model_dir: Path, features_dir: Path, content_dir: Path, output: Path) -> dict:
    output = output.resolve()
    model = json.loads((model_dir / "manifest.json").read_text(encoding="utf-8"))
    features = json.loads((features_dir / "manifest.json").read_text(encoding="utf-8"))
    content_manifest = json.loads((content_dir / "manifest.json").read_text(encoding="utf-8"))
    if not model.get("passed") or not features.get("passed") or not content_manifest.get("passed"):
        raise ValueError("Dashboard analysis requires passed content, feature, and model releases")
    if model["feature_version"] != features["feature_version"] or model["dataset_version"] != content_manifest["dataset_version"]:
        raise ValueError("Model, feature, and content releases do not match")
    if features["dataset_version"] != content_manifest["dataset_version"]:
        raise ValueError("Feature and content dataset versions differ")
    for filename, checksum in features["checksums"].items():
        if sha256(features_dir / filename) != checksum:
            raise ValueError(f"Feature checksum mismatch: {filename}")
    if sha256(model_dir / "best.zip") != model["best_model_sha256"]:
        raise ValueError("Best model checksum mismatch")

    content: dict[str, dict] = {}
    for part in sorted((content_dir / "posts").glob("part-*.json")):
        if sha256(part) != content_manifest["checksums"].get(part.name):
            raise ValueError(f"Content checksum mismatch: {part.name}")
        with part.open(encoding="utf-8") as stream:
            for line in stream:
                row = json.loads(line)
                if row["post_id"] in content:
                    raise ValueError("Duplicate Post ID in G05 content")
                content[row["post_id"]] = row
    if len(content) != content_manifest["posts"]:
        raise ValueError("Content Post count differs from manifest")

    split_rows = [json.loads(line) for line in (features_dir / "split_manifest.jsonl").read_text(encoding="utf-8").splitlines()]
    split_by_id = {row["post_id"]: row for row in split_rows}
    if len(split_by_id) != len(split_rows) or set(split_by_id) != set(content):
        raise ValueError("Feature split manifest does not match content Post IDs")

    vectors: dict[str, tuple[list[str], np.ndarray]] = {}
    embeddings: dict[str, tuple[list[str], np.ndarray]] = {}
    for split in ("train", "validation", "test"):
        ids, matrix = read_vectors(features_dir / f"{split}.tsv", 512)
        embedded_ids, latent = read_vectors(model_dir / f"{split}_embeddings.tsv", 64)
        if ids != embedded_ids or len(ids) != features["usable_counts"][split]:
            raise ValueError(f"Post order/count mismatch for {split}")
        vectors[split] = (ids, matrix)
        embeddings[split] = (embedded_ids, latent)

    train_ids, train_vectors = vectors["train"]
    ae_train_ids, ae_train = embeddings["train"]
    kmeans = KMeans(n_clusters=8, random_state=42, n_init=10).fit(ae_train)
    second = KMeans(n_clusters=8, random_state=43, n_init=10).fit(ae_train)
    validation_labels = kmeans.predict(embeddings["validation"][1])
    stability = float(adjusted_rand_score(kmeans.labels_, second.labels_))
    silhouette = float(silhouette_score(embeddings["validation"][1], validation_labels))

    svd = TruncatedSVD(n_components=64, random_state=42, n_iter=7).fit(train_vectors)
    svd_train = svd.transform(train_vectors)
    svd_validation = svd.transform(vectors["validation"][1])
    svd_first = KMeans(n_clusters=8, random_state=42, n_init=10).fit(svd_train)
    svd_second = KMeans(n_clusters=8, random_state=43, n_init=10).fit(svd_train)
    svd_stability = float(adjusted_rand_score(svd_first.labels_, svd_second.labels_))
    svd_silhouette = float(silhouette_score(svd_validation, svd_first.predict(svd_validation)))
    svd_test = svd.transform(vectors["test"][1])
    svd_test_mse = float(np.mean(np.square(vectors["test"][1] - svd.inverse_transform(svd_test))))

    assignments: list[dict] = []
    labels_by_id: dict[str, int] = {}
    for split in ("train", "validation", "test"):
        ids, latent = embeddings[split]
        labels = kmeans.labels_ if split == "train" else kmeans.predict(latent)
        for post_id, label in zip(ids, labels):
            source = content[post_id]
            published = datetime.fromisoformat(source["published_minute_utc"].replace("Z", "+00:00"))
            local_date = published.astimezone(TIMEZONE).date().isoformat()
            labels_by_id[post_id] = int(label)
            assignments.append({"post_id": post_id, "published_date": local_date,
                                "media_type": source["media_type"], "split": split, "cluster": int(label)})
    if len(assignments) != sum(features["usable_counts"].values()):
        raise ValueError("Cluster assignment count does not reconcile to usable embeddings")

    train_members: dict[int, list[str]] = defaultdict(list)
    global_caption_support: Counter[str] = Counter()
    cluster_caption_support: dict[int, Counter[str]] = defaultdict(Counter)
    caption_stop = {"những", "chúng", "mình", "mọi", "người", "hôm", "nay", "này", "kia", "đó", "nha", "nè", "nhé", "ạ", "ơi", "thôi", "quá", "rất", "được", "không", "thì", "mà", "bạn", "em", "chị", "anh", "các", "và", "là", "của", "cho", "với", "một", "có", "trên", "dưới", "tiktok", "video", "fyp", "xuhuong", "viral", "trend", "trending", "follow", "review", "link", "bio", "ib", "inbox", "shop",
                    # Common unaccented forms in this source otherwise dominate the top-lift list.
                    "hon", "nhu", "ngu", "thu", "nhung", "cung", "nhi", "thi", "nhe", "chua", "roi", "nay", "do", "nhat", "mot", "la", "va", "voi", "cho", "tren", "duoi", "minh", "nguoi", "hom", "cac", "duoc", "khong", "ban", "em", "chi", "anh"}

    def display_caption(value: str) -> str:
        if not any(marker in value for marker in ("Ã", "Â", "ð")) and not any("\x80" <= char <= "\x9f" for char in value):
            return value
        try:
            repaired = value.encode("latin-1").decode("utf-8")
            return repaired if sum(repaired.count(marker) for marker in ("Ã", "Â", "ð")) < sum(value.count(marker) for marker in ("Ã", "Â", "ð")) else value
        except (UnicodeEncodeError, UnicodeDecodeError):
            return value

    def caption_terms(value: str) -> set[str]:
        text = re.sub(r"https?://\S+|@[\w.]+|#[\wÀ-ỹ]+", " ", display_caption(value or "").casefold())
        words = re.findall(r"[^\W_\d][^\W_]{1,}", text, flags=re.UNICODE)
        valid = {word for word in words if word not in caption_stop and len(word) > 2}
        pairs = {f"{left} {right}" for left, right in zip(words, words[1:])
                 if left != right and left in valid and right in valid}
        return valid | pairs

    for post_id in train_ids:
        cluster = labels_by_id[post_id]
        train_members[cluster].append(post_id)
        terms = caption_terms(content[post_id].get("caption") or "")
        global_caption_support.update(terms)
        cluster_caption_support[cluster].update(terms)

    distances = kmeans.transform(ae_train)
    rng = np.random.default_rng(42)
    clusters = []
    for cluster in range(8):
        members = train_members[cluster]
        cluster_size = len(members)
        keywords = []
        caption_count = sum(bool((content[pid].get("caption") or "").strip()) for pid in members)
        for term, count in cluster_caption_support[cluster].items():
            total = global_caption_support[term]
            if count >= 6 and total >= 15 and caption_count:
                lift = (count / caption_count) / (total / max(1, sum(bool((content[pid].get("caption") or "").strip()) for pid in train_ids)))
                if lift >= 1.15:
                    keywords.append({"term": term, "cluster_caption_posts": count,
                                     "train_caption_posts": total, "lift": lift})
        keywords.sort(key=lambda row: (-row["lift"], -row["cluster_caption_posts"], row["term"]))
        nearest_idx = sorted(np.where(kmeans.labels_ == cluster)[0], key=lambda index: float(distances[index, cluster]))[:3]
        remaining = [index for index in np.where(kmeans.labels_ == cluster)[0] if index not in set(nearest_idx)]
        random_idx = list(map(int, rng.choice(remaining, size=min(2, len(remaining)), replace=False)))
        examples = []
        for index in nearest_idx + random_idx:
            post_id = train_ids[index]
            source = content[post_id]
            examples.append({"sample_type": "centroid" if index in nearest_idx else "random",
                             "post_id": post_id,
                             "distance": float(distances[index, cluster]),
                             "caption": display_caption(source.get("caption") or "")})
        clusters.append({"cluster": cluster, "train_posts": cluster_size,
                         "train_caption_posts": caption_count,
                         "caption_keywords": keywords[:10], "examples": examples})

    output_payload = {
        "descriptor_version": "caption-terms-v2-stop-unaccented-filler",
        "dataset_version": features["dataset_version"],
        "feature_version": features["feature_version"],
        "feature_mode": features.get("feature_mode", "caption_hashtag"),
        "model_version": model["model_version"],
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_checksums": {"model": model["best_model_sha256"],
                             "features": features["checksums"],
                             "content": content_manifest["checksums"]},
        "population": {"source_posts": len(content), "abstain_posts": features["abstain_count"],
                       "usable_posts": len(assignments), "usable_by_split": features["usable_counts"]},
        "training": {"architecture": model["architecture"], "rounds": model["rounds"],
                     "batch_size": model["batch_size"], "elapsed_seconds": model["distributed_train_seconds"],
                     "initial_validation_mse": model["initial_validation"]["mse"],
                     "rounds_validation_mse": [item["validation"]["mse"] for item in model["history"]],
                     "best_validation_mse": model["best_validation_mse"],
                     "test_mse": model["test"]["mse"], "zero_baseline_test_mse": model["test"]["zero_mse"]},
        "comparison": {"baseline": "Train-only Hash-IDF 512 → TruncatedSVD 64",
                       "svd_test_mse": svd_test_mse,
                       "ae_test_mse": model["test"]["mse"],
                       "ae_vs_svd_reconstruction_lift": model["test"]["mse"] / svd_test_mse - 1,
                       "ae_cluster_seed_ari": stability, "svd_cluster_seed_ari": svd_stability,
                       "ae_validation_silhouette": silhouette, "svd_validation_silhouette": svd_silhouette},
        "cluster_method": "KMeans k=8, seed=42, n_init=10 fitted only on train embeddings; validation/test assigned by predict",
        "interpretation": "Exploratory cluster IDs; caption-only Vietnamese terms exclude common TikTok filler and hashtags; not human-approved topics or classifier predictions",
        "clusters": clusters,
        "assignments": assignments,
        "passed": len(assignments) == len(content) - features["abstain_count"]
                  and len(train_ids) == features["usable_counts"]["train"],
    }
    if output.exists():
        raise FileExistsError(output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(output_payload, ensure_ascii=False, separators=(",", ":")) + "\n", encoding="utf-8")
    print(json.dumps({"passed": output_payload["passed"], "model_version": model["model_version"],
                      "usable_posts": len(assignments), "cluster_count": len(clusters),
                      "svd_test_mse": svd_test_mse, "ae_test_mse": model["test"]["mse"],
                      "output": str(output.relative_to(ROOT))}, ensure_ascii=False))
    return output_payload


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--model", type=Path, required=True)
    parser.add_argument("--features", type=Path, required=True)
    parser.add_argument("--content", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    run(args.model, args.features, args.content, args.output)
