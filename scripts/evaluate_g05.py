"""Compare G05 autoencoder to a train-only SVD baseline and stage cluster review."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
from sklearn.cluster import KMeans
from sklearn.decomposition import TruncatedSVD
from sklearn.metrics import adjusted_rand_score, silhouette_score


ROOT = Path(__file__).resolve().parents[1]


def read_vectors(path: Path, dimension: int) -> tuple[list[str], np.ndarray]:
    ids, vectors = [], []
    with path.open(encoding="utf-8") as stream:
        for line in stream:
            post_id, values = line.rstrip("\n").split("\t", 1)
            vector = np.fromstring(values, sep=",", dtype=np.float32)
            if len(vector) != dimension:
                raise ValueError(f"Wrong dimension in {path}")
            ids.append(post_id)
            vectors.append(vector)
    if len(ids) != len(set(ids)):
        raise ValueError(f"Duplicate Post in {path}")
    return ids, np.asarray(vectors, dtype=np.float32)


def run(features: Path, model: Path, content: Path) -> dict:
    feature_manifest = json.loads((features / "manifest.json").read_text(encoding="utf-8"))
    model_manifest = json.loads((model / "manifest.json").read_text(encoding="utf-8"))
    if not feature_manifest["passed"] or not model_manifest["passed"] or model_manifest["feature_version"] != feature_manifest["feature_version"]:
        raise ValueError("G05 feature/model releases do not match")
    if hashlib.sha256((model / "best.zip").read_bytes()).hexdigest() != model_manifest["best_model_sha256"]:
        raise ValueError("Best model checksum changed")
    data = {split: read_vectors(features / f"{split}.tsv", 512) for split in ("train", "validation", "test")}
    embedded = {split: read_vectors(model / f"{split}_embeddings.tsv", 64) for split in data}
    for split in data:
        if data[split][0] != embedded[split][0] or len(data[split][0]) != feature_manifest["usable_counts"][split]:
            raise ValueError(f"G05 embedding Post order/count mismatch: {split}")

    svd = TruncatedSVD(n_components=64, random_state=42, n_iter=7)
    svd.fit(data["train"][1])
    svd_mse = {}
    svd_vectors = {}
    for split, (_, matrix) in data.items():
        svd_vectors[split] = svd.transform(matrix)
        reconstructed = svd.inverse_transform(svd_vectors[split])
        svd_mse[split] = float(np.mean(np.square(matrix - reconstructed)))

    svd_first = KMeans(n_clusters=8, random_state=42, n_init=10).fit(svd_vectors["train"])
    svd_second = KMeans(n_clusters=8, random_state=43, n_init=10).fit(svd_vectors["train"])
    svd_stability = float(adjusted_rand_score(svd_first.labels_, svd_second.labels_))
    svd_validation_labels = svd_first.predict(svd_vectors["validation"])
    svd_silhouette = float(silhouette_score(svd_vectors["validation"], svd_validation_labels))

    train_vectors = embedded["train"][1]
    first = KMeans(n_clusters=8, random_state=42, n_init=10).fit(train_vectors)
    second = KMeans(n_clusters=8, random_state=43, n_init=10).fit(train_vectors)
    validation_labels = first.predict(embedded["validation"][1])
    stability = float(adjusted_rand_score(first.labels_, second.labels_))
    silhouette = float(silhouette_score(embedded["validation"][1], validation_labels))
    size = {str(key): value for key, value in sorted(Counter(map(int, first.labels_)).items())}

    content_rows = {}
    for part in sorted((content / "posts").glob("part-*.json")):
        with part.open(encoding="utf-8") as stream:
            for line in stream:
                row = json.loads(line)
                content_rows[row["post_id"]] = row
    review = ROOT / f"outputs/g05/cluster_review_{model_manifest['model_version']}_mixed.csv"
    review.parent.mkdir(parents=True, exist_ok=True)
    with review.open("w", encoding="utf-8-sig", newline="") as stream:
        writer = csv.writer(stream)
        writer.writerow(["cluster", "sample_type", "post_id", "distance", "caption_first_snapshot", "hashtags_first_snapshot", "source_file", "source_row"])
        distances = first.transform(train_vectors)
        ids = embedded["train"][0]
        rng = np.random.default_rng(42)
        for cluster in range(8):
            members = np.where(first.labels_ == cluster)[0]
            nearest = sorted(members, key=lambda index: float(distances[index, cluster]))[:5]
            remaining = [index for index in members if index not in set(nearest)]
            random_sample = list(map(int, rng.choice(remaining, size=min(5, len(remaining)), replace=False)))
            for index in nearest + random_sample:
                row = content_rows[ids[index]]
                writer.writerow([cluster, "centroid" if index in nearest else "random", ids[index], float(distances[index, cluster]),
                                 row.get("caption") or "", ",".join(row.get("hashtags") or []),
                                 row["source_file"], row["source_row"]])
    summary = ROOT / f"outputs/g05/cluster_summary_{model_manifest['model_version']}.csv"
    with summary.open("w", encoding="utf-8-sig", newline="") as stream:
        writer = csv.writer(stream)
        writer.writerow(["cluster", "train_posts", "posts_with_caption", "top_hashtags", "status"])
        for cluster in range(8):
            members = np.where(first.labels_ == cluster)[0]
            tags = Counter(tag for index in members for tag in (content_rows[embedded["train"][0][index]].get("hashtags") or []))
            caption_count = sum(bool((content_rows[embedded["train"][0][index]].get("caption") or "").strip()) for index in members)
            writer.writerow([cluster, len(members), caption_count,
                             ", ".join(f"{tag}:{count}" for tag, count in tags.most_common(10)),
                             "pending_manual_review"])
    report = {"feature_version": feature_manifest["feature_version"], "model_version": model_manifest["model_version"],
              "baseline": "train-only hash-IDF 512 -> TruncatedSVD 64", "svd_mse": svd_mse,
              "svd_cluster_seed_stability_ari": svd_stability,
              "svd_validation_silhouette": svd_silhouette,
              "autoencoder_test_mse": model_manifest["test"]["mse"],
              "zero_test_mse": model_manifest["test"]["zero_mse"],
              "cluster_k": 8, "cluster_seed_stability_ari": stability,
              "validation_silhouette": silhouette, "train_cluster_sizes": size,
              "review_file": str(review), "summary_file": str(summary),
              "review_status": "pending_manual_review",
              "created_at_utc": datetime.now(timezone.utc).isoformat(), "passed": True}
    quality = ROOT / f"outputs/quality/g05_evaluation_{model_manifest['model_version']}.json"
    quality.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    (ROOT / "outputs/quality/g05_evaluation.json").write_text(quality.read_text(encoding="utf-8"), encoding="utf-8")
    print(quality.read_text(encoding="utf-8"))
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--features", type=Path, required=True)
    parser.add_argument("--model", type=Path, required=True)
    parser.add_argument("--content", type=Path, required=True)
    arguments = parser.parse_args()
    run(arguments.features, arguments.model, arguments.content)
