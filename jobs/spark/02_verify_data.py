"""Compare local and distributed Silver and record data-quality/task evidence."""

from __future__ import annotations

from hashlib import sha256
import json
from pathlib import Path
import socket

from pyspark.sql import SparkSession, functions as F


ROOT = Path(__file__).resolve().parents[2]
MANIFEST = json.loads((ROOT / "data/raw/manifests/apify_g01.json").read_text(encoding="utf-8"))
CHECKSUMS = {item["source_file"]: item["sha256"] for item in MANIFEST["files"]}
VERSION = sha256("".join(CHECKSUMS[key] for key in sorted(CHECKSUMS)).encode()).hexdigest()[:16]
LOCAL = f"file:///workspace/outputs/local-silver/datasets/{VERSION}"
HDFS = f"hdfs://namenode:8020/bigdata-topic/silver/datasets/{VERSION}"


def host_count(index, rows):
    yield (index, socket.gethostname(), sum(1 for _ in rows))


def main() -> None:
    spark = (SparkSession.builder.appName("g03-verify-local-vs-hdfs")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    counts = {}
    differences = {}
    tables = {}
    for name in ("videos", "observations", "quarantine"):
        distributed = spark.read.parquet(f"{HDFS}/{name}")
        local = spark.read.parquet(f"{LOCAL}/{name}")
        if distributed.schema != local.schema:
            raise AssertionError(f"Schema differs for {name}")
        left_only = distributed.exceptAll(local).count()
        right_only = local.exceptAll(distributed).count()
        counts[name] = distributed.count()
        differences[name] = {"hdfs_only": left_only, "local_only": right_only}
        tables[name] = distributed
    videos, observations, quarantine = (tables[name] for name in ("videos", "observations", "quarantine"))
    expected = MANIFEST["totals"]
    if counts != {"videos": expected["unique_video_ids"],
                  "observations": expected["video_records"],
                  "quarantine": expected["records"] - expected["video_records"]}:
        raise AssertionError(f"Unexpected Silver counts: {counts}")
    if any(value for item in differences.values() for value in item.values()):
        raise AssertionError(f"Local/HDFS content differs: {differences}")
    quality = {
        "unique_video_ids": videos.select("video_id").distinct().count(),
        "null_observed_at": observations.filter(F.col("observed_at_utc").isNull()).count(),
        "confirmed_empty_hashtags": videos.filter(
            F.col("hashtags").isNotNull() & (F.size("hashtags") == 0)).count(),
        "empty_caption_and_hashtags": videos.filter(
            (F.col("text").isNull() | (F.length(F.trim("text")) == 0)) &
            (F.col("hashtags").isNull() | (F.size("hashtags") == 0))
        ).count(),
        "quarantine_by_reason": {row["reject_reason"]: row["count"]
                                 for row in quarantine.groupBy("reject_reason").count().collect()},
    }
    if quality != {"unique_video_ids": expected["unique_video_ids"],
                   "null_observed_at": expected["video_records"],
                   "confirmed_empty_hashtags": expected["confirmed_empty_hashtags"],
                   "empty_caption_and_hashtags": expected["empty_or_missing_text"],
                   "quarantine_by_reason": {"source_error": expected["source_error"],
                                            "profile_only": expected["profile_only"]}}:
        raise AssertionError(f"Silver quality differs from G01: {quality}")
    expected_rejected = {(item["source_file"], item["collection_id"], item["record_index"], item["reason"])
                         for item in MANIFEST["quarantine"]}
    actual_rejected = {(row["source_file"], row["collection_id"], row["record_index"], row["reject_reason"])
                       for row in quarantine.select("source_file", "collection_id", "record_index", "reject_reason").collect()}
    if actual_rejected != expected_rejected:
        raise AssertionError("Silver quarantine source pointers differ from G01")
    source_video_rows = []
    for item in MANIFEST["files"]:
        source_video_rows.extend(row for row in json.loads((ROOT / item["raw_path"]).read_text(encoding="utf-8-sig"))
                                 if isinstance(row, dict) and row.get("id") is not None and row.get("error") is None)
    zero_play_source = sum(row.get("playCount") == 0 for row in source_video_rows)
    false_ad_source = sum(row.get("isAd") is False for row in source_video_rows)
    if observations.filter(F.col("play_count") == 0).count() != zero_play_source:
        raise AssertionError("Zero play count was changed during normalization")
    if videos.filter(F.col("is_ad") == F.lit(False)).count() != false_ad_source:
        raise AssertionError("False isAd values were changed during normalization")
    hosts = videos.rdd.mapPartitionsWithIndex(host_count).collect()
    tasks_by_host = {host: sum(count for _, h, count in hosts if h == host)
                     for _, host, _ in hosts}
    if not {"spark-worker-1", "spark-worker-2"}.issubset(tasks_by_host):
        raise AssertionError(f"Both workers did not read Silver: {hosts}")
    result = {"schema_version": "g03-validation-v1", "dataset_version": VERSION,
              "counts": counts, "local_hdfs_differences": differences,
              "quality": quality, "partition_hosts": hosts,
              "rows_by_worker": tasks_by_host,
              "quarantine_pointers_match_g01": True,
              "zero_play_count": zero_play_source, "false_is_ad_count": false_ad_source}
    (ROOT / "outputs/quality/g03_validation.json").write_text(
        json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print("G03_VALIDATION=" + json.dumps(result, sort_keys=True))
    spark.stop()


if __name__ == "__main__":
    main()
