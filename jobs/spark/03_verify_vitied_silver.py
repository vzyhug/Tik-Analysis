"""Compare G03 Parquet keys and Post metadata against independent G01/local CSV."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from pyspark.sql import SparkSession, functions as F


def main(silver: str) -> dict:
    spark = SparkSession.builder.appName("vitied-g03-verify-local-equivalence").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    try:
        base = f"hdfs://namenode:9000{silver}"
        posts = spark.read.parquet(base + "/posts")
        observations = spark.read.parquet(base + "/observations")
        expected_posts = spark.read.option("header", True).csv("file:///workspace/outputs/quality/vitied_id_resolution.csv").select(
            F.col("canonical_post_id").alias("post_id"), "media_type", "author_username",
            F.to_timestamp("published_minute_utc").alias("published_minute_utc"),
            F.col("observation_count").cast("long").alias("observation_count"),
        )
        actual_posts = posts.select("post_id", "media_type", "author_username", "published_minute_utc", "observation_count")
        post_missing = expected_posts.exceptAll(actual_posts).count()
        post_extra = actual_posts.exceptAll(expected_posts).count()
        expected_keys = spark.read.option("header", True).csv("file:///workspace/outputs/quality/g03_expected_observation_keys.csv").select(
            "post_id", F.to_timestamp("observed_at_utc").alias("observed_at_utc"))
        actual_keys = observations.select("post_id", "observed_at_utc")
        key_missing = expected_keys.exceptAll(actual_keys).count()
        key_extra = actual_keys.exceptAll(expected_keys).count()
        media = {r["media_type"]: r["count"] for r in posts.groupBy("media_type").count().collect()}
        share_placeholder_null = observations.where((F.lower(F.col("raw_share_count")) == "share") & F.col("share_count").isNull()).count()
        unresolved_sound = observations.where(F.col("raw_music_id").isNotNull() & F.col("sound_id").isNull()).count()
        result = {"silver": silver, "post_missing_vs_g01": post_missing, "post_extra_vs_g01": post_extra,
                  "observation_key_missing_vs_local": key_missing, "observation_key_extra_vs_local": key_extra,
                  "media_type_posts": media, "share_placeholder_as_null": share_placeholder_null,
                  "unresolved_sound_observations": unresolved_sound,
                  "passed": post_missing == post_extra == key_missing == key_extra == 0 and media == {"video": 8731, "photo": 383} and share_placeholder_null == 848}
        Path("/workspace/outputs/quality/g03_verify.json").write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        print(json.dumps(result, ensure_ascii=False, indent=2))
        if not result["passed"]:
            raise RuntimeError("G03 local equivalence failed")
        return result
    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--silver", required=True)
    main(parser.parse_args().silver)
