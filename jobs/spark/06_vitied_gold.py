"""Build an immutable, versioned ViTIED Gold publication from verified Silver."""

from __future__ import annotations

import argparse
import json
import socket
import time
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession, Window, functions as F, types as T


ROOT = Path("/workspace")
TIMEZONE = "Asia/Ho_Chi_Minh"
GOLD_SCHEMA_VERSION = "vitied-gold-v1"
G04_DIRECTORY = "outputs/g04/vitied-20261013-370234d0358b"


def empty_table(spark, fields):
    """Create a typed empty table so missing labels cannot be mistaken for zero counts."""
    return spark.createDataFrame([], T.StructType(fields))


def run(silver_path: str, publication_id: str) -> dict:
    silver_report = json.loads((ROOT / "outputs/quality/g03_silver.json").read_text(encoding="utf-8"))
    verify_report = json.loads((ROOT / "outputs/quality/g03_verify.json").read_text(encoding="utf-8"))
    graph_report = json.loads((ROOT / G04_DIRECTORY / "manifest.json").read_text(encoding="utf-8"))
    if not silver_report.get("passed") or not verify_report.get("passed"):
        raise ValueError("G06 requires passed G03 Silver and verification evidence")
    if silver_report["silver"] != silver_path or verify_report["silver"] != silver_path:
        raise ValueError("G06 Silver path differs from verified G03 release")
    dataset_version = silver_report["bronze"].rsplit("/", 1)[-1]
    if graph_report.get("dataset_version") != dataset_version or graph_report.get("silver_path") != silver_path:
        raise ValueError("G04 graph export does not match this Silver release")
    if not graph_report.get("passed"):
        raise ValueError("G06 requires a passed G04 graph export")
    if not publication_id or "/" in publication_id or publication_id in {".", ".."}:
        raise ValueError("publication_id must be a simple non-empty path component")

    spark = SparkSession.builder.appName("vitied-g06-gold").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    hdfs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
        spark._jvm.java.net.URI.create("hdfs://namenode:9000"), spark._jsc.hadoopConfiguration()
    )
    final_path = f"/trend/vitied/gold/{publication_id}"
    final_hdfs_path = spark._jvm.org.apache.hadoop.fs.Path(final_path)
    stage_path = f"{final_path}.__staging__{spark.sparkContext.applicationId}"
    stage_hdfs_path = spark._jvm.org.apache.hadoop.fs.Path(stage_path)
    try:
        if hdfs.exists(final_hdfs_path) or hdfs.exists(stage_hdfs_path):
            raise FileExistsError(f"Gold publication or staging path already exists: {final_path}")
        base = f"hdfs://namenode:9000{silver_path}"
        posts = spark.read.parquet(base + "/posts")
        observations = spark.read.parquet(base + "/observations")
        post_hashtags = spark.read.parquet(base + "/post_hashtags")
        graph_posts = spark.read.json(f"file://{ROOT / G04_DIRECTORY / 'posts'}")
        graph_edges = spark.read.json(f"file://{ROOT / G04_DIRECTORY / 'post_hashtags'}")
        source_version = dataset_version
        for frame in (posts, observations, post_hashtags):
            if "dataset_version" in frame.columns and frame.where(F.col("dataset_version") != source_version).limit(1).count():
                raise ValueError("Mixed dataset versions in Silver")
        if posts.count() != silver_report["tables"]["posts"] or observations.count() != silver_report["tables"]["observations"]:
            raise ValueError("Silver row counts differ from G03 evidence")
        if post_hashtags.count() != silver_report["tables"]["post_hashtags"] or post_hashtags.count() != post_hashtags.select("post_id", "tag_key").distinct().count():
            raise ValueError("Silver Post–Hashtag grain differs from G03 evidence")
        if (graph_report["tables"]["posts"] != silver_report["tables"]["posts"]
                or graph_report["tables"]["post_hashtags"] != silver_report["tables"]["post_hashtags"]):
            raise ValueError("G04 graph export counts differ from verified Silver")
        if posts.where("post_id IS NULL OR published_minute_utc IS NULL OR media_type IS NULL").limit(1).count():
            raise ValueError("Post key, publication time, or media type is missing")
        if posts.count() != posts.select("post_id").distinct().count():
            raise ValueError("Silver posts are not unique by post_id")
        if observations.groupBy("post_id", "observed_at_utc").count().where("count > 1").limit(1).count():
            raise ValueError("Silver observations are not unique by Post and observation time")
        if post_hashtags.join(posts.select("post_id"), "post_id", "left_anti").limit(1).count():
            raise ValueError("Silver hashtag relation refers to an unknown Post")
        silver_post_ids = posts.select("post_id").distinct()
        if (silver_post_ids.join(graph_posts.select("post_id").distinct(), "post_id", "left_anti").limit(1).count()
                or graph_posts.select("post_id").distinct().join(silver_post_ids, "post_id", "left_anti").limit(1).count()):
            raise ValueError("G04 graph Post IDs differ from Silver")
        silver_edges = post_hashtags.select("post_id", "tag_key").distinct()
        graph_edge_ids = graph_edges.select("post_id", "tag_key").distinct()
        if (silver_edges.join(graph_edge_ids, ["post_id", "tag_key"], "left_anti").limit(1).count()
                or graph_edge_ids.join(silver_edges, ["post_id", "tag_key"], "left_anti").limit(1).count()):
            raise ValueError("G04 graph Post–Hashtag edges differ from Silver")

        local_published_date = F.to_date(F.from_utc_timestamp(F.col("published_minute_utc"), TIMEZONE))
        post_period = posts.withColumn("published_date", local_published_date).groupBy(
            "published_date", "media_type"
        ).agg(F.countDistinct("post_id").cast("long").alias("post_count")) \
            .withColumn("dataset_id", F.lit("vitied")) \
            .withColumn("dataset_version", F.lit(source_version)) \
            .withColumn("publication_id", F.lit(publication_id)) \
            .withColumn("aggregation_version", F.lit(GOLD_SCHEMA_VERSION)) \
            .withColumn("timezone", F.lit(TIMEZONE))

        # Use the earliest observation per Post for content features; never union future hashtags.
        observation_order = Window.partitionBy("post_id").orderBy("observed_at_utc", "source_file", "source_row")
        first_observation = observations.withColumn("_rn", F.row_number().over(observation_order)).where("_rn = 1").drop("_rn")
        first_observation = first_observation.select("post_id", "caption", "hashtags")
        first_tags = posts.select("post_id", "published_minute_utc", "media_type").join(first_observation, "post_id") \
            .select("post_id", F.to_date(F.from_utc_timestamp(F.col("published_minute_utc"), TIMEZONE)).alias("published_date"),
                    F.explode_outer("hashtags").alias("tag_key")) \
            .where("tag_key IS NOT NULL AND tag_key <> ''").distinct()
        hashtag_period = first_tags.groupBy("published_date", "tag_key").agg(
            F.countDistinct("post_id").cast("long").alias("post_count")
        ).withColumn("dataset_id", F.lit("vitied")).withColumn("dataset_version", F.lit(source_version)) \
            .withColumn("publication_id", F.lit(publication_id)).withColumn("aggregation_version", F.lit(GOLD_SCHEMA_VERSION)) \
            .withColumn("timezone", F.lit(TIMEZONE))

        left_tags = first_tags.alias("left_tag")
        right_tags = first_tags.alias("right_tag")
        hashtag_pair_period = left_tags.join(
            right_tags,
            (F.col("left_tag.post_id") == F.col("right_tag.post_id"))
            & (F.col("left_tag.tag_key") < F.col("right_tag.tag_key")),
        ).select(
            F.col("left_tag.published_date").alias("published_date"),
            F.col("left_tag.tag_key").alias("tag_key_1"),
            F.col("right_tag.tag_key").alias("tag_key_2"),
            F.col("left_tag.post_id").alias("post_id"),
        ).groupBy("published_date", "tag_key_1", "tag_key_2").agg(
            F.countDistinct("post_id").cast("long").alias("post_count")
        ).withColumn("dataset_id", F.lit("vitied")).withColumn("dataset_version", F.lit(source_version)) \
            .withColumn("publication_id", F.lit(publication_id)).withColumn("aggregation_version", F.lit(GOLD_SCHEMA_VERSION)) \
            .withColumn("timezone", F.lit(TIMEZONE))

        engagement = observations.select(
            "post_id", "observed_at_utc", "published_at_utc", "view_count", "like_count", "comment_count", "share_count", "save_count"
        ).join(posts.select("post_id", "media_type"), "post_id") \
            .withColumn("observed_date", F.to_date(F.from_utc_timestamp(F.col("observed_at_utc"), TIMEZONE))) \
            .withColumn("dataset_id", F.lit("vitied")).withColumn("dataset_version", F.lit(source_version)) \
            .withColumn("publication_id", F.lit(publication_id)).withColumn("schema_version", F.lit(GOLD_SCHEMA_VERSION)) \
            .withColumn("timezone", F.lit(TIMEZONE))

        observation_coverage = observations.join(
            posts.select("post_id", "published_minute_utc"), "post_id"
        ).withColumn("published_date", F.to_date(F.from_utc_timestamp(F.col("published_minute_utc"), TIMEZONE))) \
            .groupBy("published_date", "media_type").agg(
                F.count("*").cast("long").alias("observation_count"),
                F.count("view_count").cast("long").alias("view_observation_count"),
                F.count("like_count").cast("long").alias("like_observation_count"),
                F.count("comment_count").cast("long").alias("comment_observation_count"),
                F.count("share_count").cast("long").alias("share_observation_count"),
                F.count("save_count").cast("long").alias("save_observation_count"),
            )
        coverage = posts.select("post_id", "published_minute_utc", "media_type").join(first_observation, "post_id") \
            .withColumn("published_date", F.to_date(F.from_utc_timestamp(F.col("published_minute_utc"), TIMEZONE))) \
            .withColumn("has_caption", F.col("caption").isNotNull() & (F.length(F.trim("caption")) > 0)) \
            .withColumn("has_hashtag", F.size(F.coalesce(F.col("hashtags"), F.array().cast("array<string>"))) > 0) \
            .groupBy("published_date", "media_type").agg(
                F.countDistinct("post_id").cast("long").alias("post_count"),
                F.sum(F.when(F.col("has_caption"), 1).otherwise(0)).cast("long").alias("posts_with_caption_first_observation"),
                F.sum(F.when(F.col("has_hashtag"), 1).otherwise(0)).cast("long").alias("posts_with_hashtag_first_observation"),
            ).join(observation_coverage, ["published_date", "media_type"], "left") \
            .withColumn("dataset_id", F.lit("vitied")).withColumn("dataset_version", F.lit(source_version)) \
            .withColumn("publication_id", F.lit(publication_id)).withColumn("schema_version", F.lit(GOLD_SCHEMA_VERSION)) \
            .withColumn("timezone", F.lit(TIMEZONE))

        # No reviewed Post-topic labels or verified Sound IDs exist for this source release.
        topic_period = empty_table(spark, [
            T.StructField("published_date", T.DateType()), T.StructField("topic_id", T.StringType()),
            T.StructField("post_count", T.LongType()), T.StructField("label_source", T.StringType()),
            T.StructField("dataset_version", T.StringType()), T.StructField("publication_id", T.StringType()),
        ])
        topic_hashtag = empty_table(spark, [
            T.StructField("published_date", T.DateType()), T.StructField("topic_id", T.StringType()),
            T.StructField("tag_key", T.StringType()), T.StructField("post_count", T.LongType()),
            T.StructField("label_source", T.StringType()), T.StructField("dataset_version", T.StringType()),
            T.StructField("publication_id", T.StringType()),
        ])
        topic_sound = empty_table(spark, [
            T.StructField("published_date", T.DateType()), T.StructField("topic_id", T.StringType()),
            T.StructField("sound_id", T.StringType()), T.StructField("post_count", T.LongType()),
            T.StructField("label_source", T.StringType()), T.StructField("dataset_version", T.StringType()),
            T.StructField("publication_id", T.StringType()),
        ])

        tables = {"post_period": post_period, "topic_period": topic_period, "hashtag_period": hashtag_period,
                  "hashtag_pair_period": hashtag_pair_period,
                  "topic_hashtag": topic_hashtag, "topic_sound": topic_sound,
                  "engagement_observation": engagement, "coverage": coverage}
        expected = {name: frame.count() for name, frame in tables.items()}
        distinct_posts_by_period = post_period.agg(F.sum("post_count").alias("n")).first()["n"]
        distinct_posts = posts.select("post_id").distinct().count()
        if distinct_posts_by_period != distinct_posts:
            raise ValueError(f"Post period aggregation does not reconcile: {distinct_posts_by_period} != {distinct_posts}")
        if expected["engagement_observation"] != silver_report["tables"]["observations"]:
            raise ValueError("Gold engagement observation grain differs from Silver")
        if expected["topic_period"] or expected["topic_hashtag"] or expected["topic_sound"]:
            raise ValueError("Unreviewed labels or unverified Sound IDs entered Topic Gold")

        def task_witness(rows):
            count = sum(1 for _ in rows)
            if count:
                time.sleep(0.15)
                yield socket.gethostname(), count

        witness = hashtag_period.repartition(16).select("tag_key").rdd.mapPartitions(task_witness).collect()
        task_hosts = sorted({host for host, _ in witness})
        if sum(count for _, count in witness) != expected["hashtag_period"]:
            raise ValueError("G06 distributed task witness does not match hashtag period rows")
        if not {"spark-worker-1", "spark-worker-2"} <= set(task_hosts):
            raise RuntimeError(f"G06 Gold aggregation did not run on both workers: {task_hosts}")
        hdfs.mkdirs(stage_hdfs_path)
        for name, frame in tables.items():
            frame.write.mode("errorifexists").parquet(f"hdfs://namenode:9000{stage_path}/{name}")
        readback = {name: spark.read.parquet(f"hdfs://namenode:9000{stage_path}/{name}").count() for name in tables}
        if expected != readback:
            raise ValueError(f"Gold staging read-back mismatch: {expected} != {readback}")
        manifest = {
            "dataset_id": "vitied", "dataset_version": source_version, "publication_id": publication_id,
            "gold_schema_version": GOLD_SCHEMA_VERSION, "silver_path": silver_path,
            "silver_schema_version": silver_report["schema_version"], "graph_version": graph_report["graph_version"],
            "graph_export_counts": graph_report["tables"], "published_at_utc": datetime.now(timezone.utc).isoformat(),
            "timezone": TIMEZONE, "spark_application_id": spark.sparkContext.applicationId,
            "master": spark.sparkContext.master, "task_hosts": task_hosts, "table_grain": {
                "post_period": "local published date x media_type; distinct Post",
                "hashtag_period": "local published date x hashtag from earliest observation; distinct Post",
            "hashtag_pair_period": "local published date x unordered hashtag pair from earliest observation; distinct Post",
                "engagement_observation": "Post x observed_at_utc snapshot",
                "coverage": "local published date x media_type; distinct Post coverage and snapshot count",
                "topic_period": "published date x reviewed Post Topic; empty until manual labels are approved",
                "topic_hashtag": "published date x reviewed Post Topic x hashtag; empty until labels are approved",
                "topic_sound": "published date x reviewed Post Topic x verified Sound ID; empty for current source",
            },
            "tables": expected, "readback": readback, "topic_label_rows": 0,
            "topic_label_status": "unavailable_no_reviewed_post_labels",
            "sound_status": "unavailable_no_verified_sound_ids",
            "reconciliation": {"silver_posts": distinct_posts, "post_period_distinct_sum": distinct_posts_by_period,
                               "silver_observations": silver_report["tables"]["observations"],
                               "graph_posts": graph_report["tables"]["posts"],
                               "graph_post_hashtags": graph_report["tables"]["post_hashtags"],
                               "silver_graph_post_ids_equal": True, "silver_graph_post_hashtag_edges_equal": True},
            "passed": expected == readback and distinct_posts == graph_report["tables"]["posts"]
                      and expected["engagement_observation"] == silver_report["tables"]["observations"],
        }
        if not manifest["passed"]:
            raise RuntimeError("G06 reconciliation gate failed")
        manifest_json = json.dumps(manifest, ensure_ascii=False, indent=2) + "\n"
        spark.sparkContext.parallelize([manifest_json], 1).saveAsTextFile(f"hdfs://namenode:9000{stage_path}/_publication_manifest")
        if not hdfs.rename(stage_hdfs_path, final_hdfs_path):
            raise RuntimeError("Gold publication atomic rename failed")
        (ROOT / "outputs/quality").mkdir(parents=True, exist_ok=True)
        with (ROOT / "outputs/quality" / f"g06_gold_{publication_id}.json").open("w", encoding="utf-8") as handle:
            handle.write(manifest_json)
        print(manifest_json)
        return manifest
    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--silver", required=True)
    parser.add_argument("--publication-id", required=True)
    args = parser.parse_args()
    run(args.silver, args.publication_id)
