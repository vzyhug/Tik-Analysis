"""Export the verified ViTIED Silver graph facts for bounded Neo4j loading."""

from __future__ import annotations

import argparse
import hashlib
import json
import socket
import time
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession, functions as F


ROOT = Path("/workspace")
GRAPH_VERSION = "vitied-g04-v2"


def task_host(rows):
    count = sum(1 for _ in rows)
    time.sleep(0.15)
    yield socket.gethostname(), count


def run(silver_path: str, output_directory: Path) -> dict:
    silver_report = json.loads((ROOT / "outputs/quality/g03_silver.json").read_text(encoding="utf-8"))
    verify_report = json.loads((ROOT / "outputs/quality/g03_verify.json").read_text(encoding="utf-8"))
    if not silver_report.get("passed") or not verify_report.get("passed") or silver_report["silver"] != silver_path or verify_report["silver"] != silver_path:
        raise ValueError("G03 Silver/verification evidence does not match the requested path")
    if output_directory.exists():
        raise FileExistsError(f"Graph export already exists: {output_directory}")

    spark = SparkSession.builder.appName("vitied-g04-graph-export").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    stage = Path(str(output_directory) + ".__staging__" + spark.sparkContext.applicationId)
    try:
        base = f"hdfs://namenode:9000{silver_path}"
        posts = spark.read.parquet(base + "/posts")
        observations = spark.read.parquet(base + "/observations")
        hashtags = spark.read.parquet(base + "/post_hashtags")
        source_version = silver_report["bronze"].rsplit("/", 1)[-1]
        if any(frame.where(F.col("dataset_version") != source_version).limit(1).count()
               for frame in (posts, observations, hashtags)):
            raise ValueError("Mixed dataset versions in Silver")
        if posts.where("post_id IS NULL OR media_type NOT IN ('video','photo') OR author_username IS NULL OR published_minute_utc IS NULL").limit(1).count():
            raise ValueError("Post key/type/time missing in Silver")
        if hashtags.where("post_id IS NULL OR tag_key IS NULL OR tag_key = ''").limit(1).count():
            raise ValueError("Hashtag key missing in Silver")
        if posts.count() != posts.select("post_id").distinct().count() or hashtags.count() != hashtags.select("post_id", "tag_key").distinct().count():
            raise ValueError("Silver graph grain is not unique")
        if hashtags.join(posts.select("post_id"), "post_id", "left_anti").limit(1).count():
            raise ValueError("Hashtag edge refers to missing Post")

        post_rows = posts.select(
            "post_id", "media_type", "author_username",
            F.date_format("published_minute_utc", "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("published_minute_utc"),
            "observation_count", "dataset_version", "schema_version",
        )
        hashtag_rows = hashtags.select("post_id", "tag_key").distinct()
        tag_nodes = hashtag_rows.select("tag_key").distinct()
        post_sounds = observations.where("sound_id IS NOT NULL").select("post_id", "sound_id").distinct()
        if post_sounds.groupBy("post_id").count().where("count > 1").limit(1).count():
            raise ValueError("Multiple verified Sound IDs for one Post; review before graph load")
        if post_sounds.join(posts.select("post_id"), "post_id", "left_anti").limit(1).count():
            raise ValueError("Sound edge refers to missing Post")
        sound_nodes = post_sounds.select("sound_id").distinct()
        unresolved_sound_observations = observations.where("sound_id IS NULL").count()
        observation_rows = observations.select(
            "post_id", F.expr("unix_micros(observed_at_utc)").cast("string").alias("observed_micros"),
            F.date_format("observed_at_utc", "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'").alias("observed_at_utc"),
            F.date_format("published_at_utc", "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'").alias("published_at_utc"),
            "source_file", "source_row", "caption", "hashtags", "view_count", "like_count",
            "comment_count", "share_count", "save_count",
        )
        tables = {"posts": post_rows, "hashtags": tag_nodes, "post_hashtags": hashtag_rows,
                  "sounds": sound_nodes, "post_sounds": post_sounds, "observations": observation_rows}
        expected = {name: frame.count() for name, frame in tables.items()}
        if expected["posts"] != silver_report["tables"]["posts"] or expected["post_hashtags"] != silver_report["tables"]["post_hashtags"]:
            raise ValueError("G04 Post/edge count differs from G03")
        witness = hashtag_rows.select("post_id").repartition(16).rdd.mapPartitions(task_host).collect()
        task_hosts = sorted({host for host, _ in witness})
        if sum(count for _, count in witness) != expected["post_hashtags"]:
            raise ValueError("G04 task witness row count differs")

        stage.mkdir(parents=True, exist_ok=False)
        for name, frame in tables.items():
            parts = 8 if expected[name] else 1
            frame.repartition(parts).write.mode("errorifexists").json("file://" + str(stage / name))
        readback = {name: spark.read.schema(frame.schema).json("file://" + str(stage / name)).count() for name, frame in tables.items()}
        if expected != readback:
            raise ValueError(f"Graph export read-back mismatch: {expected} vs {readback}")
        checksums = {
            name: {part.name: hashlib.sha256(part.read_bytes()).hexdigest()
                   for part in sorted((stage / name).glob("part-*.json"))}
            for name in tables
        }
        manifest = {"graph_version": GRAPH_VERSION, "dataset_id": "vitied", "dataset_version": source_version,
                    "silver_path": silver_path, "silver_schema_version": silver_report["schema_version"],
                    "exported_at_utc": datetime.now(timezone.utc).isoformat(),
                    "spark_application_id": spark.sparkContext.applicationId, "master": spark.sparkContext.master,
                    "task_hosts": task_hosts, "tables": expected, "readback": readback, "checksums": checksums,
                    "unresolved_sound_observations": unresolved_sound_observations,
                    "topic_node_count": 0, "topic_reason": "No reviewed Post Topic labels yet",
                    "passed": set(task_hosts) >= {"spark-worker-1", "spark-worker-2"}}
        if not manifest["passed"]:
            raise ValueError("Graph export did not witness both Spark workers")
        (stage / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        stage.rename(output_directory)
        quality = ROOT / "outputs/quality/g04_graph_export.json"
        quality.write_text(json.dumps({**manifest, "directory": str(output_directory)}, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        print(quality.read_text(encoding="utf-8"))
        return manifest
    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--silver", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    run(args.silver, args.output)
