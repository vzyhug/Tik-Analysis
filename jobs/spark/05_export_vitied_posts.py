"""Export one earliest observed content snapshot per ViTIED Post for G05."""

from __future__ import annotations

import argparse
import hashlib
import json
import socket
import time
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession, Window, functions as F


ROOT = Path("/workspace")
FEATURE_SOURCE_VERSION = "vitied-g05-content-v1"


def task_host(rows):
    count = sum(1 for _ in rows)
    time.sleep(0.15)
    yield socket.gethostname(), count


def run(silver_path: str, output: Path) -> dict:
    g03 = json.loads((ROOT / "outputs/quality/g03_silver.json").read_text(encoding="utf-8"))
    verified = json.loads((ROOT / "outputs/quality/g03_verify.json").read_text(encoding="utf-8"))
    if not g03["passed"] or not verified["passed"] or g03["silver"] != silver_path or verified["silver"] != silver_path:
        raise ValueError("G03 evidence does not match Silver input")
    if output.exists():
        raise FileExistsError(output)
    spark = SparkSession.builder.appName("vitied-g05-content-export").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    stage = Path(str(output) + ".__staging__" + spark.sparkContext.applicationId)
    try:
        base = "hdfs://namenode:9000" + silver_path
        posts = spark.read.parquet(base + "/posts")
        observations = spark.read.parquet(base + "/observations")
        version = g03["bronze"].rsplit("/", 1)[-1]
        if any(frame.where(F.col("dataset_version") != version).limit(1).count() for frame in (posts, observations)):
            raise ValueError("Mixed dataset versions")
        if observations.where("post_id IS NULL OR observed_at_utc IS NULL OR source_file IS NULL OR source_row IS NULL").limit(1).count():
            raise ValueError("Missing observation ordering key")
        order = Window.partitionBy("post_id").orderBy("observed_at_utc", "source_file", "source_row")
        first = observations.withColumn("rank", F.row_number().over(order)).where("rank = 1").select(
            "post_id", "observed_at_utc", "source_file", "source_row", "caption", "hashtags"
        )
        content = posts.select("post_id", "author_username", "media_type", "published_minute_utc").join(first, "post_id", "inner")
        content = content.select(
            "post_id", "author_username", "media_type",
            F.date_format("published_minute_utc", "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("published_minute_utc"),
            F.date_format("observed_at_utc", "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("observed_at_utc"),
            "source_file", "source_row", "caption", "hashtags",
        )
        count = content.count()
        if count != g03["tables"]["posts"] or content.select("post_id").distinct().count() != count:
            raise ValueError("G05 content grain does not match G03 Post grain")
        missing = content.agg(
            F.sum(F.when(F.col("caption").isNull() | (F.length(F.trim("caption")) == 0), 1).otherwise(0)).alias("no_caption"),
            F.sum(F.when(F.col("hashtags").isNull() | (F.size("hashtags") == 0), 1).otherwise(0)).alias("no_hashtags"),
            F.sum(F.when((F.col("caption").isNull() | (F.length(F.trim("caption")) == 0)) &
                         (F.col("hashtags").isNull() | (F.size("hashtags") == 0)), 1).otherwise(0)).alias("neither"),
        ).first().asDict()
        witness = content.repartition(16).rdd.mapPartitions(task_host).collect()
        hosts = sorted({host for host, _ in witness})
        if sum(n for _, n in witness) != count or set(hosts) < {"spark-worker-1", "spark-worker-2"}:
            raise ValueError("G05 content export did not use both workers")
        stage.mkdir(parents=True, exist_ok=False)
        content.repartition(8).write.mode("errorifexists").json("file://" + str(stage / "posts"))
        readback = spark.read.schema(content.schema).json("file://" + str(stage / "posts")).count()
        if readback != count:
            raise ValueError("G05 content read-back mismatch")
        checksums = {file.name: hashlib.sha256(file.read_bytes()).hexdigest()
                     for file in sorted((stage / "posts").glob("part-*.json"))}
        manifest = {"feature_source_version": FEATURE_SOURCE_VERSION, "dataset_version": version,
                    "silver_path": silver_path, "source_rows": g03["tables"]["observations"],
                    "posts": count, "missing_first_snapshot": missing, "readback": readback,
                    "task_hosts": hosts, "application_id": spark.sparkContext.applicationId,
                    "exported_at_utc": datetime.now(timezone.utc).isoformat(), "checksums": checksums,
                    "passed": True}
        (stage / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        stage.rename(output)
        report = {**manifest, "output": str(output)}
        quality = ROOT / "outputs/quality/g05_content_export.json"
        quality.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        print(json.dumps({k: v for k, v in report.items() if k != "checksums"}, ensure_ascii=False))
        return report
    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--silver", required=True)
    parser.add_argument("--output", type=Path, required=True)
    arguments = parser.parse_args()
    run(arguments.silver, arguments.output)
