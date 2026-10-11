"""G02 smoke job: read Bronze through HDFS on both workers and write/read Parquet."""

from __future__ import annotations

import json
from pathlib import Path
import socket

from pyspark.sql import SparkSession


BRONZE = "hdfs://namenode:8020/bigdata-topic/bronze/apify/*/*.json"
OUTPUT = "hdfs://namenode:8020/bigdata-topic/smoke/spark_read_write"


def partition_host(index, rows):
    count = sum(1 for _ in rows)
    yield (index, socket.gethostname(), count)


def main() -> None:
    spark = SparkSession.builder.appName("g02-hdfs-two-worker-smoke").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    source = spark.read.format("binaryFile").load(BRONZE).selectExpr("path", "length(content) AS content_bytes")
    source_count = source.count()
    hosts = source.rdd.mapPartitionsWithIndex(partition_host).collect()
    if source_count != 8:
        raise AssertionError(f"Expected 8 PACK files, got {source_count}")
    workers = {host for _, host, count in hosts if count > 0}
    if workers != {"spark-worker-1", "spark-worker-2"}:
        raise AssertionError(f"Tasks did not process input on both workers: {hosts}")
    source.write.mode("overwrite").parquet(OUTPUT)
    readback_count = spark.read.parquet(OUTPUT).count()
    if readback_count != source_count:
        raise AssertionError(f"HDFS Parquet read-back count: {readback_count} != {source_count}")
    result = {"schema_version": "g02-spark-smoke-v1", "source_files": source_count,
              "partition_hosts": hosts, "parquet_rows": readback_count}
    (Path(__file__).resolve().parents[2] / "outputs/quality/g02_spark_smoke.json").write_text(
        json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print("G02_SMOKE_RESULT=" + json.dumps(result, sort_keys=True))
    spark.stop()


if __name__ == "__main__":
    main()
