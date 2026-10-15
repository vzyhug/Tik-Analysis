"""G02 diagnostic: read actual ViTIED CSVs from HDFS and witness both workers.

This checks infrastructure only. It does not normalize CSVs or publish Silver.
"""

from __future__ import annotations

import argparse
import json
import socket
import time
from pathlib import Path

from pyspark.sql import SparkSession


EXPECTED_ROWS = {"training_data.csv": 29636, "streaming_data.csv": 5114}
EXPECTED_WORKERS = {"spark-worker-1", "spark-worker-2"}


def witness_partition(values):
    count = sum(1 for _ in values)
    time.sleep(0.25)
    yield socket.gethostname(), count


def run(hdfs_directory: str) -> dict:
    spark = SparkSession.builder.appName("vitied-g02-hdfs-two-worker").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        registered = set()
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            iterator = spark.sparkContext._jsc.sc().getExecutorMemoryStatus().keysIterator()
            registered = set()
            while iterator.hasNext():
                registered.add(str(iterator.next()).split(":")[0])
            if EXPECTED_WORKERS <= registered:
                break
            time.sleep(1)
        by_file = {}
        task_hosts = set()
        for filename, expected in EXPECTED_ROWS.items():
            path = f"hdfs://namenode:9000{hdfs_directory}/{filename}"
            frame = spark.read.option("header", "true").option("multiLine", "true").csv(path)
            if "vid_id" not in frame.columns:
                raise RuntimeError(f"vid_id missing after Spark CSV read: {filename}")
            witnessed = frame.select("vid_id").repartition(32).rdd.mapPartitions(witness_partition).collect()
            hosts = {host for host, _ in witnessed}
            count = sum(total for _, total in witnessed)
            task_hosts.update(hosts)
            by_file[filename] = {"rows": count, "expected_rows": expected,
                                 "worker_hosts_with_tasks": sorted(hosts), "input_path": path}
        check_path = f"hdfs://namenode:9000{hdfs_directory}/spark-check"
        rows = [(name, item["rows"]) for name, item in by_file.items()]
        spark.createDataFrame(rows, ["source_file", "row_count"]).write.mode("errorifexists").parquet(check_path)
        readback = {row["source_file"]: row["row_count"] for row in spark.read.parquet(check_path).collect()}
        result = {"diagnostic_only": True, "application_id": spark.sparkContext.applicationId,
                  "master": spark.sparkContext.master, "executor_hosts_registered": sorted(registered),
                  "worker_hosts_with_tasks": sorted(task_hosts), "sources": by_file,
                  "spark_hdfs_write_readback": readback, "spark_hdfs_path": check_path}
        result["passed"] = (EXPECTED_WORKERS <= registered and EXPECTED_WORKERS <= task_hosts
                            and all(item["rows"] == item["expected_rows"] for item in by_file.values())
                            and readback == EXPECTED_ROWS)
        output = Path("/workspace/outputs/quality/g02_spark.json")
        output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        print(json.dumps(result, ensure_ascii=False, indent=2))
        if not result["passed"]:
            raise RuntimeError("G02 Spark worker/readback gate failed")
        return result
    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--hdfs-directory", required=True)
    args = parser.parse_args()
    run(args.hdfs_directory)
