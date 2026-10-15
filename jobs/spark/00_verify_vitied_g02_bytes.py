"""G02 diagnostic: Spark executors read and hash real ViTIED HDFS file bytes.

CSV parsing/normalization belongs to G03. This job also checks Spark's HDFS
write/read path and records task hosts for both Standalone workers.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import socket
import time
from pathlib import Path

from pyspark.sql import SparkSession


EXPECTED_WORKERS = {"spark-worker-1", "spark-worker-2"}


def witness_partition(values):
    files = []
    for row in values:
        files.append({"source_file": row.path.rsplit("/", 1)[-1], "bytes": row.length,
                      "sha256": hashlib.sha256(bytes(row.content)).hexdigest()})
    time.sleep(0.25)
    yield socket.gethostname(), files


def run(hdfs_directory: str) -> dict:
    manifest = json.loads(Path("/workspace/data/raw/manifests/vitied_source_20261013.json").read_text(encoding="utf-8"))
    expected = {Path(item["local_path"]).name: item for item in manifest["files"]}
    spark = SparkSession.builder.appName("vitied-g02-hdfs-bytes-two-worker").getOrCreate()
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
        paths = [f"hdfs://namenode:9000{hdfs_directory}/{name}" for name in expected]
        witnessed = (spark.read.format("binaryFile").load(paths).repartition(32).rdd
                     .mapPartitions(witness_partition).collect())
        hosts = sorted({host for host, _ in witnessed})
        source_files = {}
        for _, files in witnessed:
            for item in files:
                name = item["source_file"]
                if name in source_files:
                    raise RuntimeError(f"Spark read source more than once: {name}")
                source_files[name] = {**item, "expected_bytes": expected[name]["bytes"],
                                      "expected_sha256": expected[name]["sha256"]}
        check_path = f"hdfs://namenode:9000{hdfs_directory}/spark-byte-check"
        rows = [(name, item["bytes"], item["sha256"]) for name, item in source_files.items()]
        spark.createDataFrame(rows, ["source_file", "byte_count", "sha256"]).write.mode("errorifexists").parquet(check_path)
        readback = {row["source_file"]: {"bytes": row["byte_count"], "sha256": row["sha256"]}
                    for row in spark.read.parquet(check_path).collect()}
        result = {"diagnostic_only": True, "application_id": spark.sparkContext.applicationId,
                  "master": spark.sparkContext.master, "executor_hosts_registered": sorted(registered),
                  "worker_hosts_with_tasks": hosts, "source_files": source_files,
                  "spark_hdfs_write_readback": readback, "spark_hdfs_path": check_path}
        result["passed"] = (EXPECTED_WORKERS <= registered and EXPECTED_WORKERS <= set(hosts)
                            and set(source_files) == set(expected)
                            and all(item["bytes"] == item["expected_bytes"] and
                                    item["sha256"] == item["expected_sha256"] for item in source_files.values())
                            and readback == {name: {"bytes": item["bytes"], "sha256": item["sha256"]}
                                             for name, item in source_files.items()})
        output = Path("/workspace/outputs/quality/g02_spark_binary.json")
        output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        print(json.dumps(result, ensure_ascii=False, indent=2))
        if not result["passed"]:
            raise RuntimeError("G02 Spark byte readback/worker gate failed")
        return result
    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--hdfs-directory", required=True)
    args = parser.parse_args()
    run(args.hdfs_directory)
