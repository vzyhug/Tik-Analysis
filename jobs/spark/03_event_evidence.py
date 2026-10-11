"""Summarize persisted Spark event logs for G03 executor/task evidence."""

from __future__ import annotations

from collections import Counter, defaultdict
import json
from pathlib import Path

from pyspark.sql import SparkSession


ROOT = Path(__file__).resolve().parents[2]
LOGS = ROOT / "outputs/spark-events"


def main() -> None:
    spark = SparkSession.builder.appName("g03-event-evidence-reader").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    grouped = defaultdict(list)
    jvm = spark._jvm
    codec = jvm.org.apache.spark.io.CompressionCodec.createCodec(spark._jsc.sc().conf(), "zstd")
    for path in LOGS.glob("eventlog_v2_app-*/events_*.zstd"):
        stream = codec.compressedInputStream(jvm.java.io.FileInputStream(str(path)))
        reader = jvm.java.io.BufferedReader(jvm.java.io.InputStreamReader(stream, "UTF-8"))
        try:
            while True:
                line = reader.readLine()
                if line is None:
                    break
                if line.strip():
                    grouped[path.parent.name].append(json.loads(line))
        finally:
            reader.close()
    results = []
    for directory, events in grouped.items():
        starts = [event for event in events if event.get("Event") == "SparkListenerApplicationStart"]
        if not starts or not starts[0].get("App Name", "").startswith("g03-normalize-"):
            continue
        name = starts[0]["App Name"]
        executors = {event["Executor ID"]: event["Executor Info"].get("Host")
                     for event in events if event.get("Event") == "SparkListenerExecutorAdded"}
        completed = [event for event in events if event.get("Event") == "SparkListenerTaskEnd"
                     and event.get("Task End Reason", {}).get("Reason") == "Success"]
        task_by_executor = Counter(event["Task Info"]["Executor ID"] for event in completed)
        results.append({"event_log": directory, "app_name": name,
                        "app_id": starts[0].get("App ID"), "executor_hosts": executors,
                        "successful_tasks_by_executor": dict(sorted(task_by_executor.items())),
                        "successful_tasks_by_host": {executors.get(key, "unknown"): count
                                                     for key, count in task_by_executor.items()},
                        "successful_tasks": len(completed),
                        "stages_with_tasks": sorted({event["Stage ID"] for event in completed})})
    if not results:
        raise AssertionError("No G03 normalize event log found")
    output = ROOT / "outputs/quality/g03_event_evidence.json"
    output.write_text(json.dumps({"schema_version": "g03-event-evidence-v1", "apps": results},
                                 ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print("G03_EVENT_EVIDENCE=" + json.dumps(results, sort_keys=True))
    spark.stop()


if __name__ == "__main__":
    main()
