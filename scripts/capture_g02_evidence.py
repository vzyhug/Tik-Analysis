"""Capture HDFS replica and Spark worker evidence from the running demo cluster."""

from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
import re
import subprocess
from urllib.request import urlopen


ROOT = Path(__file__).resolve().parents[1]
COMPOSE = ["docker", "compose", "--env-file", str(ROOT / "infra/.env.example"),
           "-f", str(ROOT / "infra/compose.yaml")]


def main() -> None:
    command = [*COMPOSE, "exec", "-T", "namenode", "hdfs"]
    report = subprocess.run([*command, "dfsadmin", "-report"], check=True,
                            capture_output=True, text=True).stdout
    fsck = subprocess.run([*command, "fsck", "/bigdata-topic/bronze", "-files", "-blocks", "-locations"],
                          check=True, capture_output=True, text=True).stdout
    (ROOT / "outputs/quality/g02_dfsadmin_report.txt").write_text(report, encoding="utf-8")
    (ROOT / "outputs/quality/g02_fsck.txt").write_text(fsck, encoding="utf-8")
    with urlopen("http://localhost:8080/json", timeout=10) as response:
        master = json.load(response)
    live = re.search(r"Live datanodes \((\d+)\)", report)
    replica_counts = [int(value) for value in re.findall(r"Live_repl=(\d+)", fsck)]
    worker_info = [{"id": worker["id"], "state": worker["state"],
                    "cores": worker["cores"], "memory_mib": worker["memory"]}
                   for worker in master["workers"]]
    bronze = json.loads((ROOT / "outputs/quality/g02_bronze_readback.json").read_text(encoding="utf-8"))
    if not live or int(live.group(1)) != 3:
        raise AssertionError("HDFS must have three live DataNodes")
    if "Status: HEALTHY" not in fsck or len(replica_counts) != 8 or any(value != 2 for value in replica_counts):
        raise AssertionError(f"Bronze must have eight healthy blocks with two replicas: {replica_counts}")
    if len(bronze["files"]) != 8:
        raise AssertionError("Bronze read-back checksums are incomplete")
    if len(worker_info) != 2 or any(worker["state"] != "ALIVE" for worker in worker_info):
        raise AssertionError(f"Expected two Spark workers: {worker_info}")
    result = {"schema_version": "g02-evidence-v1",
              "captured_at_utc": datetime.now(timezone.utc).isoformat(),
              "hdfs_live_datanodes": int(live.group(1)),
              "bronze_files": len(bronze["files"]),
              "bronze_block_live_replica_counts": replica_counts,
              "hdfs_fsck_status": "HEALTHY",
              "spark_workers": worker_info,
              "smoke_result": "See outputs/quality/g02_spark_smoke.json and g03_event_evidence.json"}
    (ROOT / "outputs/quality/g02_cluster_evidence.json").write_text(
        json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
