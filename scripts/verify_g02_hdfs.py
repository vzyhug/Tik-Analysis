"""Verify ViTIED source bytes on the existing Docker HDFS cluster.

G02 writes to /trend/diagnostics only. Bronze publication belongs to G03.
The --verify-only mode is intended for read-back after a NameNode restart.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
import uuid
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
COMPOSE = ["docker", "compose", "-f", "infra/compose.yaml", "exec", "-T", "namenode"]
REPORT = ROOT / "outputs/quality/g02_hdfs.json"


def command(*arguments: str) -> str:
    result = subprocess.run([*COMPOSE, *arguments], cwd=ROOT, text=True,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False)
    if result.returncode:
        raise RuntimeError(f"{' '.join(arguments)} failed: {result.stderr[-2000:]}")
    return result.stdout


def hdfs_sha256(path: str) -> str:
    process = subprocess.Popen([*COMPOSE, "hdfs", "dfs", "-cat", path], cwd=ROOT,
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    digest = hashlib.sha256()
    assert process.stdout is not None
    while block := process.stdout.read(1024 * 1024):
        digest.update(block)
    _, error = process.communicate()
    if process.returncode:
        raise RuntimeError(f"HDFS read-back failed: {error.decode(errors='replace')[-2000:]}")
    return digest.hexdigest()


def run(verify_only: bool) -> dict:
    manifest = json.loads((ROOT / "data/raw/manifests/vitied_source_20261013.json").read_text(encoding="utf-8"))
    if verify_only:
        previous = json.loads(REPORT.read_text(encoding="utf-8"))
        directory = previous["hdfs_directory"]
    else:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        directory = f"/trend/diagnostics/vitied-g02/{stamp}-{uuid.uuid4().hex[:8]}"
        command("hdfs", "dfs", "-mkdir", "-p", directory)
    files = []
    for spec in manifest["files"]:
        filename = Path(spec["local_path"]).name
        source = ROOT / spec["local_path"]
        local_hash = hashlib.sha256(source.read_bytes()).hexdigest()
        if local_hash != spec["sha256"]:
            raise ValueError(f"Local source checksum changed: {filename}")
        target = f"{directory}/{filename}"
        if not verify_only:
            command("test", "-f", f"/source/vitied/{filename}")
            command("hdfs", "dfs", "-put", f"/source/vitied/{filename}", target)
            command("hdfs", "dfs", "-setrep", "-w", "2", target)
        remote_hash = hdfs_sha256(target)
        files.append({"source_file": filename, "bytes": spec["bytes"],
                      "expected_sha256": spec["sha256"], "hdfs_sha256": remote_hash,
                      "match": remote_hash == spec["sha256"], "hdfs_path": target})
    fsck = command("hdfs", "fsck", directory, "-files", "-blocks", "-locations")
    report = command("hdfs", "dfsadmin", "-report")
    fsck_path = ROOT / "outputs/quality/g02_hdfs_fsck.txt"
    fsck_path.parent.mkdir(parents=True, exist_ok=True)
    fsck_path.write_text(fsck, encoding="utf-8")
    live_match = re.search(r"Live datanodes \((\d+)\)", report)
    replication_matches = re.findall(r"repl=(\d+)", fsck)
    result = {
        "checked_at_utc": datetime.now(timezone.utc).isoformat(),
        "mode": "restart_readback" if verify_only else "initial_upload_readback",
        "hdfs_directory": directory,
        "files": files,
        "fsck_status_healthy": "Status: HEALTHY" in fsck,
        "fsck_replication_values": sorted({int(value) for value in replication_matches}),
        "fsck_block_count": len(replication_matches),
        "live_datanodes": int(live_match.group(1)) if live_match else None,
        "fsck_details": str(fsck_path.relative_to(ROOT)).replace("\\", "/"),
    }
    result["passed"] = (all(item["match"] for item in files) and result["fsck_status_healthy"]
                        and result["live_datanodes"] == 3 and result["fsck_block_count"] >= 2
                        and result["fsck_replication_values"] == [2])
    target_report = ROOT / ("outputs/quality/g02_hdfs_restart.json" if verify_only else "outputs/quality/g02_hdfs.json")
    target_report.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    if not result["passed"]:
        raise RuntimeError(f"G02 HDFS gate failed; inspect {target_report}")
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--verify-only", action="store_true")
    args = parser.parse_args()
    print(json.dumps(run(args.verify_only), ensure_ascii=False, indent=2))
