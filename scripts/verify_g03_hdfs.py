"""Persist HDFS fsck evidence for the active ViTIED Bronze and Silver release."""

from __future__ import annotations

import json
import re
import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PATHS = {
    "bronze": "/trend/vitied/bronze/vitied-20261013-370234d0358b",
    "silver": "/trend/vitied/silver/vitied-silver-v3/vitied-20261013-370234d0358b",
}


def main() -> dict:
    result = {}
    for label, path in PATHS.items():
        command = ["docker", "compose", "-f", "infra/compose.yaml", "exec", "-T", "namenode", "hdfs", "fsck", path, "-files", "-blocks", "-locations"]
        run = subprocess.run(command, cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False)
        output = run.stdout.decode("utf-8", errors="replace")
        target = ROOT / f"outputs/quality/g03_hdfs_fsck_{label}.txt"
        target.write_text(output, encoding="utf-8")
        blocks = re.search(r"Total blocks \(validated\):\s*(\d+)", output)
        nodes = re.search(r"Number of data-nodes:\s*(\d+)", output)
        missing = re.search(r"Missing blocks:\s*(\d+)", output)
        replication = re.search(r"Average block replication:\s*([0-9.]+)", output)
        healthy = run.returncode == 0 and "Status: HEALTHY" in output and blocks and nodes and missing and replication and int(nodes.group(1)) == 3 and int(missing.group(1)) == 0 and float(replication.group(1)) == 2.0
        result[label] = {"path": path, "fsck_evidence": str(target.relative_to(ROOT)), "blocks": int(blocks.group(1)) if blocks else None,
                         "data_nodes": int(nodes.group(1)) if nodes else None, "missing_blocks": int(missing.group(1)) if missing else None,
                         "average_replication": float(replication.group(1)) if replication else None, "healthy": bool(healthy)}
    result["passed"] = all(item["healthy"] for item in result.values())
    (ROOT / "outputs/quality/g03_hdfs.json").write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, indent=2))
    if not result["passed"]:
        raise RuntimeError("G03 HDFS fsck failed")
    return result


if __name__ == "__main__":
    main()
