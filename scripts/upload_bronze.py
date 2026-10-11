"""Copy the eight verified PACK exports to immutable HDFS Bronze and read them back."""

from __future__ import annotations

from hashlib import sha256
import json
from pathlib import Path
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[1]
COMPOSE = ["docker", "compose", "--env-file", str(ROOT / "infra/.env.example"),
           "-f", str(ROOT / "infra/compose.yaml")]
BASE = "/bigdata-topic/bronze/apify"


def hdfs(*args: str, check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run([*COMPOSE, "exec", "-T", "namenode", "hdfs", "dfs", *args],
                          check=check, capture_output=True, text=True)


def hdfs_digest(path: str) -> str:
    process = subprocess.Popen([*COMPOSE, "exec", "-T", "namenode", "hdfs", "dfs", "-cat", path],
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    checksum = sha256()
    assert process.stdout is not None
    for chunk in iter(lambda: process.stdout.read(1024 * 1024), b""):
        checksum.update(chunk)
    error = process.stderr.read().decode("utf-8", errors="replace") if process.stderr else ""
    if process.wait() != 0:
        raise RuntimeError(f"HDFS read-back failed for {path}: {error}")
    return checksum.hexdigest()


def upload() -> list[dict]:
    manifest = json.loads((ROOT / "data/raw/manifests/apify_g01.json").read_text(encoding="utf-8"))
    if len(manifest["files"]) != 8:
        raise ValueError("G01 manifest must contain exactly eight PACK files")
    results = []
    for item in manifest["files"]:
        local = ROOT / item["raw_path"]
        expected = item["sha256"]
        if sha256(local.read_bytes()).hexdigest() != expected:
            raise ValueError(f"Local raw checksum changed: {local}")
        directory = f'{BASE}/{item["pack"]}'
        remote = f'{directory}/{item["source_file"]}'
        hdfs("-mkdir", "-p", directory)
        found = hdfs("-test", "-e", remote, check=False)
        if found.returncode == 1:
            container_source = f'/workspace/{item["raw_path"]}'
            hdfs("-put", container_source, remote)
        elif found.returncode != 0:
            raise RuntimeError(f"HDFS existence check failed for {remote}: {found.stderr}")
        actual = hdfs_digest(remote)
        if actual != expected:
            raise ValueError(f"Bronze checksum mismatch for {remote}: {actual} != {expected}")
        results.append({"pack": item["pack"], "hdfs_path": remote,
                        "sha256": actual, "size_bytes": item["size_bytes"]})
        print(f'{item["pack"]}: HDFS read-back SHA-256 matched')
    for directory in ("/bigdata-topic/silver", "/bigdata-topic/gold", "/bigdata-topic/smoke"):
        hdfs("-mkdir", "-p", directory)
        hdfs("-chown", "spark", directory)
    output = ROOT / "outputs/quality/g02_bronze_readback.json"
    output.write_text(json.dumps({"schema_version": "g02-bronze-v1", "files": results},
                                 ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return results


if __name__ == "__main__":
    try:
        upload()
    except (OSError, ValueError, RuntimeError, subprocess.CalledProcessError) as exc:
        print(f"Bronze upload failed: {exc}", file=sys.stderr)
        sys.exit(1)
