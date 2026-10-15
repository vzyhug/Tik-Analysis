"""Publish the audited ViTIED source bytes to immutable HDFS Bronze."""

from __future__ import annotations

import hashlib
import json
import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "data/raw/manifests/vitied_source_20261013.json"


def docker_hdfs(*args: str, input_bytes: bytes | None = None) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["docker", "compose", "-f", "infra/compose.yaml", "exec", "-T", "namenode", "hdfs", "dfs", *args],
        cwd=ROOT, input=input_bytes, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False,
    )


def must_run(*args: str, input_bytes: bytes | None = None) -> bytes:
    result = docker_hdfs(*args, input_bytes=input_bytes)
    if result.returncode:
        raise RuntimeError(f"HDFS {' '.join(args)}: {result.stderr.decode(errors='replace')}")
    return result.stdout


def main() -> dict:
    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    version = "vitied-20261013-" + manifest["files"][0]["sha256"][:12]
    directory = f"/trend/vitied/bronze/{version}"
    for spec in manifest["files"]:
        path = ROOT / spec["local_path"]
        if hashlib.sha256(path.read_bytes()).hexdigest() != spec["sha256"] or path.stat().st_size != spec["bytes"]:
            raise ValueError(f"Local source changed: {path}")
    must_run("-mkdir", "-p", directory)
    evidence = []
    for spec in manifest["files"]:
        filename = Path(spec["local_path"]).name
        destination = f"{directory}/{filename}"
        exists = docker_hdfs("-test", "-e", destination).returncode == 0
        if not exists:
            must_run("-put", f"/source/vitied/{filename}", destination)
        actual = hashlib.sha256(must_run("-cat", destination)).hexdigest()
        if actual != spec["sha256"]:
            raise ValueError(f"Bronze differs from manifest: {destination}")
        evidence.append({"file": filename, "hdfs_path": destination, "bytes": spec["bytes"],
                         "sha256": actual, "already_present": exists})
    result = {"dataset_version": version, "hdfs_directory": directory, "files": evidence, "passed": True}
    output = ROOT / "outputs/quality/g03_bronze.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return result


if __name__ == "__main__":
    main()
