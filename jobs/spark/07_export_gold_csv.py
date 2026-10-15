"""Export one verified G06 publication as immutable, partitioned CSV files."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession


ROOT = Path("/workspace")
GOLD_ROOT = "/trend/vitied/gold"


def run(publication_id: str) -> dict:
    manifest_path = ROOT / "outputs/quality" / f"g06_gold_{publication_id}.json"
    gold_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if not gold_manifest.get("passed") or gold_manifest.get("publication_id") != publication_id:
        raise ValueError("G07 requires a passed G06 manifest matching this publication")
    final = ROOT / "outputs/g07/csv" / publication_id
    if final.exists():
        raise FileExistsError(f"CSV release already exists: {final}")
    final.parent.mkdir(parents=True, exist_ok=True)
    stage = final.with_name(final.name + f".__staging__{os.getpid()}")
    if stage.exists():
        raise FileExistsError(f"CSV staging path already exists: {stage}")

    spark = SparkSession.builder.appName("vitied-g07-gold-csv-export").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    source = f"hdfs://namenode:9000{GOLD_ROOT}/{publication_id}"
    expected = gold_manifest["tables"]
    try:
        stage.mkdir(parents=True, exist_ok=False)
        readback = {}
        checksums = {}
        for table_name, row_count in expected.items():
            frame = spark.read.parquet(f"{source}/{table_name}")
            if frame.count() != row_count:
                raise ValueError(f"G06 table count changed for {table_name}")
            table_dir = stage / table_name
            if row_count:
                frame.write.mode("errorifexists").option("header", "false") \
                    .option("nullValue", r"\N") \
                    .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'") \
                    .csv("file://" + str(table_dir))
                reopened = spark.read.schema(frame.schema).option("header", "false") \
                    .option("nullValue", r"\N").csv("file://" + str(table_dir))
                readback[table_name] = reopened.count()
                checksums[table_name] = {
                    part.name: hashlib.sha256(part.read_bytes()).hexdigest()
                    for part in sorted(table_dir.glob("part-*.csv"))
                }
            else:
                table_dir.mkdir()
                readback[table_name] = 0
                checksums[table_name] = {}
        if expected != readback:
            raise ValueError(f"CSV export read-back mismatch: {expected} != {readback}")
        manifest = {
            "dataset_id": "vitied", "dataset_version": gold_manifest["dataset_version"],
            "publication_id": publication_id, "gold_schema_version": gold_manifest["gold_schema_version"],
            "gold_path": f"{GOLD_ROOT}/{publication_id}", "exported_at_utc": datetime.now(timezone.utc).isoformat(),
            "format": "headerless CSV UTF-8, backslash-N null marker", "tables": expected,
            "readback": readback, "checksums": checksums, "passed": expected == readback,
        }
        (stage / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        stage.rename(final)
        print(json.dumps({**manifest, "directory": str(final)}, ensure_ascii=False, indent=2))
        return manifest
    except Exception:
        if stage.exists():
            shutil.rmtree(stage)
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--publication-id", required=True)
    run(parser.parse_args().publication_id)
