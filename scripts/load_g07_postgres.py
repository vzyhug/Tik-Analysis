"""Atomically load an exported G06 publication into immutable PostgreSQL facts."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
TABLE_COLUMNS = {
    "post_period": "published_date,media_type,post_count,dataset_id,dataset_version,publication_id,aggregation_version,timezone",
    "hashtag_period": "published_date,tag_key,post_count,dataset_id,dataset_version,publication_id,aggregation_version,timezone",
    "hashtag_pair_period": "published_date,tag_key_1,tag_key_2,post_count,dataset_id,dataset_version,publication_id,aggregation_version,timezone",
    "topic_period": "published_date,topic_id,post_count,label_source,dataset_version,publication_id",
    "topic_hashtag": "published_date,topic_id,tag_key,post_count,label_source,dataset_version,publication_id",
    "topic_sound": "published_date,topic_id,sound_id,post_count,label_source,dataset_version,publication_id",
    "engagement_observation": "post_id,observed_at_utc,published_at_utc,view_count,like_count,comment_count,share_count,save_count,media_type,observed_date,dataset_id,dataset_version,publication_id,schema_version,timezone",
    "coverage": "published_date,media_type,post_count,posts_with_caption_first_observation,posts_with_hashtag_first_observation,observation_count,view_observation_count,like_observation_count,comment_observation_count,share_observation_count,save_observation_count,dataset_id,dataset_version,publication_id,schema_version,timezone",
}


def sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def run(publication_id: str) -> dict:
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,99}", publication_id):
        raise ValueError("publication_id contains unsupported characters")
    directory = ROOT / "outputs/g07/csv" / publication_id
    export_manifest = json.loads((directory / "manifest.json").read_text(encoding="utf-8"))
    gold_manifest = json.loads((ROOT / "outputs/quality" / f"g06_gold_{publication_id}.json").read_text(encoding="utf-8"))
    if (not export_manifest.get("passed") or not gold_manifest.get("passed")
            or export_manifest.get("publication_id") != publication_id
            or gold_manifest.get("publication_id") != publication_id):
        raise ValueError("G07 requires passed G06 and CSV manifests for the same publication")
    if export_manifest["tables"] != gold_manifest["tables"] or export_manifest["readback"] != gold_manifest["tables"]:
        raise ValueError("CSV manifest differs from its G06 source counts")
    for table, expected_names in export_manifest["checksums"].items():
        actual_parts = sorted((directory / table).glob("part-*.csv"))
        if {part.name for part in actual_parts} != set(expected_names):
            raise ValueError(f"CSV part list changed for {table}")
        for part in actual_parts:
            digest = hashlib.sha256(part.read_bytes()).hexdigest()
            if digest != expected_names[part.name]:
                raise ValueError(f"CSV checksum mismatch: {table}/{part.name}")

    compose_args = ["docker", "compose", "-f", "infra/compose.yaml", "-f", "infra/compose.powerbi.yaml"]
    psql_args = compose_args + ["exec", "-T", "postgres", "psql", "-X", "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", "trend_detect"]
    schema_sql = (ROOT / "sql/g07_schema.sql").read_text(encoding="utf-8")
    schema_result = subprocess.run(psql_args, input=schema_sql, text=True, capture_output=True, cwd=ROOT)
    if schema_result.returncode:
        raise RuntimeError(f"PostgreSQL G07 schema setup failed:\n{schema_result.stderr[-4000:]}")

    def lit(value):
        return sql_literal(str(value))

    source_silver_path = gold_manifest["silver_path"]
    gold_path = f"/trend/vitied/gold/{publication_id}"
    publication_insert = (
        "INSERT INTO vitied.publication (publication_id,dataset_id,dataset_version,gold_schema_version,"
        "gold_path,source_silver_path,graph_version,published_at_utc,timezone,topic_label_status,sound_status,is_current) VALUES ("
        f"{lit(publication_id)},'vitied',{lit(gold_manifest['dataset_version'])},{lit(gold_manifest['gold_schema_version'])},"
        f"{lit(gold_path)},{lit(source_silver_path)},{lit(gold_manifest['graph_version'])},"
        f"{lit(gold_manifest['published_at_utc'])}::timestamptz,{lit(gold_manifest['timezone'])},"
        f"{lit(gold_manifest['topic_label_status'])},{lit(gold_manifest['sound_status'])},false);"
    )
    commands = ["SET TIME ZONE 'UTC';", "BEGIN;", publication_insert]
    for table, count in export_manifest["tables"].items():
        if count == 0:
            continue
        if table not in TABLE_COLUMNS:
            raise ValueError(f"No PostgreSQL table mapping for {table}")
        container_dir = f"/workspace/outputs/g07/csv/{publication_id}/{table}"
        program = f"cat {container_dir}/part-*.csv"
        commands.append(
            f"\\copy vitied.{table} ({TABLE_COLUMNS[table]}) FROM PROGRAM {sql_literal(program)} WITH (FORMAT csv, NULL '\\N')"
        )
    checks = []
    for table, count in export_manifest["tables"].items():
        checks.append(
            f"IF (SELECT count(*) FROM vitied.{table} WHERE publication_id={lit(publication_id)}) <> {int(count)} "
            f"THEN RAISE EXCEPTION 'G07 row count mismatch: {table}'; END IF;"
        )
    expected_posts = int(gold_manifest["reconciliation"]["silver_posts"])
    expected_snapshots = int(gold_manifest["reconciliation"]["silver_observations"])
    checks.extend([
        f"IF (SELECT COALESCE(sum(post_count),0) FROM vitied.post_period WHERE publication_id={lit(publication_id)}) <> {expected_posts} THEN RAISE EXCEPTION 'G07 Post total mismatch'; END IF;",
        f"IF (SELECT count(*) FROM vitied.engagement_observation WHERE publication_id={lit(publication_id)}) <> {expected_snapshots} THEN RAISE EXCEPTION 'G07 Observation total mismatch'; END IF;",
    ])
    commands.append("DO $g07_validate$ BEGIN " + " ".join(checks) + " END $g07_validate$;")
    commands.append("UPDATE vitied.publication SET is_current=false WHERE is_current;")
    commands.append(f"UPDATE vitied.publication SET is_current=true WHERE publication_id={lit(publication_id)};")
    commands.append("COMMIT;")
    result = subprocess.run(psql_args, input="\n".join(commands) + "\n", text=True, capture_output=True, cwd=ROOT)
    if result.returncode:
        raise RuntimeError(f"PostgreSQL G07 import rolled back or failed:\n{result.stdout[-2000:]}\n{result.stderr[-4000:]}")

    verification_sql = (
        "SELECT json_build_object('current_publication', (SELECT publication_id FROM vitied.publication WHERE is_current),"
        f"'publication_id', {lit(publication_id)}, 'post_period_rows', (SELECT count(*) FROM vitied.vw_post_period),"
        "'posts', (SELECT COALESCE(sum(post_count),0) FROM vitied.vw_post_period),"
        "'hashtag_period_rows', (SELECT count(*) FROM vitied.vw_hashtag_period),"
        "'hashtag_pair_period_rows', (SELECT count(*) FROM vitied.vw_hashtag_pair_period),"
        "'engagement_observation_rows', (SELECT count(*) FROM vitied.vw_engagement_observation),"
        "'coverage_rows', (SELECT count(*) FROM vitied.vw_coverage),"
        "'topic_rows', (SELECT count(*) FROM vitied.vw_topic_period),"
        "'sound_rows', (SELECT count(*) FROM vitied.vw_topic_sound));"
    )
    verified = subprocess.run(psql_args + ["-Atc", verification_sql], text=True, capture_output=True, cwd=ROOT)
    if verified.returncode:
        raise RuntimeError(f"PostgreSQL post-load query failed:\n{verified.stderr[-4000:]}")
    report = json.loads(verified.stdout.strip())
    if (report["current_publication"] != publication_id or report["posts"] != expected_posts
            or report["engagement_observation_rows"] != expected_snapshots):
        raise RuntimeError(f"PostgreSQL current-release verification failed: {report}")
    output = ROOT / "outputs/quality" / f"g07_postgres_{publication_id}.json"
    output.write_text(json.dumps({**report, "passed": True}, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({**report, "passed": True}, ensure_ascii=False, indent=2))
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--publication-id", required=True)
    run(parser.parse_args().publication_id)
