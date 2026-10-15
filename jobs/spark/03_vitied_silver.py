"""G03 ViTIED: parse exact HDFS CSV bytes and publish typed Silver Parquet.

Spark's multiline CSV reader drops three training observations in this source
version.  The binaryFile RDD plus Python's CSV reader preserves quoted newlines;
normalization and Parquet publication still execute on Spark workers.
"""

from __future__ import annotations

import argparse
import ast
import csv
import hashlib
import io
import json
import re
import socket
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse, unquote

from pyspark.sql import SparkSession, functions as F, types as T, Window


ROOT = Path("/workspace")
RAW_COLUMNS = (
    "user_name", "vid_id", "vid_url", "vid_postTime", "vid_scrapeTime", "vid_caption",
    "vid_hashtags", "music_id", "music_title", "music_authorName", "topic",
    "vid_nview", "vid_nlike", "vid_ncomment", "vid_nshare", "vid_nsave",
)
MEDIA = re.compile(r"^/@([^/]+)/(video|photo)/(\d{18,20})/?$")
INTEGER_ID = re.compile(r"^\d{18,20}$")
METRIC = re.compile(r"^(\d+(?:[.,]\d+)?)\s*([KMB]?)$", re.I)
ALLOWED_CATEGORIES = {"beauty", "fashion", "food", "health", "mom_and_baby", "tech_and_household"}
SCHEMA_VERSION = "vitied-silver-v3"


def csv_records(pair):
    path, payload = pair
    name = path.rsplit("/", 1)[-1]
    raw_bytes = payload.read() if hasattr(payload, "read") else payload
    reader = csv.DictReader(io.StringIO(raw_bytes.decode("utf-8-sig"), newline=""))
    if not set(RAW_COLUMNS) <= set(reader.fieldnames or []) or len(reader.fieldnames or []) != len(set(reader.fieldnames or [])):
        raise ValueError(f"Missing/duplicate columns: {name}")
    for ordinal, row in enumerate(reader, 2):
        yield (name, ordinal, row)


def utc(value):
    try:
        result = datetime.fromisoformat((value or "").strip())
        return result.astimezone(timezone.utc) if result.tzinfo else None
    except ValueError:
        return None


def metric(value):
    raw = (value or "").strip()
    if not raw or raw.casefold() in {"nan", "share"}:
        return None
    match = METRIC.fullmatch(raw)
    if not match:
        raise ValueError("invalid_metric")
    factor = {"": 1, "K": 1000, "M": 1000000, "B": 1000000000}[match.group(2).upper()]
    return int(round(float(match.group(1).replace(",", ".")) * factor))


def normalize(item):
    source_file, source_row, row = item
    failures = []
    if None in row or any(value is None for value in row.values()):
        failures.append("malformed_width")
    raw_id = (row.get("vid_id") or "").strip()
    if raw_id and not INTEGER_ID.fullmatch(raw_id):
        failures.append("invalid_raw_id_format")
    url = (row.get("vid_url") or "").strip()
    parsed = urlparse(url)
    host = (parsed.hostname or "").casefold()
    match = MEDIA.fullmatch(unquote(parsed.path)) if parsed.scheme in {"http", "https"} and (host == "tiktok.com" or host.endswith(".tiktok.com")) else None
    if not match:
        failures.append("no_trusted_media_url_id")
    post_id = match.group(3) if match else None
    media_type = match.group(2) if match else None
    author = (row.get("user_name") or "").strip().lstrip("@").casefold()
    if not author:
        failures.append("missing_author_username")
    if match and author != match.group(1).casefold():
        failures.append("author_url_handle_mismatch")
    published = utc(row.get("vid_postTime"))
    observed = utc(row.get("vid_scrapeTime"))
    if published is None:
        failures.append("invalid_published_at")
    if observed is None:
        failures.append("invalid_observed_at")
    if published and observed and published > observed:
        failures.append("published_after_observed")
    category_raw = (row.get("topic") or "").strip()
    categories = None
    if category_raw and category_raw.casefold() not in {"nan", "[nan]", "null"}:
        try:
            values = ast.literal_eval(category_raw)
            if not isinstance(values, list) or not all(isinstance(x, str) for x in values):
                raise ValueError("invalid_category_type")
            categories = sorted({x.strip().casefold() for x in values if x.strip()}) or None
            if categories and not set(categories) <= ALLOWED_CATEGORIES:
                raise ValueError("unknown_category")
        except (ValueError, SyntaxError) as error:
            failures.append(str(error) if str(error) in {"invalid_category_type", "unknown_category"} else "invalid_category_syntax")
    engagement = {}
    for field in ("vid_nview", "vid_nlike", "vid_ncomment", "vid_nshare", "vid_nsave"):
        try:
            engagement[field] = metric(row.get(field))
        except ValueError:
            failures.append(f"invalid_{field}")
            engagement[field] = None
    music_id = (row.get("music_id") or "").strip()
    if not INTEGER_ID.fullmatch(music_id):
        music_id = None
    hashtag_text = row.get("vid_hashtags")
    hashtags = None if hashtag_text is None or not hashtag_text.strip() else [unicodedata.normalize("NFC", x.strip().lstrip("#")).casefold() for x in hashtag_text.split(",") if x.strip().lstrip("#")]
    fingerprint = hashlib.sha256(json.dumps({k: row.get(k, "") for k in RAW_COLUMNS if k not in {"vid_id", "vid_url"}}, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
    return (source_file, source_row, raw_id, post_id, media_type, author,
            published, observed, row.get("vid_caption"), hashtag_text, hashtags,
            music_id, row.get("music_id"), row.get("music_title"), row.get("music_authorName"),
            category_raw, categories, engagement["vid_nview"], engagement["vid_nlike"],
            engagement["vid_ncomment"], engagement["vid_nshare"], engagement["vid_nsave"], row.get("vid_nshare"),
            fingerprint, "|".join(failures) if failures else None, socket.gethostname())


SCHEMA = T.StructType([
    T.StructField(name, dtype, True) for name, dtype in (
        ("source_file", T.StringType()), ("source_row", T.LongType()), ("raw_vid_id", T.StringType()),
        ("post_id", T.StringType()), ("media_type", T.StringType()), ("author_username", T.StringType()),
        ("published_at_utc", T.TimestampType()), ("observed_at_utc", T.TimestampType()),
        ("caption", T.StringType()), ("hashtags_raw", T.StringType()), ("hashtags", T.ArrayType(T.StringType())),
        ("sound_id", T.StringType()), ("raw_music_id", T.StringType()), ("music_title", T.StringType()),
        ("music_author_name", T.StringType()), ("creator_category_raw", T.StringType()),
        ("creator_categories", T.ArrayType(T.StringType())), ("view_count", T.LongType()),
        ("like_count", T.LongType()), ("comment_count", T.LongType()), ("share_count", T.LongType()),
        ("save_count", T.LongType()), ("raw_share_count", T.StringType()), ("fingerprint", T.StringType()), ("error", T.StringType()),
        ("task_host", T.StringType()),
    )
])


def run(bronze: str, output: str) -> dict:
    spark = SparkSession.builder.appName("vitied-g03-silver").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    try:
        paths = ",".join(f"hdfs://namenode:9000{bronze}/{name}" for name in ("training_data.csv", "streaming_data.csv"))
        source = spark.sparkContext.binaryFiles(paths, minPartitions=2).flatMap(csv_records)
        parsed = spark.createDataFrame(source.repartition(32).map(normalize), SCHEMA).persist()
        raw_count = parsed.count()
        input_counts = {r["source_file"]: r["count"] for r in parsed.groupBy("source_file").count().collect()}
        task_hosts = sorted(r["task_host"] for r in parsed.select("task_host").distinct().collect())
        invalid = parsed.where(F.col("error").isNotNull())
        valid = parsed.where(F.col("error").isNull())
        id_conflicts = valid.groupBy("raw_vid_id").agg(F.countDistinct("post_id").alias("n")).where("n > 1").select("raw_vid_id").withColumn("bad_raw", F.lit(True))
        url_conflicts = valid.groupBy("post_id").agg(F.countDistinct("raw_vid_id").alias("n"), F.countDistinct("author_username").alias("a"), F.countDistinct("media_type").alias("m"), F.countDistinct(F.date_trunc("minute", F.col("published_at_utc"))).alias("p")).where("n > 1 OR a > 1 OR m > 1 OR p > 1").select("post_id").withColumn("bad_post", F.lit(True))
        identity_tagged = valid.join(id_conflicts, "raw_vid_id", "left").join(url_conflicts, "post_id", "left")
        identity_bad = identity_tagged.where("bad_raw = true OR bad_post = true").withColumn("error", F.lit("identity_or_published_minute_conflict")).select(parsed.columns)
        identity_good = identity_tagged.where("bad_raw IS NULL AND bad_post IS NULL").select(parsed.columns)
        identity_conflict_count = identity_bad.count()
        window = Window.partitionBy("post_id", "observed_at_utc").orderBy("source_file", "source_row")
        variants = identity_good.groupBy("post_id", "observed_at_utc").agg(F.countDistinct("fingerprint").alias("variants"))
        observation_tagged = identity_good.join(variants, ["post_id", "observed_at_utc"])
        conflicting = observation_tagged.where("variants > 1").withColumn("error", F.lit("conflicting_observation")).select(parsed.columns)
        conflict_count = conflicting.count()
        ranked = observation_tagged.where("variants = 1").drop("variants").withColumn("observation_rank", F.row_number().over(window))
        duplicates = ranked.where("observation_rank > 1").withColumn("error", F.lit("duplicate_observation")).select(parsed.columns)
        duplicate_count = duplicates.count()
        accepted = ranked.where("observation_rank = 1").drop("observation_rank")
        accepted_count = accepted.count()
        quarantine_all = invalid.unionByName(identity_bad).unionByName(conflicting).unionByName(duplicates)
        quarantine_count = quarantine_all.count()
        posts = accepted.groupBy("post_id").agg(
            F.first("media_type").alias("media_type"), F.first("author_username").alias("author_username"),
            F.min(F.date_trunc("minute", F.col("published_at_utc"))).alias("published_minute_utc"),
            F.count("*").alias("observation_count"),
        ).withColumn("dataset_version", F.lit(bronze.rsplit("/", 1)[-1])).withColumn("schema_version", F.lit(SCHEMA_VERSION))
        observations = accepted.drop("task_host", "fingerprint", "error").withColumn("dataset_version", F.lit(bronze.rsplit("/", 1)[-1])).withColumn("schema_version", F.lit(SCHEMA_VERSION))
        post_hashtags = accepted.select("post_id", F.explode_outer("hashtags").alias("tag_key")).where("tag_key IS NOT NULL").distinct().withColumn("dataset_version", F.lit(bronze.rsplit("/", 1)[-1]))
        creator_categories = accepted.select("author_username", F.explode_outer("creator_categories").alias("creator_category")).where("creator_category IS NOT NULL").distinct().withColumn("dataset_version", F.lit(bronze.rsplit("/", 1)[-1]))
        quarantine = quarantine_all.select("source_file", "source_row", "raw_vid_id", "post_id", "error")
        counts = {"posts": posts.count(), "observations": observations.count(), "post_hashtags": post_hashtags.count(), "creator_categories": creator_categories.count(), "quarantine": quarantine_count}
        if raw_count != sum(input_counts.values()) or raw_count != accepted_count + quarantine_count:
            raise RuntimeError("G03 source row accounting failed")
        hdfs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jvm.java.net.URI.create("hdfs://namenode:9000"), spark._jsc.hadoopConfiguration())
        final_path = spark._jvm.org.apache.hadoop.fs.Path(output)
        if hdfs.exists(final_path):
            raise RuntimeError(f"Silver release already exists: {output}")
        stage = output + ".__staging__" + spark.sparkContext.applicationId
        for name, frame in (("posts", posts), ("observations", observations), ("post_hashtags", post_hashtags), ("creator_categories", creator_categories), ("quarantine", quarantine)):
            frame.write.mode("errorifexists").parquet(f"hdfs://namenode:9000{stage}/{name}")
        readback = {name: spark.read.parquet(f"hdfs://namenode:9000{stage}/{name}").count() for name in counts}
        result = {"schema_version": SCHEMA_VERSION, "bronze": bronze, "silver": output,
                  "application_id": spark.sparkContext.applicationId, "master": spark.sparkContext.master,
                  "source_rows": raw_count, "source_by_file": input_counts, "accepted_observations": accepted_count,
                  "duplicate_observations": duplicate_count, "conflicting_observations": conflict_count,
                  "identity_conflict_rows": identity_conflict_count,
                  "quarantine_rows": quarantine_count, "tables": counts, "readback": readback,
                  "normalization_task_hosts": task_hosts,
                  "passed": input_counts == {"training_data.csv": 29636, "streaming_data.csv": 5114} and counts == readback and set(task_hosts) >= {"spark-worker-1", "spark-worker-2"}}
        if not result["passed"]:
            raise RuntimeError("G03 gate failed")
        hdfs.mkdirs(final_path.getParent())
        if not hdfs.rename(spark._jvm.org.apache.hadoop.fs.Path(stage), final_path):
            raise RuntimeError("Silver staging rename failed")
        (ROOT / "outputs/quality/g03_silver.json").write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return result
    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--bronze", required=True)
    parser.add_argument("--silver", required=True)
    args = parser.parse_args()
    run(args.bronze, args.silver)
