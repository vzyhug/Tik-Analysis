"""G03: normalize the eight Apify JSON arrays into versioned Parquet Silver."""

from __future__ import annotations

import argparse
from hashlib import sha256
import json
from pathlib import Path
import uuid

from pyspark.sql import DataFrame, SparkSession, functions as F, types as T


SCHEMA_VERSION = "silver-v1"
ROOT = Path(__file__).resolve().parents[2]
PIPELINE = json.loads((ROOT / "config/pipeline.json").read_text(encoding="utf-8"))


def source_schema() -> T.StructType:
    string = T.StringType()
    return T.StructType([
        T.StructField("id", string),
        T.StructField("error", string),
        T.StructField("errorCode", string),
        T.StructField("note", string),
        T.StructField("input", string),
        T.StructField("text", string),
        T.StructField("textLanguage", string),
        T.StructField("createTime", T.LongType()),
        T.StructField("createTimeISO", string),
        T.StructField("authorMeta", T.StructType([
            T.StructField("id", string), T.StructField("name", string),
            T.StructField("nickName", string), T.StructField("fans", T.LongType()),
            T.StructField("heart", T.LongType()),
        ])),
        T.StructField("musicMeta", T.StructType([
            T.StructField("musicId", string), T.StructField("musicName", string),
            T.StructField("musicAuthor", string), T.StructField("musicOriginal", T.BooleanType()),
            T.StructField("playUrl", string),
        ])),
        T.StructField("hashtags", T.ArrayType(T.StructType([
            T.StructField("name", string),
        ]))),
        T.StructField("effectStickers", T.ArrayType(T.StructType([
            T.StructField("ID", string), T.StructField("name", string),
        ]))),
        T.StructField("locationMeta", T.StructType([
            T.StructField("locationId", string), T.StructField("locationName", string),
            T.StructField("address", string), T.StructField("city", string),
            T.StructField("cityCode", string), T.StructField("countryCode", string),
        ])),
        T.StructField("videoMeta", T.StructType([
            T.StructField("duration", T.DoubleType()),
            T.StructField("subtitleLinks", T.ArrayType(T.StructType([
                T.StructField("language", string), T.StructField("downloadLink", string),
            ]))),
        ])),
        T.StructField("mentions", T.ArrayType(string)),
        T.StructField("playCount", T.LongType()),
        T.StructField("diggCount", T.LongType()),
        T.StructField("shareCount", T.LongType()),
        T.StructField("commentCount", T.LongType()),
        T.StructField("collectCount", T.LongType()),
        T.StructField("repostCount", T.LongType()),
        T.StructField("isAd", T.BooleanType()),
        T.StructField("isSponsored", T.BooleanType()),
        T.StructField("hasTikTokShopProduct", T.BooleanType()),
        T.StructField("isSlideshow", T.BooleanType()),
        T.StructField("isPinned", T.BooleanType()),
        T.StructField("isStory", T.BooleanType()),
    ])


def load_source(spark: SparkSession, path: str) -> DataFrame:
    files = spark.read.format("binaryFile").load(path)
    parsed = files.select(
        F.col("path"),
        F.from_json(F.decode(F.col("content"), "UTF-8"), T.ArrayType(source_schema())).alias("records"),
    )
    broken = parsed.filter(F.col("records").isNull()).count()
    if broken:
        raise ValueError(f"Could not parse {broken} JSON array files")
    return (parsed.select("path", F.posexplode("records").alias("record_index", "record"))
            .withColumn("source_file", F.regexp_extract("path", r"([^/]+)$", 1))
            .withColumn("collection_id", F.concat(F.lit("PACK"),
                F.regexp_extract("source_file", r"PACK([1-8])\.json$", 1)))
            .drop("path"))


def normalize(base: DataFrame, dataset_version: str, checksums: dict[str, str]) -> tuple[DataFrame, DataFrame, DataFrame]:
    spark = base.sparkSession
    checksum_table = spark.createDataFrame(sorted(checksums.items()), ["source_file", "source_checksum"])
    base = base.join(F.broadcast(checksum_table), "source_file", "left")
    if base.filter(F.col("source_checksum").isNull()).count():
        raise ValueError("Source file missing from G01 manifest")
    reason = (F.when(F.col("record.error").isNotNull(), F.lit("source_error"))
              .when(F.col("record.id").isNull() & F.col("record.authorMeta").isNotNull(), F.lit("profile_only"))
              .when(F.col("record.id").isNull(), F.lit("missing_video_id"))
              .when(F.length(F.col("record.id")) == 0, F.lit("invalid_video_id")))
    base = base.withColumn("reject_reason", reason)
    quarantine = base.filter(F.col("reject_reason").isNotNull()).select(
        "source_file", "collection_id", "record_index", "source_checksum", "reject_reason",
        F.col("record.errorCode").alias("error_code"), F.col("record.input").alias("input_username"),
        F.lit(SCHEMA_VERSION).alias("schema_version"), F.lit(dataset_version).alias("dataset_version"),
    )
    accepted = base.filter(F.col("reject_reason").isNull())
    video_id = F.col("record.id").cast("string")
    hashtags = F.transform(F.col("record.hashtags"), lambda x: x["name"])
    effects = F.transform(F.col("record.effectStickers"),
                          lambda x: F.struct(x["ID"].cast("string").alias("effect_id"), x["name"].alias("name")))
    videos = accepted.select(
        video_id.alias("video_id"), F.col("record.authorMeta.id").cast("string").alias("author_id"),
        F.col("record.authorMeta.name").alias("author_username"),
        F.col("record.authorMeta.nickName").alias("author_nickname"),
        F.col("record.authorMeta.fans").alias("author_followers"),
        F.col("record.authorMeta.heart").alias("author_hearts"),
        F.col("record.input").alias("input_username"),
        F.col("record.text").alias("text"), F.col("record.textLanguage").alias("text_language"),
        F.timestamp_seconds(F.col("record.createTime")).alias("published_at_utc"),
        F.col("record.createTimeISO").alias("create_time_iso_source"),
        hashtags.alias("hashtags"), F.col("record.mentions").alias("mentions"),
        F.col("record.musicMeta.musicId").cast("string").alias("sound_id"),
        F.col("record.musicMeta.musicName").alias("sound_title"),
        F.col("record.musicMeta.musicAuthor").alias("sound_author"),
        F.col("record.musicMeta.playUrl").alias("sound_play_url"),
        F.col("record.musicMeta.musicOriginal").alias("is_original_sound"),
        effects.alias("effects"),
        F.col("record.locationMeta.locationId").cast("string").alias("location_id"),
        F.col("record.locationMeta.locationName").alias("location_name"),
        F.col("record.locationMeta.address").alias("location_address"),
        F.col("record.locationMeta.city").alias("location_city"),
        F.col("record.locationMeta.cityCode").alias("location_city_code"),
        F.col("record.locationMeta.countryCode").alias("location_country_code"),
        F.col("record.videoMeta.duration").alias("duration_seconds"),
        F.col("record.videoMeta.subtitleLinks").alias("subtitle_links"),
        F.col("record.isAd").alias("is_ad"),
        F.col("record.isSponsored").alias("is_sponsored"),
        F.col("record.hasTikTokShopProduct").alias("has_shop_product"),
        F.col("record.isSlideshow").alias("is_slideshow"),
        F.col("record.isPinned").alias("is_pinned"),
        F.col("record.isStory").alias("is_story"),
        "source_file", "source_checksum", "collection_id", "record_index",
        F.lit(SCHEMA_VERSION).alias("schema_version"), F.lit(dataset_version).alias("dataset_version"),
    ).repartition(8, "video_id")
    observations = accepted.select(
        video_id.alias("video_id"),
        F.timestamp_seconds(F.col("record.createTime")).alias("published_at_utc"),
        F.lit(None).cast("timestamp").alias("observed_at_utc"),
        F.col("record.playCount").alias("play_count"),
        F.col("record.diggCount").alias("digg_count"),
        F.col("record.shareCount").alias("share_count"),
        F.col("record.commentCount").alias("comment_count"),
        F.col("record.collectCount").alias("collect_count"),
        F.col("record.repostCount").alias("repost_count"),
        "source_file", "source_checksum", "collection_id", "record_index",
        F.lit(SCHEMA_VERSION).alias("schema_version"), F.lit(dataset_version).alias("dataset_version"),
    ).repartition(8, "video_id")
    return videos, observations, quarantine


def fs_path(spark: SparkSession, uri: str):
    path = spark._jvm.org.apache.hadoop.fs.Path(uri)
    return path.getFileSystem(spark._jsc.hadoopConfiguration()), path


def save_result(result: dict, output: str) -> None:
    suffix = "_local" if output.startswith("file:") else ""
    (ROOT / f"outputs/quality/g03_normalize{suffix}.json").write_text(
        json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=sorted(PIPELINE["modes"]), default="distributed-demo")
    parser.add_argument("--input")
    parser.add_argument("--output")
    args = parser.parse_args()
    if PIPELINE["schema_version"] != SCHEMA_VERSION:
        raise ValueError("Pipeline config and job schema versions differ")
    selected = PIPELINE["modes"][args.mode]
    input_path = args.input or selected["input_glob"]
    output_path = args.output or selected["silver_root"]
    manifest = json.loads((ROOT / "data/raw/manifests/apify_g01.json").read_text(encoding="utf-8"))
    if manifest["conflicts"] or manifest["exact_duplicates"]:
        raise ValueError("Resolve duplicate/conflict records from G01 before G03")
    checksums = {item["source_file"]: item["sha256"] for item in manifest["files"]}
    dataset_version = sha256("".join(checksums[key] for key in sorted(checksums)).encode()).hexdigest()[:16]
    spark = (SparkSession.builder.appName(f"g03-normalize-{dataset_version}")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    output = output_path.rstrip("/")
    final = f"{output}/datasets/{dataset_version}"
    fs, final_path = fs_path(spark, final)
    expected = manifest["totals"]
    if fs.exists(final_path):
        counts = {name: spark.read.parquet(f"{final}/{name}").count()
                  for name in ("videos", "observations", "quarantine")}
        if counts != {"videos": expected["unique_video_ids"],
                      "observations": expected["video_records"],
                      "quarantine": expected["records"] - expected["video_records"]}:
            raise ValueError(f"Published Silver counts differ: {counts}")
        result = {"dataset_version": dataset_version, "schema_version": SCHEMA_VERSION,
                  "source_records": expected["records"], "counts": counts,
                  "status": "already_published", "silver_path": final}
        save_result(result, output)
        print("G03_RESULT=" + json.dumps(result, sort_keys=True))
        spark.stop()
        return
    base = load_source(spark, input_path)
    raw_count = base.count()
    if raw_count != expected["records"]:
        raise ValueError(f"Raw count differs: {raw_count} != {expected['records']}")
    videos, observations, quarantine = normalize(base, dataset_version, checksums)
    video_count = videos.count()
    distinct_video_count = videos.select("video_id").distinct().count()
    observation_count = observations.count()
    reject_count = quarantine.count()
    if (video_count, distinct_video_count, observation_count, reject_count) != (
        expected["video_records"], expected["unique_video_ids"], expected["video_records"],
        expected["records"] - expected["video_records"]
    ):
        raise ValueError("G03 reconciliation differs from G01 manifest")
    staging = f"{output}/_staging/{dataset_version}-{uuid.uuid4().hex}"
    for name, frame in (("videos", videos), ("observations", observations), ("quarantine", quarantine)):
        frame.write.mode("errorifexists").parquet(f"{staging}/{name}")
    staged_counts = {name: spark.read.parquet(f"{staging}/{name}").count()
                     for name in ("videos", "observations", "quarantine")}
    if staged_counts != {"videos": video_count, "observations": observation_count,
                         "quarantine": reject_count}:
        raise ValueError(f"Staged Silver read-back differs: {staged_counts}")
    staging_fs, staging_path = fs_path(spark, staging)
    if not fs.mkdirs(final_path.getParent()):
        raise RuntimeError(f"Could not create Silver dataset parent for {final}")
    if not staging_fs.rename(staging_path, final_path):
        raise RuntimeError(f"Could not publish Silver {staging} -> {final}")
    result = {"dataset_version": dataset_version, "schema_version": SCHEMA_VERSION,
              "source_records": raw_count, "counts": staged_counts, "status": "published",
              "silver_path": final}
    save_result(result, output)
    print("G03_RESULT=" + json.dumps(result, sort_keys=True))
    spark.stop()


if __name__ == "__main__":
    main()
