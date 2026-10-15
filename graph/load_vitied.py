"""Load the checked G04 ViTIED graph export into Neo4j Community."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import re
import unicodedata
from collections import Counter
from pathlib import Path
from urllib.request import Request, urlopen


ROOT = Path(__file__).resolve().parents[1]
ID_PATTERN = re.compile(r"[0-9]{18,20}\Z")
CONSTRAINTS = {
    "vitied_post_key": ("Post", "postKey"),
    "vitied_hashtag_key": ("Hashtag", "tagKey"),
    "vitied_sound_key": ("Sound", "soundKey"),
    "vitied_topic_key": ("Topic", "topicKey"),
    "vitied_observation_key": ("Observation", "observationKey"),
}


class Neo4jClient:
    def __init__(self, url: str, auth_file: Path):
        raw = auth_file.read_text(encoding="utf-8").strip()
        if "/" not in raw:
            raise ValueError("Neo4j auth file must contain user/password")
        self.url = url.rstrip("/") + "/db/neo4j/tx/commit"
        self.authorization = "Basic " + base64.b64encode(raw.replace("/", ":", 1).encode()).decode()

    def query(self, statement: str, parameters: dict | None = None) -> list[dict]:
        payload = json.dumps({"statements": [{"statement": statement, "parameters": parameters or {},
                                               "resultDataContents": ["row"]}]}).encode("utf-8")
        request = Request(self.url, data=payload, headers={"Authorization": self.authorization,
                          "Content-Type": "application/json", "Accept": "application/json"})
        with urlopen(request, timeout=120) as response:
            result = json.load(response)
        if result.get("errors"):
            raise RuntimeError("Neo4j query failed: " + json.dumps(result["errors"], ensure_ascii=False))
        columns = result["results"][0]["columns"]
        return [dict(zip(columns, item["row"])) for item in result["results"][0]["data"]]


def load_export(directory: Path) -> tuple[dict, dict[str, list[dict]]]:
    manifest = json.loads((directory / "manifest.json").read_text(encoding="utf-8"))
    if manifest["graph_version"] not in {"vitied-g04-v1", "vitied-g04-v2"} or manifest["dataset_id"] != "vitied" or not manifest["passed"]:
        raise ValueError("Unsupported or failed graph export")
    names = ("posts", "hashtags", "post_hashtags", "sounds", "post_sounds")
    if manifest["graph_version"] == "vitied-g04-v2":
        names += ("observations",)
    tables = {}
    for name in names:
        expected_files = manifest["checksums"][name]
        actual_files = sorted((directory / name).glob("part-*.json"))
        if {path.name for path in actual_files} != set(expected_files):
            raise ValueError(f"Changed export file set: {name}")
        rows = []
        for path in actual_files:
            if hashlib.sha256(path.read_bytes()).hexdigest() != expected_files[path.name]:
                raise ValueError(f"Checksum mismatch: {path}")
            with path.open(encoding="utf-8") as handle:
                rows.extend(json.loads(line) for line in handle if line.strip())
        if len(rows) != manifest["tables"][name]:
            raise ValueError(f"Count mismatch in {name}")
        tables[name] = rows
    return manifest, tables


def validate_tables(tables: dict[str, list[dict]]) -> None:
    posts = tables["posts"]
    tags = tables["hashtags"]
    sounds = tables["sounds"]
    post_ids = {row["post_id"] for row in posts}
    tag_ids = {row["tag_key"] for row in tags}
    sound_ids = {row["sound_id"] for row in sounds}
    if len(post_ids) != len(posts) or any(not ID_PATTERN.fullmatch(value) for value in post_ids):
        raise ValueError("Invalid or duplicated Post ID")
    if any(row["media_type"] not in ("video", "photo") or row["observation_count"] < 1 for row in posts):
        raise ValueError("Invalid Post type or observation count")
    if len(tag_ids) != len(tags) or any(not value or value != unicodedata.normalize("NFC", value).casefold().strip().lstrip("#") for value in tag_ids):
        raise ValueError("Invalid or duplicated Hashtag key")
    if len(sound_ids) != len(sounds) or any(not ID_PATTERN.fullmatch(value) for value in sound_ids):
        raise ValueError("Invalid or duplicated verified Sound ID")
    for name, key, allowed in (("post_hashtags", "tag_key", tag_ids), ("post_sounds", "sound_id", sound_ids)):
        pairs = [(row["post_id"], row[key]) for row in tables[name]]
        if len(set(pairs)) != len(pairs) or any(post not in post_ids or entity not in allowed for post, entity in pairs):
            raise ValueError(f"Duplicate or orphan {name} edge")
    if "observations" in tables:
        keys = [(row["post_id"], row["observed_micros"]) for row in tables["observations"]]
        if len(keys) != len(set(keys)) or any(post not in post_ids for post, _ in keys):
            raise ValueError("Duplicate or orphan Observation")
    if any(value > 1 for value in Counter(row["post_id"] for row in tables["post_sounds"]).values()):
        raise ValueError("Multiple Sound edges for one Post")


def batches(rows: list[dict], size: int):
    for offset in range(0, len(rows), size):
        yield rows[offset:offset + size]


def graph_counts(client: Neo4jClient) -> dict[str, int]:
    statements = {
        "posts": "MATCH (n:Post {datasetId:'vitied'}) RETURN count(n) AS n",
        "hashtags": "MATCH (n:Hashtag {datasetId:'vitied'}) RETURN count(n) AS n",
        "sounds": "MATCH (n:Sound {datasetId:'vitied'}) RETURN count(n) AS n",
        "topics": "MATCH (n:Topic {datasetId:'vitied'}) RETURN count(n) AS n",
        "post_hashtags": "MATCH (p:Post {datasetId:'vitied'})-[r:HAS_HASHTAG]->(h:Hashtag {datasetId:'vitied'}) RETURN count(r) AS n",
        "post_sounds": "MATCH (p:Post {datasetId:'vitied'})-[r:USES_SOUND]->(s:Sound {datasetId:'vitied'}) RETURN count(r) AS n",
        "post_topics": "MATCH (p:Post {datasetId:'vitied'})-[r:HAS_TOPIC]->(t:Topic {datasetId:'vitied'}) RETURN count(r) AS n",
        "observations": "MATCH (n:Observation {datasetId:'vitied'}) RETURN count(n) AS n",
        "post_observations": "MATCH (p:Post {datasetId:'vitied'})-[r:HAS_OBSERVATION]->(o:Observation {datasetId:'vitied'}) RETURN count(r) AS n",
    }
    return {name: client.query(query)[0]["n"] for name, query in statements.items()}


def connectivity(tables: dict[str, list[dict]]) -> dict[str, int]:
    parent = {"p:" + row["post_id"]: "p:" + row["post_id"] for row in tables["posts"]}
    parent.update({"h:" + row["tag_key"]: "h:" + row["tag_key"] for row in tables["hashtags"]})

    def root(value: str) -> str:
        while parent[value] != value:
            parent[value] = parent[parent[value]]
            value = parent[value]
        return value

    for row in tables["post_hashtags"]:
        left, right = root("p:" + row["post_id"]), root("h:" + row["tag_key"])
        parent[right] = left
    components = Counter(root(value) for value in parent)
    post_components = Counter(root("p:" + row["post_id"]) for row in tables["posts"])
    linked = {row["post_id"] for row in tables["post_hashtags"]}
    return {"component_count": len(components), "largest_component_nodes": max(components.values(), default=0),
            "largest_component_posts": max(post_components.values(), default=0),
            "isolated_posts": len(tables["posts"]) - len(linked)}


def load_once(client: Neo4jClient, manifest: dict, tables: dict[str, list[dict]], batch_size: int) -> dict[str, int]:
    version = manifest["dataset_version"]
    foreign = client.query("MATCH (p:Post {datasetId:'vitied'}) WHERE p.datasetVersion IS NULL OR p.datasetVersion <> $version RETURN count(p) AS n", {"version": version})[0]["n"]
    if foreign:
        raise ValueError("Existing ViTIED Posts have a different dataset version; explicit migration needed")
    for name, (label, key) in CONSTRAINTS.items():
        client.query(f"CREATE CONSTRAINT {name} IF NOT EXISTS FOR (n:{label}) REQUIRE n.{key} IS UNIQUE")
    queries = {
        "posts": "UNWIND $rows AS row MERGE (n:Post {postKey:'vitied:' + row.post_id}) "
                 "SET n.postId=row.post_id, n.datasetId='vitied', n.datasetVersion=$version, "
                 "n.mediaType=row.media_type, n.authorUsername=row.author_username, "
                 "n.publishedMinuteUtc=datetime(row.published_minute_utc), n.observationCount=row.observation_count, "
                 "n.silverSchemaVersion=row.schema_version",
        "hashtags": "UNWIND $rows AS row MERGE (n:Hashtag {tagKey:'vitied:' + row.tag_key}) "
                    "SET n.tag=row.tag_key, n.datasetId='vitied', n.datasetVersion=$version",
        "sounds": "UNWIND $rows AS row MERGE (n:Sound {soundKey:'vitied:' + row.sound_id}) "
                  "SET n.soundId=row.sound_id, n.datasetId='vitied', n.datasetVersion=$version",
        "post_hashtags": "UNWIND $rows AS row MATCH (p:Post {postKey:'vitied:' + row.post_id}) "
                         "MATCH (h:Hashtag {tagKey:'vitied:' + row.tag_key}) "
                         "MERGE (p)-[r:HAS_HASHTAG]->(h) SET r.datasetVersion=$version, r.graphVersion=$graphVersion",
        "post_sounds": "UNWIND $rows AS row MATCH (p:Post {postKey:'vitied:' + row.post_id}) "
                       "MATCH (s:Sound {soundKey:'vitied:' + row.sound_id}) "
                       "MERGE (p)-[r:USES_SOUND]->(s) SET r.datasetVersion=$version, r.graphVersion=$graphVersion",
        "observations": "UNWIND $rows AS row MATCH (p:Post {postKey:'vitied:' + row.post_id}) "
                        "MERGE (o:Observation {observationKey:'vitied:' + row.post_id + ':' + row.observed_micros}) "
                        "SET o.postId=row.post_id, o.observedAtUtc=datetime(row.observed_at_utc), "
                        "o.publishedAtUtc=datetime(row.published_at_utc), o.sourceFile=row.source_file, "
                        "o.sourceRow=row.source_row, o.caption=row.caption, o.hashtags=row.hashtags, "
                        "o.viewCount=row.view_count, o.likeCount=row.like_count, o.commentCount=row.comment_count, "
                        "o.shareCount=row.share_count, o.saveCount=row.save_count, o.datasetId='vitied', "
                        "o.datasetVersion=$version, o.graphVersion=$graphVersion "
                        "MERGE (p)-[r:HAS_OBSERVATION]->(o) SET r.datasetVersion=$version, r.graphVersion=$graphVersion",
    }
    for name in ("posts", "hashtags", "sounds", "post_hashtags", "post_sounds"):
        for batch in batches(tables[name], batch_size):
            client.query(queries[name], {"rows": batch, "version": version, "graphVersion": manifest["graph_version"]})
    if "observations" in tables:
        for batch in batches(tables["observations"], batch_size):
            client.query(queries["observations"], {"rows": batch, "version": version, "graphVersion": manifest["graph_version"]})
    return graph_counts(client)


def run(directory: Path, auth_file: Path, url: str, batch_size: int) -> dict:
    manifest, tables = load_export(directory)
    validate_tables(tables)
    client = Neo4jClient(url, auth_file)
    before = graph_counts(client)
    first = load_once(client, manifest, tables, batch_size)
    second = load_once(client, manifest, tables, batch_size)
    expected = {**manifest["tables"], "topics": before["topics"], "post_topics": before["post_topics"]}
    if "observations" in manifest["tables"]:
        expected["post_observations"] = manifest["tables"]["observations"]
    else:
        expected["observations"] = before["observations"]
        expected["post_observations"] = before["post_observations"]
    constraints = {row["name"] for row in client.query("SHOW CONSTRAINTS YIELD name RETURN name")}
    passed = first == second == expected and set(CONSTRAINTS) <= constraints
    report = {"dataset_id": "vitied", "dataset_version": manifest["dataset_version"],
              "graph_version": manifest["graph_version"], "source": str(directory),
              "export_checksums_verified": True, "before": before, "expected": expected, "first_load": first,
              "reload": second, "constraints": sorted(set(CONSTRAINTS) & constraints),
              "projection": "Post-HAS_HASHTAG-Hashtag, undirected, all dates, datasetId=vitied",
              "connectivity": connectivity(tables),
              "sound_limit": "All source music_id values unresolved in this source version" if not tables["sounds"] else None,
              "topic_limit": "No reviewed Post Topic labels" if before["topics"] == 0 else None,
              "passed": passed}
    output = ROOT / "outputs/quality/g04_graph_load.json"
    output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    if not passed:
        raise ValueError(f"Neo4j graph count/constraint check failed; see {output}")
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--export", type=Path, required=True)
    parser.add_argument("--auth-file", type=Path, default=ROOT / "config/secrets/neo4j_auth.txt")
    parser.add_argument("--url", default=os.environ.get("NEO4J_HTTP_URL", "http://127.0.0.1:7474"))
    parser.add_argument("--batch-size", type=int, default=500)
    args = parser.parse_args()
    if args.batch_size < 1 or args.batch_size > 5000:
        parser.error("batch-size must be between 1 and 5000")
    run(args.export, args.auth_file, args.url, args.batch_size)
