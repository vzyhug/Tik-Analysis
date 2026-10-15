"""Weighted link analysis for the global ViTIED hashtag co-occurrence graph."""

from __future__ import annotations

import heapq
import math
from typing import Any


def _articlerank(adjacency: dict[str, list[tuple[str, float]]], iterations: int = 50,
                 damping: float = 0.85) -> dict[str, float]:
    """ArticleRank: damp low-degree sources using degree + mean degree."""
    nodes = sorted(adjacency)
    if not nodes:
        return {}
    n = len(nodes)
    rank = {node: 1.0 / n for node in nodes}
    strength = {node: sum(weight for _, weight in adjacency[node]) for node in nodes}
    mean_strength = sum(strength.values()) / n
    for _ in range(iterations):
        updated = {node: (1.0 - damping) / n for node in nodes}
        for source in nodes:
            total = strength[source]
            if total == 0:
                continue
            contribution = damping * rank[source] / (total + mean_strength)
            for target, weight in adjacency[source]:
                updated[target] += contribution * weight
        rank = updated
    return rank


def _closeness_centrality(adjacency: dict[str, list[tuple[str, float]]]) -> dict[str, float]:
    """Weighted, Wasserman-Faust normalized closeness for a disconnected graph."""
    nodes = sorted(adjacency)
    active = [node for node in nodes if adjacency[node]]
    score = dict.fromkeys(nodes, 0.0)
    if len(nodes) < 2:
        return score
    for source in active:
        distance = {source: 0.0}
        queue: list[tuple[float, str]] = [(0.0, source)]
        while queue:
            dist_v, v = heapq.heappop(queue)
            if dist_v > distance[v] + 1e-12:
                continue
            for neighbor, weight in adjacency[v]:
                candidate = dist_v + 1.0 / max(weight, 1e-12)
                if candidate < distance.get(neighbor, math.inf) - 1e-12:
                    distance[neighbor] = candidate
                    heapq.heappush(queue, (candidate, neighbor))
        reachable = len(distance) - 1
        distance_sum = sum(value for node, value in distance.items() if node != source)
        if reachable and distance_sum > 0:
            score[source] = (reachable / distance_sum) * (reachable / (len(nodes) - 1))
    return score


def score_hashtag_network(post_counts: dict[str, int], pair_rows: list[dict[str, Any]],
                          total_posts: int, min_shared_posts: int = 3,
                          min_shared_creators: int = 2,
                          max_tag_prevalence: float = 0.05) -> dict[str, Any]:
    """Build a PMI-weighted hashtag graph and rank it with ArticleRank and closeness.

    Positive PMI compares co-occurrence with the frequency expected from the two
    individual tags. Ubiquitous tags therefore receive little or no edge weight.
    Support and creator thresholds suppress one-off links before centrality scoring.
    """
    population = max(1, int(total_posts))
    frequencies = {str(tag): max(1, int(count)) for tag, count in post_counts.items()}
    generic_tags = {tag for tag, count in frequencies.items()
                    if count / population > max_tag_prevalence}
    nodes = set(frequencies)
    adjacency: dict[str, list[tuple[str, float]]] = {tag: [] for tag in nodes}
    edges: list[dict[str, Any]] = []
    for row in pair_rows:
        source, target = str(row["source"]), str(row["target"])
        shared = int(row["shared_posts"])
        creators = int(row["shared_creators"])
        if source not in frequencies or target not in frequencies or source == target:
            continue
        # Do not let broadly applied reach tags define the scored topology.
        if source in generic_tags or target in generic_tags:
            continue
        if shared < min_shared_posts or creators < min_shared_creators:
            continue
        lift = (shared * population) / (frequencies[source] * frequencies[target])
        pmi = math.log(lift) if lift > 0 else 0.0
        if pmi <= 0:
            continue
        # IDF correction reduces the influence of tags that occur across many
        # otherwise unrelated posts (for example generic reach tags).
        source_idf = math.log((population + 1.0) / (frequencies[source] + 1.0))
        target_idf = math.log((population + 1.0) / (frequencies[target] + 1.0))
        specificity = math.sqrt(max(0.0, source_idf) * max(0.0, target_idf))
        # Shrink small supports and links driven by very few creators.
        weight = (pmi * specificity * math.log1p(shared) * (shared / (shared + 8.0))
                  * min(1.0, creators / 5.0))
        if weight <= 0:
            continue
        adjacency[source].append((target, weight))
        adjacency[target].append((source, weight))
        edges.append({"source": source, "target": target, "shared_posts": shared,
                      "shared_creators": creators, "lift": lift, "pmi": pmi, "weight": weight})

    articlerank = _articlerank(adjacency)
    closeness = _closeness_centrality(adjacency)
    strength = {tag: sum(weight for _, weight in adjacency[tag]) for tag in nodes}
    degree = {tag: len(adjacency[tag]) for tag in nodes}
    rankings = [{"tag": tag, "posts": frequencies[tag], "links": degree[tag],
                 "strength": strength[tag], "articlerank": articlerank[tag],
                 "closeness": closeness[tag]}
                for tag in nodes]
    return {"rankings": rankings, "edges": edges, "articlerank": articlerank,
            "closeness": closeness, "strength": strength, "degree": degree,
            "candidate_pairs": len(pair_rows), "scored_pairs": len(edges),
            "suppressed_pairs": max(0, len(pair_rows) - len(edges)),
            "min_shared_posts": min_shared_posts, "min_shared_creators": min_shared_creators,
            "generic_tags_suppressed": len(generic_tags),
            "max_tag_prevalence": max_tag_prevalence}
