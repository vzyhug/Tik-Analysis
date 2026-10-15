"""Measure the same G05 dataset/rounds on one DL4J worker."""

from __future__ import annotations

import argparse
import importlib.util
import json
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("g05_train", ROOT / "scripts/train_g05_distributed.py")
TRAIN = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(TRAIN)


def run(features: Path, distributed: Path, output: Path) -> dict:
    feature_manifest = json.loads((features / "manifest.json").read_text(encoding="utf-8"))
    distributed_manifest = json.loads((distributed / "manifest.json").read_text(encoding="utf-8"))
    if not distributed_manifest["passed"] or distributed_manifest["feature_version"] != feature_manifest["feature_version"]:
        raise ValueError("Distributed model and features do not match")
    if output.exists():
        raise FileExistsError(output)
    output.mkdir(parents=True)
    current = distributed / "initial.zip"
    history = []
    started = time.perf_counter()
    for index in range(1, distributed_manifest["rounds"] + 1):
        model = output / f"round_{index}.zip"
        worker_metric = output / f"worker_{index}.json"
        TRAIN.java("spark-worker-1", "worker", TRAIN.inside(current), TRAIN.inside(features / "train.tsv"),
                   TRAIN.inside(model), TRAIN.inside(worker_metric), str(distributed_manifest["batch_size"]))
        metric = output / f"validation_{index}.json"
        TRAIN.java("spark-driver", "evaluate", TRAIN.inside(model), TRAIN.inside(features / "validation.tsv"), TRAIN.inside(metric))
        worker = json.loads(worker_metric.read_text(encoding="utf-8"))
        if worker["samples"] != feature_manifest["usable_counts"]["train"] or worker["host"] != "spark-worker-1":
            raise ValueError("Single-worker benchmark did not process the same training set")
        history.append({"round": index, "worker": worker, "validation": json.loads(metric.read_text(encoding="utf-8"))})
        current = model
    seconds = time.perf_counter() - started
    result = {"feature_version": feature_manifest["feature_version"], "same_training_posts": worker["samples"],
              "rounds": distributed_manifest["rounds"], "batch_size": distributed_manifest["batch_size"],
              "single_worker_seconds": seconds, "two_worker_seconds": distributed_manifest["distributed_train_seconds"],
              "single_worker_last_validation_mse": history[-1]["validation"]["mse"],
              "two_worker_last_validation_mse": distributed_manifest["history"][-1]["validation"]["mse"],
              "history": history, "scope": "same feature release and rounds; 1 CPU worker vs 2 CPU workers, warm containers/cache",
              "passed": True}
    (output / "manifest.json").write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    quality = ROOT / "outputs/quality/g05_benchmark.json"
    quality.write_text(json.dumps({**result, "directory": str(output)}, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({k: v for k, v in result.items() if k != "history"}, ensure_ascii=False, indent=2))
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--features", type=Path, required=True)
    parser.add_argument("--distributed", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    run(args.features, args.distributed, args.output)
