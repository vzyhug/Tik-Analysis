"""Coordinate two DL4J JVM workers with synchronous weighted model averaging."""

from __future__ import annotations

import argparse
import atexit
import hashlib
import json
import shutil
import subprocess
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MAIN = "vn.bigdata.topic.DistributedAutoencoder"
CLASSPATH = "/workspace/apps/dl4j/target/classes:/workspace/apps/dl4j/target/dependency/*"
LOG_LOCK = threading.Lock()
LOG_STREAM = None


def close_log() -> None:
    global LOG_STREAM
    if LOG_STREAM is not None:
        LOG_STREAM.close()
        LOG_STREAM = None


atexit.register(close_log)


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def inside(path: Path) -> str:
    resolved = path.resolve()
    if ROOT not in resolved.parents:
        raise ValueError(f"Path is outside workspace: {resolved}")
    return "/workspace/" + resolved.relative_to(ROOT).as_posix()


def java(service: str, *arguments: str) -> str:
    command = ["docker", "compose", "-f", "infra/compose.yaml", "exec", "-T", service,
               "env", "OPENBLAS_NUM_THREADS=1", "OMP_NUM_THREADS=1", "java", "-Xmx512m",
               "-Dorg.bytedeco.javacpp.maxphysicalbytes=805306368",
               "-Dorg.nd4j.linalg.defaultbackend=org.nd4j.linalg.cpu.nativecpu.CpuBackend",
               "-cp", CLASSPATH, MAIN, *arguments]
    process = subprocess.Popen(command, cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                               text=True, encoding="utf-8", errors="replace", bufsize=1)
    lines: list[str] = []

    def forward_output() -> None:
        assert process.stdout is not None
        for line in process.stdout:
            line = line.rstrip("\r\n")
            lines.append(line)
            rendered = f"[{service}] {line}"
            with LOG_LOCK:
                print(rendered, flush=True)
                if LOG_STREAM is not None:
                    LOG_STREAM.write(rendered + "\n")
                    LOG_STREAM.flush()

    reader = threading.Thread(target=forward_output, daemon=True)
    reader.start()
    try:
        return_code = process.wait(timeout=600)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()
        reader.join()
        raise TimeoutError(f"{service} DL4J command exceeded 600 seconds")
    reader.join()
    if return_code:
        raise RuntimeError(f"{service} DL4J failed (exit {return_code}):\n" + "\n".join(lines[-80:]))
    return "\n".join(lines).strip()


def run(features: Path, output: Path, rounds: int, batch_size: int,
        initial_model: Path | None = None, log_file: Path | None = None) -> dict:
    global LOG_STREAM
    feature_manifest = json.loads((features / "manifest.json").read_text(encoding="utf-8"))
    if not feature_manifest["passed"] or feature_manifest.get("feature_mode", "caption_hashtag") not in {"caption_hashtag", "caption_only"}:
        raise ValueError("Invalid G05 feature release")
    for name, expected in feature_manifest["checksums"].items():
        if digest(features / name) != expected:
            raise ValueError(f"Feature checksum mismatch: {name}")
    if rounds < 1 or batch_size < 1:
        raise ValueError("Rounds and batch size must be positive")
    if output.exists():
        raise FileExistsError(output)
    quality = ROOT / "outputs/quality" / f"g05_train_{output.name}.json"
    if quality.exists():
        raise FileExistsError(quality)
    if log_file is None:
        log_file = ROOT / "outputs/quality" / f"g05_train_{output.name}.console.txt"
    elif not log_file.is_absolute():
        log_file = ROOT / log_file
    if log_file.exists():
        raise FileExistsError(log_file)
    log_file.parent.mkdir(parents=True, exist_ok=True)
    LOG_STREAM = log_file.open("x", encoding="utf-8", buffering=1)
    print(f"live_worker_logs={log_file.relative_to(ROOT).as_posix()}", flush=True)
    stage = Path(str(output) + ".__staging__")
    stage.mkdir(parents=True, exist_ok=False)
    initial = stage / "initial.zip"
    parent_version = None
    if initial_model is None:
        java("spark-driver", "init", str(feature_manifest["input_dimension"]), "42", inside(initial))
    else:
        parent = json.loads((initial_model.parent / "manifest.json").read_text(encoding="utf-8"))
        if parent["feature_version"] != feature_manifest["feature_version"] or digest(initial_model) != parent["best_model_sha256"]:
            raise ValueError("Continuation checkpoint does not match feature release or checksum")
        shutil.copyfile(initial_model, initial)
        parent_version = parent["model_version"]
    initial_metric = stage / "validation_initial.json"
    java("spark-driver", "evaluate", inside(initial), inside(features / "validation.tsv"), inside(initial_metric))
    best_loss = json.loads(initial_metric.read_text(encoding="utf-8"))["mse"]
    best_round = 0
    best = stage / "best.zip"
    shutil.copyfile(initial, best)
    current = initial
    history = []
    training_started = time.perf_counter()
    for round_index in range(1, rounds + 1):
        workers = []
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = []
            for worker_index in (1, 2):
                model = stage / f"round_{round_index}_worker_{worker_index}.zip"
                metric = stage / f"round_{round_index}_worker_{worker_index}.json"
                shard = features / f"train_worker_{worker_index}.tsv"
                futures.append(pool.submit(java, f"spark-worker-{worker_index}", "worker",
                                           inside(current), inside(shard), inside(model), inside(metric),
                                           str(batch_size), str(round_index)))
                workers.append((model, metric))
            for future in futures:
                future.result()
        worker_metrics = [json.loads(metric.read_text(encoding="utf-8")) for _, metric in workers]
        if {item["host"] for item in worker_metrics} != {"spark-worker-1", "spark-worker-2"}:
            raise ValueError("DL4J updates were not witnessed on both workers")
        if sum(item["samples"] for item in worker_metrics) != feature_manifest["usable_counts"]["train"]:
            raise ValueError("Worker sample count mismatch")
        if any(item["parameter_delta_l2"] <= 0 for item in worker_metrics):
            raise ValueError("A worker did not update model parameters")
        averaged = stage / f"round_{round_index}.zip"
        java("spark-driver", "average", inside(workers[0][0]), str(worker_metrics[0]["samples"]),
             inside(workers[1][0]), str(worker_metrics[1]["samples"]), inside(averaged))
        metric = stage / f"validation_round_{round_index}.json"
        java("spark-driver", "evaluate", inside(averaged), inside(features / "validation.tsv"), inside(metric))
        validation = json.loads(metric.read_text(encoding="utf-8"))
        history.append({"round": round_index, "workers": worker_metrics,
                        "validation": validation, "model_sha256": digest(averaged)})
        print(f"round={round_index} validation_mse={validation['mse']:.8f} hosts={[item['host'] for item in worker_metrics]}", flush=True)
        if validation["mse"] < best_loss:
            best_loss = validation["mse"]
            best_round = round_index
            shutil.copyfile(averaged, best)
        current = averaged
    training_seconds = time.perf_counter() - training_started
    test_file = stage / "test_metrics.json"
    java("spark-driver", "evaluate", inside(best), inside(features / "test.tsv"), inside(test_file))
    for split in ("train", "validation", "test"):
        java("spark-driver", "embed", inside(best), inside(features / f"{split}.tsv"),
             inside(stage / f"{split}_embeddings.tsv"))
    report = {"model_version": output.name, "parent_model_version": parent_version,
              "feature_version": feature_manifest["feature_version"],
              "dataset_version": feature_manifest["dataset_version"], "architecture": "512-128-64-128-512 MSE autoencoder",
              "training": "two container JVMs, Adam 0.003 local epochs, sample-weighted synchronous parameter averaging",
              "seed": 42, "batch_size": batch_size, "rounds": rounds, "history": history,
              "distributed_train_seconds": training_seconds,
              "initial_validation": json.loads(initial_metric.read_text(encoding="utf-8")),
              "best_round": best_round, "best_validation_mse": best_loss,
              "test": json.loads(test_file.read_text(encoding="utf-8")),
              "best_model_sha256": digest(best),
              "embedding_counts": feature_manifest["usable_counts"],
              "created_at_utc": datetime.now(timezone.utc).isoformat(),
              "passed": all(len(item["workers"]) == 2 and all(worker["parameter_delta_l2"] > 0 for worker in item["workers"])
                            for item in history) and best_loss <= json.loads(initial_metric.read_text(encoding="utf-8"))["mse"]}
    (stage / "manifest.json").write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    stage.rename(output)
    quality.write_text(json.dumps({**report, "directory": str(output)}, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"train_complete model={output.name} rounds={rounds} best_round={best_round} "
          f"best_validation_mse={best_loss:.8f} test_mse={report['test']['mse']:.8f}", flush=True)
    print(f"training_report={quality.relative_to(ROOT).as_posix()}", flush=True)
    print(f"training_log={log_file.relative_to(ROOT).as_posix()}", flush=True)
    close_log()
    if not report["passed"]:
        raise ValueError("G05 model did not improve validation MSE; inspect model report")
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--features", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--batch-size", type=int, default=64)
    parser.add_argument("--initial-model", type=Path)
    parser.add_argument("--log-file", type=Path, help="Exclusive path for live stdout/stderr from all DL4J JVMs")
    options = parser.parse_args()
    run(options.features, options.output, options.rounds, options.batch_size,
        options.initial_model, options.log_file)
