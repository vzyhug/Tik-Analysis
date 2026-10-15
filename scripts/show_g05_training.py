"""Render saved G05 training manifests as a readable terminal demo."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]


def render(report_path: Path) -> str:
    report = json.loads(report_path.read_text(encoding="utf-8"))
    lines = [
        "G05 TRAINING HISTORY - RECORDED RUN (read from saved JSON; this command does not train)",
        f"Model: {report['model_version']}" + (f"  parent={report['parent_model_version']}" if report.get("parent_model_version") else ""),
        f"Dataset: {report['dataset_version']}  features={report['feature_version']}",
        f"Architecture: {report['architecture']}",
        f"Method: {report['training']}",
        f"seed={report['seed']}  batch_size={report['batch_size']}  rounds={report['rounds']}  elapsed_s={report['distributed_train_seconds']:.1f}",
        f"Initial validation MSE: {report['initial_validation']['mse']:.8f}",
        "",
    ]
    for item in report["history"]:
        lines.append(f"Round {item['round']}/{report['rounds']}  validation_mse={item['validation']['mse']:.8f}")
        for worker in item["workers"]:
            lines.append(
                f"  {worker['host']}: samples={worker['samples']} batches={worker['batches']} "
                f"parameter_delta_l2={worker['parameter_delta_l2']:.6f}"
            )
    test = report["test"]
    lines.extend([
        "",
        f"Best checkpoint: round {report['best_round']}  validation_mse={report['best_validation_mse']:.8f}",
        f"Held-out test MSE: {test['mse']:.8f}  zero-baseline MSE={test['zero_mse']:.8f}",
        f"Model SHA-256: {report['best_model_sha256']}",
        f"Execution status: {'PASSED' if report['passed'] else 'FAILED'}",
        "Interpretation: reconstruction/embedding run completed; this is not a reviewed Topic classifier.",
        f"Source report: {report_path}",
    ])
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("report", type=Path, help="Saved G05 training report JSON")
    options = parser.parse_args()
    report = options.report if options.report.is_absolute() else ROOT / options.report
    if not report.is_file():
        raise SystemExit(f"Training report not found: {report}")
    print(render(report))


if __name__ == "__main__":
    main()
