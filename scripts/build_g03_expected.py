"""Independent stdlib CSV keys for G03 Silver reconciliation."""

from __future__ import annotations

import csv
from pathlib import Path

from intake_vitied import media_from_url, utc_timestamp


ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    output = ROOT / "outputs/quality/g03_expected_observation_keys.csv"
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", encoding="utf-8", newline="") as target:
        writer = csv.writer(target)
        writer.writerow(["post_id", "observed_at_utc"])
        count = 0
        for filename in ("training_data.csv", "streaming_data.csv"):
            with (ROOT / "data/raw/vitied" / filename).open("r", encoding="utf-8-sig", newline="") as source:
                for row in csv.DictReader(source):
                    post_id, _, _ = media_from_url(row["vid_url"])
                    observed = utc_timestamp(row["vid_scrapeTime"])
                    if not post_id or not observed:
                        raise ValueError(f"Invalid observation key in {filename}")
                    writer.writerow([post_id, observed])
                    count += 1
    if count != 34750:
        raise ValueError(f"Unexpected local source count: {count}")
    print(f"{output}: {count} observation keys")


if __name__ == "__main__":
    main()
