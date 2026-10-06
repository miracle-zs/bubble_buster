#!/usr/bin/env python3
"""Build a compact six-path acc04 equity-curve visualization."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


SERIES_ORDER = [
    "actual",
    "second",
    "bullbear",
    "bullbear3",
    "bullbear_reentry",
    "bullbear3_reentry",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build the acc04 all-strategy equity visual")
    parser.add_argument(
        "--input-json",
        default="reports/acc04-independent-signal-reentry-replay-20260902.json",
    )
    parser.add_argument(
        "--template",
        default="scripts/acc04-all-equity-curves.template.html",
    )
    parser.add_argument(
        "--output-html",
        default="reports/acc04-all-strategy-equity-curves.html",
    )
    return parser.parse_args()


def compact_payload(data: dict[str, object]) -> dict[str, object]:
    source_meta = data["meta"]
    assert isinstance(source_meta, dict)
    compact_series: dict[str, object] = {}
    source_series = data["series"]
    assert isinstance(source_series, dict)
    for key in SERIES_ORDER:
        source = source_series[key]
        assert isinstance(source, dict)
        compact_series[key] = {
            "name": source["name"],
            "label": source["label"],
            "points": source["points"],
            "metrics": source["metrics"],
            "chart_metrics": source["chart_metrics"],
            "portfolio_events": source.get("portfolio_events", []),
        }
    return {
        "meta": {
            "title": "acc04 六版本独立重算权益曲线",
            "chart_start_local": source_meta["chart_start_local"],
            "chart_end_local": source_meta["chart_end_local"],
            "simulation_start_utc": source_meta["simulation_start_utc"],
            "simulation_end_utc": source_meta["simulation_end_utc"],
            "starting_equity": source_meta["starting_equity"],
            "portfolio_loss_cut_pct": source_meta["portfolio_loss_cut_pct"],
        },
        "series": compact_series,
    }


def main() -> int:
    args = parse_args()
    data = json.loads(Path(args.input_json).read_text(encoding="utf-8"))
    serialized = json.dumps(compact_payload(data), ensure_ascii=False, separators=(",", ":"))
    template = Path(args.template).read_text(encoding="utf-8")
    if "__DATA_JSON__" not in template:
        raise RuntimeError("visual template is missing __DATA_JSON__ placeholder")
    output = template.replace("__DATA_JSON__", serialized)
    destination = Path(args.output_html)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(output, encoding="utf-8")
    print(f"wrote {destination} ({destination.stat().st_size} bytes)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
