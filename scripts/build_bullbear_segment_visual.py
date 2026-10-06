#!/usr/bin/env python3
"""Inject a compact, downsampled optimization result into the visual template."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def downsample(points: list[dict[str, object]], step: int = 4) -> list[dict[str, object]]:
    if len(points) <= step + 1:
        return points
    selected = points[::step]
    if selected[-1] != points[-1]:
        selected.append(points[-1])
    return [{"t": point["t"], "equity": point["equity"]} for point in selected]


def main() -> int:
    parser = argparse.ArgumentParser(description="Build the bullbear segment optimization visual")
    parser.add_argument("--input-json", default="reports/bullbear-segment-optimization-20260902.json")
    parser.add_argument("--template", default="reports/bullbear-segment-optimization.template.html")
    parser.add_argument("--output-html", default="reports/bullbear-segment-optimization.html")
    args = parser.parse_args()

    source = json.loads(Path(args.input_json).read_text(encoding="utf-8"))
    compact = {
        "meta": {
            key: source["meta"][key]
            for key in (
                "account",
                "chart_start_local",
                "chart_end_local",
                "starting_equity",
                "portfolio_loss_cut_pct",
            )
        },
        "series": [],
    }
    for key, item in sorted(source["series"].items(), key=lambda pair: int(pair[1]["segments"])):
        compact["series"].append(
            {
                "segments": item["segments"],
                "label": item["label"],
                "summary": item["summary"],
                "points": downsample(item["points"]),
            }
        )

    template = Path(args.template).read_text(encoding="utf-8")
    if "__DATA_JSON__" not in template:
        raise RuntimeError("visual template is missing __DATA_JSON__")
    rendered = template.replace(
        "__DATA_JSON__",
        json.dumps(compact, ensure_ascii=False, separators=(",", ":")),
    )
    destination = Path(args.output_html)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(rendered, encoding="utf-8")
    print(f"wrote {destination} ({destination.stat().st_size} bytes)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
