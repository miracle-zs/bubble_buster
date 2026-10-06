#!/usr/bin/env python3
"""Build the inline acc04 independent-replay visualization."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build acc04 independent strategy equity visual")
    parser.add_argument(
        "--input-json",
        default="reports/acc04-independent-strategy-replay-20260902.json",
    )
    parser.add_argument(
        "--template",
        default="scripts/acc04-independent-strategy-equity.template.html",
    )
    parser.add_argument(
        "--output-html",
        default="reports/acc04-independent-strategy-equity.html",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    data = json.loads(Path(args.input_json).read_text(encoding="utf-8"))
    serialized = json.dumps(data, ensure_ascii=False, separators=(",", ":"))
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
