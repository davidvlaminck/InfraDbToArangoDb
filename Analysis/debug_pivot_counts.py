"""Debug helper: show why pivot counts are low.

Runs the same AQL as the export, then prints distributions:
- counts of records with/without `ins`
- counts of records where pivot result is counted vs ignored
- breakdown of ignored reasons

Usage (example):
    python Analysis/debug_pivot_counts.py --settings settings.json --env PRD
"""

from __future__ import annotations

import argparse
import datetime as dt
from collections import Counter
from pathlib import Path

from Analysis.export_keuringsinfo import (
    _load_settings,
    _create_db_from_settings,
    _parse_iso_date,
    fetch_records,
)
from API.APIEnums import Environment


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--settings", type=Path, required=True)
    parser.add_argument("--env", type=str, default="PRD", choices=[e.name for e in Environment])
    parser.add_argument("--ls-short-uri", type=str, default="lgc:installatie#LS")
    parser.add_argument("--lsdeel-short-uri", type=str, default="lgc:installatie#LSDeel")
    parser.add_argument("--limit", type=int, default=None)
    args = parser.parse_args()

    settings = _load_settings(args.settings)
    db = _create_db_from_settings(settings, env=Environment[args.env])

    recs = fetch_records(
        db,
        ls_short_uri=args.ls_short_uri,
        lsdeel_short_uri=args.lsdeel_short_uri,
        limit=args.limit,
    )

    cutoff = dt.date(2021, 1, 1)

    stats = Counter()
    ignored = Counter()
    res_counter = Counter()

    for r in recs:
        stats["records"] += 1
        stats[f"type:{r.type}"] += 1
        stats[f"match:{r.match}"] += 1

        has_ins = bool(r.datum_laatste_keuring or r.resultaat_keuring)
        stats["has_ins"] += int(has_ins)

        d = _parse_iso_date(r.datum_laatste_keuring)
        if d is None:
            ignored["no_date"] += 1
            continue
        if d <= cutoff:
            ignored["date_le_cutoff"] += 1
            continue

        rr = (r.resultaat_keuring or "").strip()
        if not rr:
            ignored["blank_result"] += 1
            continue

        res_counter[rr] += 1
        stats["counted"] += 1

    print("=== BASIC ===")
    for k, v in stats.most_common():
        print(f"{k}: {v}")

    print("\n=== IGNORED (why not counted in pivot) ===")
    for k, v in ignored.most_common():
        print(f"{k}: {v}")

    print("\n=== COUNTED RESULTS (top 20) ===")
    for k, v in res_counter.most_common(20):
        print(f"{k}: {v}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

