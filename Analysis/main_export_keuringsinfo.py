"""Easy PyCharm entrypoint for the keuringsinfo export.

How to use in PyCharm
1) Open this file.
2) Edit SETTINGS_PATH below to point to your real settings json.
3) Optionally edit LS_SHORT_URI if needed.
4) Run this file.

It will generate an Excel file in the same folder as this script.

Note
- This file is intentionally kept in Analysis/ (as requested earlier).
- It reuses the logic from `Analysis/export_keuringsinfo.py`.
"""

from __future__ import annotations

import datetime as dt
from collections import Counter
from pathlib import Path

from Analysis.export_keuringsinfo import export_to_excel, fetch_records, _load_settings, _create_db_from_settings
from API.APIEnums import Environment


# --- Configure these ---
SETTINGS_PATH = Path("/home/davidlinux/Documenten/AWV/resources/settings_SyncToArangoDB.json")
ENV = Environment.PRD

# LSDeel is fairly stable in this project; LS short-uri differs per dataset.
LS_SHORT_URI = "lgc:installatie#LS"  # you said you updated this already
LSDEEL_SHORT_URI = "lgc:installatie#LSDeel"

# Output file
OUT_PATH = Path(__file__).with_name(f"keuringsinfo_{dt.datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx")

# For first run / debugging you can cap the amount of rows to avoid long runtimes.
DEBUG_LIMIT: int | None = None  # e.g. 500
MAX_RUNTIME_SECONDS = 600


def _assert_assettype_exists(db, short_uri: str) -> None:
    """Fail fast if the assettype short_uri isn't present."""
    found = db.aql.execute(
        "FOR at IN assettypes FILTER at.short_uri == @s LIMIT 1 RETURN at._key",
        bind_vars={"s": short_uri},
    )
    if next(iter(found), None) is None:
        # show a few close candidates
        candidates = list(
            db.aql.execute(
                "FOR at IN assettypes FILTER CONTAINS(LOWER(at.short_uri), LOWER(@needle)) LIMIT 10 RETURN at.short_uri",
                bind_vars={"needle": short_uri.split('#')[-1]},
            )
        )
        raise ValueError(
            f"assettypes.short_uri not found: {short_uri}\n"
            f"Candidates: {candidates}"
        )


def main() -> int:
    if not SETTINGS_PATH.exists():
        raise FileNotFoundError(
            f"Settings file not found: {SETTINGS_PATH}\n"
            f"Edit SETTINGS_PATH in Analysis/main_export_keuringsinfo.py"
        )

    settings = _load_settings(SETTINGS_PATH)
    db = _create_db_from_settings(settings, env=ENV)

    # Validate that short_uri values exist in this DB.
    _assert_assettype_exists(db, LS_SHORT_URI)
    _assert_assettype_exists(db, LSDEEL_SHORT_URI)

    records = fetch_records(
        db,
        ls_short_uri=LS_SHORT_URI,
        lsdeel_short_uri=LSDEEL_SHORT_URI,
        max_runtime_seconds=MAX_RUNTIME_SECONDS,
        limit=DEBUG_LIMIT,
    )

    # Quick sanity output in console
    print(f"Fetched {len(records)} records")
    print("type counts:", dict(Counter(r.type for r in records)))
    print("match counts:", dict(Counter(r.match for r in records)))
    print("sample rows:")
    for r in records[:10]:
        print(
            {
                "type": r.type,
                "match": r.match,
                "uuid": r.uuid,
                "naam": r.naam,
                "naampad": r.naampad,
                "isActief": r.isActief,
                "toestand": r.toestand,
                "datum": r.datum_laatste_keuring,
                "resultaat": r.resultaat_keuring,
            }
        )

    export_to_excel(records, OUT_PATH)
    print(f"Wrote {len(records)} rows to {OUT_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
