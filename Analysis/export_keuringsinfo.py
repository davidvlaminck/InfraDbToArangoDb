"""Export keuringsinfo (LS/LSDeel) from ArangoDB to an Excel workbook.

This script is intentionally placed in Analysis/ as requested.

Assumptions
- Assets have `ins.EMObject_*` fields as described in Analysis/spec.md.
- Assets have `toezichtgroep_key` (8 chars) referring to toezichtgroepen._key.

Behaviour
- LS and LSDeel can be *paired* **only** via the Voedt relation (LS -> LSDeel).
- Output is **one chosen object per row**:
  - If there is a pair: choose LSDeel
  - If no pair exists: choose the singleton itself
- Adds a `type` column (LS/LSDeel) to make it explicit.

Excel output
- Separate sheets per toezichtgroep + "Andere".
- A "Niet meegenomen" sheet for assets that are active but have toestand
  verwijderd/overgedragen.
  - Assets with AIMDBStatus_isActief == false are NEVER considered (not exported,
    not in pivot, not in "Niet meegenomen").
- A "Pivot" sheet with counts per toezichtgroep (plus a total row) as rows and
  keuringsresultaat as columns. Keuringsresultaat only counts when
  datum_laatste_keuring > 2021-01-01; otherwise it's treated as blank.
"""

from __future__ import annotations

import argparse
import datetime as dt
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from API.APIEnums import AuthType, Environment
from ArangoDBConnectionFactory import ArangoDBConnectionFactory


TARGET_SHEETS = {"V&W-WL", "V&W-WA", "V&W-WO", "V&W-WW", "V&W-WVB", "Tunnel Organ. VL."}
EXCLUDED_SHEET = "Niet meegenomen"
PIVOT_SHEET = "Pivot"


@dataclass(frozen=True)
class KeuringsRecord:
    toezichtgroep: str
    type: str  # LS | LSDeel
    match: str
    uuid: str
    naam: str | None
    naampad: str | None
    isActief: bool | None
    toestand: str | None
    datum_laatste_keuring: str | None
    resultaat_keuring: str | None


def _load_settings(path: Path) -> dict[str, Any]:
    import json

    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _create_db_from_settings(settings: dict[str, Any], env: Environment) -> Any:
    db_settings = settings["databases"][str(env.value[0])]
    factory = ArangoDBConnectionFactory(
        db_name=db_settings["database"],
        username=db_settings["user"],
        password=db_settings["password"],
    )
    return factory.create_connection()


def _parse_iso_date(s: str | None) -> dt.date | None:
    if not s:
        return None
    try:
        return dt.date.fromisoformat(s)
    except ValueError:
        return None


def build_aql(
    ls_short_uri: str,
    lsdeel_short_uri: str,
    voedt_short: str = "Voedt",
    *,
    limit: int | None = None,
) -> str:
    """Build AQL to emit one chosen object per row (prefer LSDeel for pairs).

    Matching
    - ONLY via the Voedt edge LS -> LSDeel (OUTBOUND).

    Inclusion
    - Only assets with AIMDBStatus_isActief == true are returned.

    Routing
    - Assets with toestand verwijderd/overgedragen are still returned, but will be
      placed on the "Niet meegenomen" sheet in Python.

    Match ranking: voedt (1) > single (3)
    """

    limit_clause = "" if limit is None else "\nLIMIT @limit"

    return f"""
LET ls_key      = FIRST(FOR at IN assettypes FILTER at.short_uri == @ls_short_uri LIMIT 1 RETURN at._key)
LET lsdeel_key  = FIRST(FOR at IN assettypes FILTER at.short_uri == @lsdeel_short_uri LIMIT 1 RETURN at._key)
LET voedt_key   = FIRST(FOR rt IN relatietypes FILTER rt.short == @voedt_short LIMIT 1 RETURN rt._key)

LET pairs = (
  FOR ls IN assets
    FILTER ls.AIMDBStatus_isActief == true
    FILTER ls.assettype_key == ls_key
    FOR lsdeel, e IN OUTBOUND ls assetrelaties
      FILTER e.relatietype_key == voedt_key
      FILTER lsdeel.AIMDBStatus_isActief == true
      FILTER lsdeel.assettype_key == lsdeel_key
      RETURN {{"ls": ls, "lsdeel": lsdeel, "match": "voedt", "rank": 1}}
)

LET matched_ls_keys = (
  FOR p IN pairs
    COLLECT k = p.ls._key
    RETURN k
)

LET matched_lsdeel_keys = (
  FOR p IN pairs
    COLLECT k = p.lsdeel._key
    RETURN k
)

LET single_ls = (
  FOR ls IN assets
    FILTER ls.AIMDBStatus_isActief == true
    FILTER ls.assettype_key == ls_key
    FILTER ls._key NOT IN matched_ls_keys
    RETURN {{"ls": ls, "lsdeel": null, "match": "single_ls", "rank": 3}}
)

LET single_lsdeel = (
  FOR ld IN assets
    FILTER ld.AIMDBStatus_isActief == true
    FILTER ld.assettype_key == lsdeel_key
    FILTER ld._key NOT IN matched_lsdeel_keys
    RETURN {{"ls": null, "lsdeel": ld, "match": "single_lsdeel", "rank": 3}}
)

LET all_candidates = UNION_DISTINCT(pairs, single_ls, single_lsdeel)

FOR chosen_doc IN (
  FOR c IN all_candidates
    LET chosen = c.lsdeel != null ? c.lsdeel : c.ls
    COLLECT k = chosen._key INTO grouped = c
    LET best = FIRST(
      FOR g IN grouped
        SORT g.rank ASC
        LIMIT 1
        RETURN g
    )
    RETURN best
)
  LET chosen = chosen_doc.lsdeel != null ? chosen_doc.lsdeel : chosen_doc.ls
  LET type = chosen_doc.lsdeel != null ? "LSDeel" : "LS"

  LET tz = FIRST(FOR t IN toezichtgroepen FILTER t._key == chosen.toezichtgroep_key LIMIT 1 RETURN t)
  LET ins = chosen.ins

  RETURN {{
    "toezichtgroep": tz != null ? tz.naam : "UNKNOWN",
    "type": type,
    "match": chosen_doc.match,

    "uuid": chosen._key,
    "naam": chosen.AIMNaamObject_naam,
    "naampad": chosen.NaampadObject_naampad,

    "isActief": chosen.AIMDBStatus_isActief,
    "toestand": chosen.toestand,

    "datum_laatste_keuring": ins != null ? ins.EMObject_datumLaatsteKeuring : null,
    "resultaat_keuring": ins != null ? ins.EMObject_resultaatKeuring : null
  }}{limit_clause}
"""


def fetch_records(
    db: Any,
    ls_short_uri: str,
    lsdeel_short_uri: str,
    *,
    max_runtime_seconds: int = 300,
    limit: int | None = None,
) -> list[KeuringsRecord]:
    aql = build_aql(ls_short_uri=ls_short_uri, lsdeel_short_uri=lsdeel_short_uri, limit=limit)
    bind_vars: dict[str, Any] = {
        "ls_short_uri": ls_short_uri,
        "lsdeel_short_uri": lsdeel_short_uri,
        "voedt_short": "Voedt",
    }
    if limit is not None:
        bind_vars["limit"] = limit

    cursor = db.aql.execute(
        aql,
        bind_vars=bind_vars,
        batch_size=2000,
        ttl=600,
        max_runtime=max_runtime_seconds,
        stream=True,
    )

    return [KeuringsRecord(**row) for row in cursor]


def fetch_records_not_meegenomen(*args: Any, **kwargs: Any) -> list[KeuringsRecord]:
    """Deprecated: kept for backward compatibility, but no longer used."""
    raise NotImplementedError("Not needed anymore: use fetch_records() and route by toestand in Python")


def _sheet_name(toezichtgroep: str | None) -> str:
    if not toezichtgroep:
        return "Andere"
    return toezichtgroep if toezichtgroep in TARGET_SHEETS else "Andere"


def _is_not_included(record: KeuringsRecord) -> bool:
    # In "Niet meegenomen":
    # - ONLY active assets with removed/transferred toestand
    # - Inactive assets are never exported at all
    return (record.toestand or "").lower() in {"verwijderd", "overgedragen"}


def _pivot_result_key(record: KeuringsRecord, *, cutoff: dt.date) -> str | None:
    """Return normalized resultaat for pivot counting, or None if it shouldn't count."""

    d = _parse_iso_date(record.datum_laatste_keuring)
    if d is None or d <= cutoff:
        return None

    r = (record.resultaat_keuring or "").strip()
    return r or None


def _build_pivot(
    records: Iterable[KeuringsRecord],
    *,
    cutoff: dt.date,
    include_not_meegenomen: bool = False,
) -> tuple[list[str], dict[str, Counter[str]]]:
    """Build pivot data.

    Returns:
      - sorted unique result columns
      - mapping toezichtgroep -> Counter(result => count)

    Notes:
    - By default we exclude "Niet meegenomen" from the pivot.
    - Blank/None results (or dates <= cutoff) are ignored for pivot counting.
    """

    counters: dict[str, Counter[str]] = defaultdict(Counter)
    all_results: set[str] = set()

    for r in records:
        if (not include_not_meegenomen) and _is_not_included(r):
            continue

        res = _pivot_result_key(r, cutoff=cutoff)
        if res is None:
            continue

        tg = r.toezichtgroep or "UNKNOWN"
        counters[tg][res] += 1
        all_results.add(res)

    result_cols = sorted(all_results)
    return result_cols, counters


def _write_pivot_sheet(
    wb: Any,
    *,
    records: list[KeuringsRecord],
    cutoff: dt.date,
) -> None:
    """Create/overwrite the Pivot sheet."""

    if PIVOT_SHEET in wb.sheetnames:
        wb.remove(wb[PIVOT_SHEET])

    sh = wb.create_sheet(title=PIVOT_SHEET, index=0)

    cols, counters = _build_pivot(records, cutoff=cutoff, include_not_meegenomen=False)

    header: list[Any] = ["toezichtgroep", *cols, "Totaal"]
    sh.append(header)

    # stable row ordering: target groups (sorted) first, then others alphabetically
    ordered_groups = list(sorted(TARGET_SHEETS))
    other_groups = sorted([g for g in counters.keys() if g not in TARGET_SHEETS])
    row_groups = ordered_groups + other_groups

    # Rows per toezichtsgroep
    grand_total: Counter[str] = Counter()
    for tg in row_groups:
        c = counters.get(tg, Counter())
        row: list[Any] = [tg]
        row_total = 0
        for res in cols:
            v = int(c.get(res, 0))
            row.append(v)
            row_total += v
            grand_total[res] += v
        row.append(row_total)
        sh.append(row)

    # Total row
    total_row: list[Any] = ["Totaal"]
    total_sum = 0
    for res in cols:
        v = int(grand_total.get(res, 0))
        total_row.append(v)
        total_sum += v
    total_row.append(total_sum)
    sh.append(total_row)


def export_to_excel(records: Iterable[KeuringsRecord], out_path: Path) -> None:
    from openpyxl import Workbook

    records_list = list(records)

    wb = Workbook()
    default = wb.active
    wb.remove(default)

    # Pivot sheet first (only counts included records / excludes Niet meegenomen by design)
    _write_pivot_sheet(wb, records=records_list, cutoff=dt.date(2021, 1, 1))

    sheet_names = [*sorted(TARGET_SHEETS), "Andere", EXCLUDED_SHEET]
    sheets = {name: wb.create_sheet(title=name) for name in sheet_names}

    headers = [
        "toezichtgroep",
        "type",
        "match",
        "uuid",
        "naam",
        "naampad",
        "isActief",
        "toestand",
        "datum_laatste_keuring",
        "resultaat_keuring",
    ]

    for sh in sheets.values():
        sh.append(headers)

    for r in records_list:
        if _is_not_included(r):
            sh = sheets[EXCLUDED_SHEET]
        else:
            sh = sheets[_sheet_name(r.toezichtgroep)]

        sh.append(
            [
                r.toezichtgroep,
                r.type,
                r.match,
                r.uuid,
                r.naam,
                r.naampad,
                r.isActief,
                r.toestand,
                r.datum_laatste_keuring,
                r.resultaat_keuring,
            ]
        )

    wb.save(out_path)


def main() -> int:
    parser = argparse.ArgumentParser(description="Export keuringsinfo naar Excel")
    parser.add_argument("--settings", type=Path, required=True)
    parser.add_argument("--env", type=str, default="PRD", choices=[e.name for e in Environment])
    parser.add_argument("--auth", type=str, default="JWT", choices=[a.name for a in AuthType])
    parser.add_argument(
        "--out",
        type=Path,
        default=Path(__file__).with_name(f"keuringsinfo_{dt.datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"),
    )

    parser.add_argument(
        "--ls-short-uri",
        type=str,
        default="lgc:installatie#LS",
        help="assettypes.short_uri for LS",
    )

    parser.add_argument(
        "--lsdeel-short-uri",
        type=str,
        default="lgc:installatie#LSDeel",
        help="assettypes.short_uri for LSDeel",
    )

    parser.add_argument("--limit", type=int, default=None, help="Optional cap rows (debug)")

    args = parser.parse_args()

    settings = _load_settings(args.settings)
    db = _create_db_from_settings(settings, env=Environment[args.env])

    records = fetch_records(
        db,
        ls_short_uri=args.ls_short_uri,
        lsdeel_short_uri=args.lsdeel_short_uri,
        max_runtime_seconds=300,
        limit=args.limit,
    )

    export_to_excel(records, args.out)
    print(f"Wrote {len(records)} rows to {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
