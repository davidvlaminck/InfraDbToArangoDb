"""Combined keuringsinfo export: all 4 asset types in a single Excel workbook.

Combines the following assettypes into one .xlsx file:
  - onderdeel#ALSB
  - onderdeel#ForfaitaireAansluiting
  - onderdeel#HSCabine
  - onderdeel#Laagspanningsbord

The output workbook contains:
  - One data sheet per toezichtgroep (+ "Andere" and "Niet meegenomen") with all
    asset types mixed; each data row includes a `typeURI` column (column A).
  - 4 pivot tables (2 result, 2 actions) where each row is prefixed with the
    assettype, so you can see the breakdown per assettype per toezichtgroep,
    including per-assettype subtotals and a grand total.
"""

from __future__ import annotations

import datetime as dt
import json
import os
import re
import sys
from collections import Counter, defaultdict
from collections.abc import Iterable
from pathlib import Path
from typing import Any

from openpyxl import Workbook
from openpyxl.styles import Font
from openpyxl.utils import get_column_letter

from Analysis.export_keuringsinfo import (  # noqa: E402
    AGENT_TO_SHEET,
    EMINFRA_HYPERLINK_BASE,
    EXCLUDED_SHEET,
    PIVOT_ACTIES_ALL_SHEET,
    PIVOT_ACTIES_SHEET,
    PIVOT_RESULTAAT_ALL_SHEET,
    PIVOT_RESULTAAT_SHEET,
    TARGET_SHEETS,
    KeuringsRecord,
    _autofit_sheet_columns,
    _create_db_from_settings,
    _is_not_included,
    _load_settings,
    _load_technique_map,
    _parse_iso_date,
    _resolved_toezichtgroep,
    _sheet_name,
    fetch_records,
)
from API.APIEnums import Environment  # noqa: E402

# Ensure repository root is on sys.path so imports work when run directly in PyCharm.
_repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _repo_root not in sys.path:
    sys.path.insert(0, _repo_root)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

SETTINGS_PATH = Path("C:/resources/settings_SyncToArangoDB.json")
ENV = Environment.PRD
DEBUG_LIMIT: int | None = None  # e.g. 500 for a quick test run
MAX_RUNTIME_SECONDS = 600

# Each assettype: short_uri, human label, validity period (years), filename slug.
ASSET_TYPES: list[dict[str, Any]] = [
    {
        "short_uri": "onderdeel#ALSB",
        "label": "ALSB",
        "validity_period": 5,
        "filename": "ALSB",
    },
    {
        "short_uri": "onderdeel#ForfaitaireAansluiting",
        "label": "ForfaitaireAansluiting",
        "validity_period": 5,
        "filename": "ForfaitaireAansluiting",
    },
    {
        "short_uri": "onderdeel#HSCabine",
        "label": "HSCabine",
        "validity_period": 1,
        "filename": "HSCabine",
    },
    {
        "short_uri": "onderdeel#Laagspanningsbord",
        "label": "Laagspanningsbord",
        "validity_period": 5,
        "filename": "Laagspanningsbord",
    },
]

VALIDITY_PERIOD_BY_TYPE: dict[str, int] = {
    cfg["short_uri"]: cfg["validity_period"] for cfg in ASSET_TYPES
}

OUT_PATH = Path(__file__).with_name(
    f"keuringsinfo_combined_{dt.datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
)

TODAY = dt.date.today()
CUTOFF_BY_TYPE: dict[str, dt.date] = {
    cfg["short_uri"]: TODAY - dt.timedelta(days=cfg["validity_period"] * 365)
    for cfg in ASSET_TYPES
}

# ---------------------------------------------------------------------------
# Pivot column definitions
# ---------------------------------------------------------------------------

# Result-mode pivot columns (flat, no validity-period suffix).
RESULT_COLS = [
    "conform",
    "conform met opmerkingen",
    "inbreuken (niet-conform)",
    "vervallen keuring, conform",
    "vervallen keuring, conform met opmerkingen",
    "vervallen keuring, inbreuken (niet conform)",
    "geen keuring",
]

# Actions-mode pivot columns.
ACTIONS_COLS = [
    "geen actie",
    "toevoegen keuringsverslag",
    "controleer installatie",
    "extraheer keuringsinfo",
]


def _strip_vp_suffix(label: str) -> str:
    """Remove the trailing validity-period suffix like ' (<5 j)' or ' (>1 j)'."""
    return re.sub(r" \((<|>)\d+ j\)$", "", label)


# ---------------------------------------------------------------------------
# Pivot key helpers (per-assettype validity period / cutoff)
# ---------------------------------------------------------------------------

def _pivot_result_key_for_record(record: KeuringsRecord) -> str:
    """Pivot result category using this record's assettype validity period."""
    type_uri = getattr(record, "type_uri", "")
    vp = VALIDITY_PERIOD_BY_TYPE.get(type_uri, 5)
    cutoff = CUTOFF_BY_TYPE.get(type_uri, TODAY - dt.timedelta(days=5 * 365))
    return _pivot_result_key(record, cutoff=cutoff, validity_period=vp)


def _pivot_result_key(
    record: KeuringsRecord, *, cutoff: dt.date, validity_period: int
) -> str:
    """Return the pivot category label for this record.

    Same logic as the single-asset-type exporter, parameterised by cutoff /
    validity_period so each assettype can use its own values.
    """
    d = _parse_iso_date(record.datum_laatste_keuring)
    r = record.resultaat_keuring
    r_norm = r.strip().lower() if r else None

    def _is_not_conform(s: str) -> bool:
        if "niet-conform" in s or "niet conform" in s:
            return True
        if s.startswith("niet-"):
            return True
        if "niet" in s and "conform" in s:
            return True
        return bool("inbreuk" in s or "inbreuken" in s)

    def _is_conform(s: str) -> bool:
        return ("conform" in s) and (not _is_not_conform(s))

    def _has_opmerking(s: str) -> bool:
        return "opmerking" in s or "opmerk" in s

    if d is None:
        return "geen keuring"

    if r_norm and ("niet gekend" in r_norm or r_norm == "geen keuring"):
        r_norm = None

    if d <= cutoff:
        if r_norm:
            if _is_not_conform(r_norm):
                return f"vervallen keuring, inbreuken (niet conform) (>{validity_period} j)"
            if _is_conform(r_norm):
                if _has_opmerking(r_norm):
                    return f"vervallen keuring, conform met opmerkingen (>{validity_period} j)"
                return f"vervallen keuring, conform (>{validity_period} j)"
        return "geen keuring"

    # d > cutoff
    if r_norm:
        if _is_not_conform(r_norm):
            return f"inbreuken (niet-conform) (<{validity_period} j)"
        if _has_opmerking(r_norm):
            return f"conform met opmerkingen (<{validity_period} j)"
        if _is_conform(r_norm):
            return f"conform (<{validity_period} j)"
    return "geen keuring"


def _pivot_actions_key(record: KeuringsRecord) -> str:
    """Return the pivot actions category label for this record."""

    def _has(v: str | None) -> bool:
        if v is None:
            return False
        return str(v).strip() != ""

    has_naam = _has(getattr(record, "naam_keuringsverslag", None))
    has_datum = _has(getattr(record, "datum_laatste_keuring", None))
    has_res = _has(getattr(record, "resultaat_keuring", None))

    if has_naam and has_datum and has_res:
        return "geen actie"
    if (not has_naam) and (has_datum or has_res):
        return "toevoegen keuringsverslag"
    if (not has_naam) and (not has_datum) and (not has_res):
        return "controleer installatie"
    if has_naam and (not has_datum or not has_res):
        return "extraheer keuringsinfo"
    return "controleer installatie"


# ---------------------------------------------------------------------------
# Pivot building with assettype dimension
# ---------------------------------------------------------------------------

def _build_pivot_combined(
    records: Iterable[KeuringsRecord],
    *,
    include_not_meegenomen: bool = False,
    mode: str = "result",
) -> tuple[list[str], dict[tuple[str, str], Counter[str]]]:
    """Build pivot data keyed by (assettype_label, toezichtgroep_group).

    Returns (fixed_column_headers, counters) where counters maps
    (assettype, toezichtgroep) -> Counter[result_category -> count].
    """
    if mode == "result":
        cols = RESULT_COLS
    elif mode == "actions":
        cols = ACTIONS_COLS
    else:
        raise ValueError(f'unknown mode: {mode}')

    counters: dict[tuple[str, str], Counter[str]] = defaultdict(Counter)

    for r in records:
        if _is_not_included(r) and not include_not_meegenomen:
            continue
        if mode == "actions":
            res = _pivot_actions_key(r)
        else:
            res = _strip_vp_suffix(_pivot_result_key_for_record(r))

        assettype = getattr(r, "asset_label", getattr(r, "type_uri", "UNKNOWN"))
        grp = _sheet_name(_resolved_toezichtgroep(r))
        counters[(assettype, grp)][res] += 1

    # Ensure all columns are present in all groups
    for c in counters.values():
        for col in cols:
            c.setdefault(col, 0)

    return cols, counters


def _write_pivot_sheet_combined(
    wb: Any,
    *,
    sheet_name: str,
    records: list[KeuringsRecord],
    include_not_meegenomen: bool,
    mode: str = "result",
) -> None:
    """Create/overwrite a Pivot sheet with assettype as the first column."""

    if sheet_name in wb.sheetnames:
        wb.remove(wb[sheet_name])

    sh = wb.create_sheet(title=sheet_name, index=0)

    cols, counters = _build_pivot_combined(
        records,
        include_not_meegenomen=include_not_meegenomen,
        mode=mode,
    )

    header: list[Any] = ["assettype", "toezichtgroep", *cols, "Totaal"]
    sh.append(header)

    # Auto-filter on the header row so users can filter on assettype etc.
    last_col = get_column_letter(len(header))
    sh.auto_filter.ref = f"A1:{last_col}1"

    # Fixed ordering: for each assettype, target groups (sorted) then Andere.
    assettypes = sorted({k[0] for k in counters})
    row_groups: list[tuple[str, str]] = []
    for at in assettypes:
        for tg in [*sorted(TARGET_SHEETS), "Andere"]:
            row_groups.append((at, tg))

    grand_total: Counter[str] = Counter()
    per_at_total: dict[str, Counter[str]] = defaultdict(Counter)
    per_at_sum: dict[str, int] = defaultdict(int)

    for at, tg in row_groups:
        c = counters.get((at, tg), Counter())
        row: list[Any] = [at, tg]
        row_total = 0
        for res in cols:
            v = int(c.get(res, 0))
            row.append(v)
            row_total += v
            grand_total[res] += v
            per_at_total[at][res] += v
            per_at_sum[at] += v
        row.append(row_total)
        sh.append(row)

    # Sub-total row per assettype
    for at in assettypes:
        sub_row: list[Any] = [at, "Totaal"]
        at_sum = 0
        for res in cols:
            v = int(per_at_total[at].get(res, 0))
            sub_row.append(v)
            at_sum += v
        sub_row.append(per_at_sum[at])
        sh.append(sub_row)

    # Grand total row
    total_row: list[Any] = ["Totaal", "Totaal"]
    total_sum = 0
    for res in cols:
        v = int(grand_total.get(res, 0))
        total_row.append(v)
        total_sum += v
    total_row.append(total_sum)
    sh.append(total_row)


# ---------------------------------------------------------------------------
# Data sheet export with typeURI column
# ---------------------------------------------------------------------------

HEADERS = [
    "typeURI",
    "uuid",
    "naam",
    "naampad",
    "techniek",
    "actief_bestek(ken)",
    "toezichtgroep",
    "toezichter",
    "schadebeheerder",
    "isActief",
    "toestand",
    "naam_keuringsverslag",
    "datum_laatste_keuring",
    "resultaat_keuring",
    "pivot_resultaat_categorie",
    "pivot_acties_categorie",
    "longitude",
    "latitude",
]

UUID_COL = 2  # 1-based index of "uuid" in HEADERS (typeURI is col 1)


def _sanitize(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, (int, float, str, bool)):
        return v
    if isinstance(v, (list, tuple)):
        if all(isinstance(x, (int, float)) for x in v) and len(v) >= 1:
            return v[0]
        return json.dumps(v, ensure_ascii=False)
    if isinstance(v, dict):
        return json.dumps(v, ensure_ascii=False)
    return str(v)


def export_to_excel_combined(records: Iterable[KeuringsRecord], out_path: Path) -> None:
    """Write the combined workbook: 4 pivot sheets + data sheets with typeURI."""
    records_list = list(records)

    wb = Workbook()
    wb.remove(wb.active)

    # --- Pivot sheets (4) ---
    _write_pivot_sheet_combined(
        wb, sheet_name=PIVOT_ACTIES_ALL_SHEET, records=records_list,
        include_not_meegenomen=True, mode="actions",
    )
    _write_pivot_sheet_combined(
        wb, sheet_name=PIVOT_ACTIES_SHEET, records=records_list,
        include_not_meegenomen=False, mode="actions",
    )
    _write_pivot_sheet_combined(
        wb, sheet_name=PIVOT_RESULTAAT_ALL_SHEET, records=records_list,
        include_not_meegenomen=True, mode="result",
    )
    _write_pivot_sheet_combined(
        wb, sheet_name=PIVOT_RESULTAAT_SHEET, records=records_list,
        include_not_meegenomen=False, mode="result",
    )

    # --- Data sheets ---
    sheet_names = [*sorted(TARGET_SHEETS), "Andere", EXCLUDED_SHEET]
    sheets = {name: wb.create_sheet(title=name) for name in sheet_names}

    for sh in sheets.values():
        sh.append(HEADERS)

        # Set filter on column headers
        last_col = get_column_letter(len(HEADERS))
        sh.auto_filter.ref = f"A1:{last_col}1"

    uuid_col = UUID_COL

    tree_structures_file = Path(__file__).parent / "TreeAnalysis" / "output" / "tree_structures.json"
    techniek_map = _load_technique_map(tree_structures_file)
    migration_file = Path(__file__).parent / "migration_LSDeel.json"
    migration_map: dict[str, list[str]] = {}
    try:
        if migration_file.exists():
            mig_data = json.loads(migration_file.read_text(encoding="utf-8"))
            if isinstance(mig_data, list):
                for o in mig_data:
                    if not isinstance(o, dict):
                        continue
                    key = o.get("uuid")
                    vals = o.get("migrated_from_uuids") or []
                    if key and isinstance(vals, list):
                        migration_map[str(key)] = [str(v) for v in vals if v]
    except Exception:
        migration_map = {}

    for r in records_list:
        if _is_not_included(r):
            sh = sheets[EXCLUDED_SHEET]
        else:
            resolved_name = _resolved_toezichtgroep(r)
            sheet_candidate = _sheet_name(resolved_name)
            if sheet_candidate == "Andere" or (r.toezichtgroep or "").upper() == "UNKNOWN":
                mapped = None
                if getattr(r, "toezichtgroep_key_raw", None):
                    k = r.toezichtgroep_key_raw
                    if k in AGENT_TO_SHEET:
                        mapped = AGENT_TO_SHEET[k]
                    elif k.lower() in AGENT_TO_SHEET:
                        mapped = AGENT_TO_SHEET[k.lower()]
                if mapped is None and getattr(r, "toezichtgroep_raw", None):
                    raw = (r.toezichtgroep_raw or "").strip()
                    raw_lower = raw.lower()
                    if raw in AGENT_TO_SHEET:
                        mapped = AGENT_TO_SHEET[raw]
                    elif raw_lower in AGENT_TO_SHEET:
                        mapped = AGENT_TO_SHEET[raw_lower]
                    else:
                        for ak, sheet in AGENT_TO_SHEET.items():
                            if ak in raw or ak in (r.toezichtgroep or ""):
                                mapped = sheet
                                break
                if mapped is None:
                    for fld in ("toezichtgroep_agent_key", "toezichtgroep_agent_uuid", "toezichtgroep_agent_name"):
                        v = getattr(r, fld, None)
                        if not v:
                            continue
                        v_str = str(v)
                        if v_str in AGENT_TO_SHEET:
                            mapped = AGENT_TO_SHEET[v_str]
                            break
                        if v_str.lower() in AGENT_TO_SHEET:
                            mapped = AGENT_TO_SHEET[v_str.lower()]
                            break
                if mapped:
                    sh = sheets.get(mapped, sheets["Andere"])
                else:
                    sh = sheets[_sheet_name(resolved_name)]
            else:
                sh = sheets[sheet_candidate]

        # techniek mapping (migration map -> lsb_uuid -> asset uuid fallback)
        techniek = ""
        lsdeel_for_mapping = None
        mig_vals = migration_map.get(str(r.uuid))
        if mig_vals:
            lsdeel_for_mapping = mig_vals[0]
        if lsdeel_for_mapping:
            techniek = techniek_map.get(lsdeel_for_mapping, "")
        if not techniek and getattr(r, "lsb_uuid", None):
            techniek = techniek_map.get(r.lsb_uuid, "")
        if not techniek:
            techniek = techniek_map.get(r.uuid, "")

        resolved_name = _resolved_toezichtgroep(r)
        pivot_result_cat = _pivot_result_key_for_record(r)
        pivot_action_cat = _pivot_actions_key(r)

        actief_bestek = None
        try:
            if getattr(r, "actief_bestekken", None):
                vals = r.actief_bestekken or []
                if isinstance(vals, list) and vals:
                    actief_bestek = "|".join(str(v) for v in vals if v)
        except Exception:
            actief_bestek = None

        type_uri = getattr(r, "type_uri", "")

        row_values = [
            _sanitize(type_uri),
            _sanitize(r.uuid),
            _sanitize(r.naam),
            _sanitize(r.naampad),
            _sanitize(techniek),
            _sanitize(actief_bestek),
            _sanitize(resolved_name if resolved_name is not None else r.toezichtgroep),
            _sanitize(getattr(r, "toezichter_agent_name", None)),
            _sanitize(getattr(r, "schadebeheerder_agent_name", None)),
            _sanitize(r.isActief),
            _sanitize(r.toestand),
            _sanitize(r.naam_keuringsverslag),
            _sanitize(r.datum_laatste_keuring),
            _sanitize(r.resultaat_keuring),
            _sanitize(pivot_result_cat),
            _sanitize(pivot_action_cat),
            _sanitize(r.longitude),
            _sanitize(r.latitude),
        ]
        sh.append(row_values)

        # Hyperlink the uuid cell (column B) to eMINFRA asset page.
        try:
            if uuid_col is not None:
                uuid_cell = sh.cell(row=sh.max_row, column=uuid_col)
                uuid_val = row_values[uuid_col - 1]
                if uuid_val is not None and str(uuid_val).strip() != "":
                    uuid_str = str(uuid_val)
                    uuid_str_escaped = uuid_str.replace('"', '""')
                    uuid_cell.hyperlink = f"{EMINFRA_HYPERLINK_BASE}{uuid_str_escaped}"
                    uuid_cell.font = Font(color="0563C1", underline="single")
        except Exception:
            pass

    for ws in wb.worksheets:
        _autofit_sheet_columns(ws)

    wb.save(out_path)


def _assert_assettype_exists(db, short_uri: str) -> None:
    """Fail fast if the assettype short_uri isn't present."""
    found = db.aql.execute(
        "FOR at IN assettypes FILTER at.short_uri == @s LIMIT 1 RETURN at._key",
        bind_vars={"s": short_uri},
    )
    if next(iter(found), None) is None:
        candidates = list(
            db.aql.execute(
                "FOR at IN assettypes FILTER CONTAINS(LOWER(at.short_uri), LOWER(@needle)) LIMIT 10 RETURN at.short_uri",
                bind_vars={"needle": short_uri.split("#")[-1]},
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
            f"Edit SETTINGS_PATH in Analysis/main_export_keuringsinfo_combined.py"
        )

    settings = _load_settings(SETTINGS_PATH)
    db = _create_db_from_settings(settings, env=ENV)

    all_records: list[KeuringsRecord] = []

    for cfg in ASSET_TYPES:
        short_uri = cfg["short_uri"]
        _assert_assettype_exists(db, short_uri)

        records = fetch_records(
            db,
            asset_short_uri=short_uri,
            max_runtime_seconds=MAX_RUNTIME_SECONDS,
            limit=DEBUG_LIMIT,
        )

        # Tag each record with its assettype so the combined export can use it.
        # (KeuringsRecord is a frozen dataclass, so use object.__setattr__.)
        for r in records:
            object.__setattr__(r, "type_uri", short_uri)
            object.__setattr__(r, "asset_label", cfg["label"])

        all_records.extend(records)
        print(f"Fetched {len(records)} records for {short_uri} (label={cfg['label']})")

    print(f"Total records: {len(all_records)}")

    export_to_excel_combined(all_records, OUT_PATH)
    print(f"Wrote {len(all_records)} rows to {OUT_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())