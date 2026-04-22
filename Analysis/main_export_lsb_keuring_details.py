"""Easy PyCharm entrypoint for the full LSB + ElektrischeKeuring detail export."""
from __future__ import annotations
import datetime as dt
import os
import sys
from pathlib import Path
_repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _repo_root not in sys.path:
    sys.path.insert(0, _repo_root)
from API.APIEnums import Environment
from Analysis.export_lsb_keuring_details import export_rows_to_excel, fetch_rows
from Analysis.export_keuringsinfo import _create_db_from_settings, _load_settings
SETTINGS_PATH = Path("/home/davidlinux/Documenten/AWV/resources/settings_SyncToArangoDB.json")
ENV = Environment.PRD
OUT_PATH = Path(__file__).with_name(f"lsb_keuring_details_{dt.datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx")
DEBUG_LIMIT: int | None = None
MAX_RUNTIME_SECONDS = 600
def main() -> int:
    if not SETTINGS_PATH.exists():
        raise FileNotFoundError(
            f"Settings file not found: {SETTINGS_PATH}\n"
            f"Edit SETTINGS_PATH in Analysis/main_export_lsb_keuring_details.py"
        )
    settings = _load_settings(SETTINGS_PATH)
    db = _create_db_from_settings(settings, env=ENV)
    rows = fetch_rows(
        db,
        max_runtime_seconds=MAX_RUNTIME_SECONDS,
        limit=DEBUG_LIMIT,
    )
    export_rows_to_excel(rows, OUT_PATH)
    unique_assets = len({row.get('uuid') for row in rows})
    print(f"Fetched {len(rows)} export rows for {unique_assets} Laagspanningsborden")
    print(f"Wrote Excel file to {OUT_PATH}")
    return 0
if __name__ == "__main__":
    raise SystemExit(main())
