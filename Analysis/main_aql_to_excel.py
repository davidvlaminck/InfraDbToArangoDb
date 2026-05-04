#!/usr/bin/env python3
"""PyCharm entrypoint for generic AQL -> Excel export."""

from __future__ import annotations

import datetime as dt
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from Analysis.aql_to_excel import main as run_aql_to_excel_main

SETTINGS_PATH = Path("/home/davidlinux/Documenten/AWV/resources/settings_SyncToArangoDB.json")
QUERY_FILE = Path(__file__).with_name("example_aql_to_excel_query.aql")
OUT_PATH = Path(__file__).with_name(f"aql_export_{dt.datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx")
DEBUG_LIMIT: int | None = None


def main() -> int:
    if not SETTINGS_PATH.exists():
        raise FileNotFoundError(
            f"Settings file not found: {SETTINGS_PATH}\n"
            "Edit SETTINGS_PATH in Analysis/main_aql_to_excel.py"
        )
    if not QUERY_FILE.exists():
        raise FileNotFoundError(
            f"Query file not found: {QUERY_FILE}\n"
            "Expected Analysis/example_aql_to_excel_query.aql"
        )

    argv = [
        "--settings",
        str(SETTINGS_PATH),
        "--query-file",
        str(QUERY_FILE),
        "--bind-vars-json",
        '{"limit": 100}',
        "--out",
        str(OUT_PATH),
    ]
    if DEBUG_LIMIT is not None:
        argv.extend(["--limit", str(DEBUG_LIMIT)])

    return run_aql_to_excel_main(argv)


if __name__ == "__main__":
    raise SystemExit(main())
