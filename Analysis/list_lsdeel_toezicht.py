"""List all LSDeel assets with toezichter and toezichtgroep.

Outputs a CSV-style table to stdout and writes a CSV file next to this script.

Uses the same settings path pattern as other Analysis entrypoints.
"""
from __future__ import annotations

import csv
import datetime as dt
from pathlib import Path
from typing import Any

from API.APIEnums import Environment, AuthType
from ArangoDBConnectionFactory import ArangoDBConnectionFactory

SETTINGS_PATH = Path("/home/davidlinux/Documenten/AWV/resources/settings_SyncToArangoDB.json")
ENV = Environment.PRD
OUT_CSV = Path(__file__).with_name(f"lsdeel_toezicht_{dt.datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")

AQL = """
LET lsdeel_key = FIRST(FOR at IN assettypes FILTER at.short_uri == @lsdeel_short_uri LIMIT 1 RETURN at._key)

FOR a IN assets
  FILTER a.AIMDBStatus_isActief == true
  FILTER a.assettype_key == lsdeel_key
  LET tz_group = (a.toezichtgroep_key != null ? FIRST(FOR t IN toezichtgroepen FILTER t._key == a.toezichtgroep_key LIMIT 1 RETURN t) : null)
  LET tz_obj = a.tz
  RETURN {
    _key: a._key,
    naam: a.AIMNaamObject_naam,
    naampad: a.NaampadObject_naampad,
    toezichtgroep_key: a.toezichtgroep_key,
    toezichtgroep_naam: tz_group != null ? tz_group.naam : null,
    toezichter_key: a.toezichter_key,
    toezichter_name_from_tz: tz_obj != null && tz_obj.Toezicht_toezichter != null ? tz_obj.Toezicht_toezichter.DtcToezichter_naam : null,
    toezichter_email_from_tz: tz_obj != null && tz_obj.Toezicht_toezichter != null ? tz_obj.Toezicht_toezichter.DtcToezichter_email : null,
    longitude: a.geometry != null ? (LENGTH(a.geometry.coordinates) > 0 ? a.geometry.coordinates[0] : null) : null,
    latitude: a.geometry != null ? (LENGTH(a.geometry.coordinates) > 1 ? a.geometry.coordinates[1] : null) : null
  }
"""


def _load_settings(path: Path) -> dict[str, Any]:
    import json

    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _create_db_from_settings(settings: dict[str, Any], env: Environment) -> Any:
    db_settings = settings["databases"][str(env.value)]
    factory = ArangoDBConnectionFactory(
        db_name=db_settings["database"],
        username=db_settings["user"],
        password=db_settings["password"],
    )
    return factory.create_connection()


def main() -> int:
    if not SETTINGS_PATH.exists():
        print(f"Settings file not found: {SETTINGS_PATH}")
        return 2

    settings = _load_settings(SETTINGS_PATH)
    db = _create_db_from_settings(settings, env=ENV)

    cursor = db.aql.execute(AQL, bind_vars={"lsdeel_short_uri": "lgc:installatie#LSDeel"}, batch_size=2000, ttl=600, stream=True)

    rows = list(cursor)
    print(f"Found {len(rows)} LSDeel assets (AIMDBStatus_isActief==true)")

    # Print header and a few rows to console
    header = [
        "_key",
        "naam",
        "naampad",
        "toezichtgroep_key",
        "toezichtgroep_naam",
        "toezichter_key",
        "toezichter_name_from_tz",
        "toezichter_email_from_tz",
        "longitude",
        "latitude",
    ]

    print(",".join(header))
    for r in rows[:50]:
        print(",".join([str(r.get(h, "")) if r.get(h, None) is not None else "" for h in header]))

    # Write CSV with all rows
    with OUT_CSV.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()
        for r in rows:
            out = {k: r.get(k, None) for k in header}
            writer.writerow(out)

    print(f"Wrote CSV to {OUT_CSV}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

