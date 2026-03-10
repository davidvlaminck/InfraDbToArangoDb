"""CLI runner for tree analysis.

Produces `tree_structures.json` and `tree_instances.json` in an output dir.
Supports --mock-csv and --assettypes-csv to run without ArangoDB by reading CSV files.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

try:
    from ArangoDBConnectionFactory import ArangoDBConnectionFactory
    from API.APIEnums import Environment
except Exception:
    ArangoDBConnectionFactory = None  # type: ignore
    Environment = None  # type: ignore

from tree_analysis import build_assettype_map, build_structures_and_instances


SETTINGS_PATH = Path("/home/davidlinux/Documenten/AWV/resources/settings_SyncToArangoDB.json")
DEFAULT_OUT = Path(__file__).parent / "output"


def _load_settings(path: Path) -> dict[str, Any]:
    import json

    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _create_db_from_settings(settings: dict[str, Any], env: Any):
    db_settings = settings["databases"][str(env.value)]
    factory = ArangoDBConnectionFactory(
        db_name=db_settings["database"],
        username=db_settings["user"],
        password=db_settings["password"],
    )
    return factory.create_connection()


def _read_assets_from_csv(path: Path) -> list[dict]:
    import csv, ast

    rows = []
    if not path.exists():
        raise SystemExit(f"Mock CSV not found: {path}")
    with path.open("r", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            # coerce naampad_parts if present as a JSON/list-like string
            parts = None
            if row.get("naampad_parts"):
                raw = row["naampad_parts"].strip()
                try:
                    parts = json.loads(raw)
                except Exception:
                    try:
                        parts = ast.literal_eval(raw)
                    except Exception:
                        # fallback: split on comma or | and strip
                        for sep in [",", "|", ";"]:
                            if sep in raw:
                                parts = [p.strip() for p in raw.split(sep) if p.strip()]
                                break
                        else:
                            parts = [raw]
            # minimal asset dict
            asset = {"_key": row.get("_key") or row.get("key")}
            if row.get("assettype_key"):
                asset["assettype_key"] = row.get("assettype_key")
            if parts:
                asset["naampad_parts"] = parts
            rows.append(asset)
    return rows


def _read_assettypes_from_csv(path: Path) -> list[dict]:
    import csv

    rows = []
    if not path.exists():
        raise SystemExit(f"Assettypes CSV not found: {path}")
    with path.open("r", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            # expect columns: _key and short_uri (or short)
            at = {"_key": row.get("_key") or row.get("key")}
            if row.get("short_uri"):
                at["short_uri"] = row.get("short_uri")
            elif row.get("short"):
                at["short"] = row.get("short")
            rows.append(at)
    return rows


def main(argv: list[str] | None = None) -> int:
    import argparse

    argv = argv if argv is not None else sys.argv[1:]
    p = argparse.ArgumentParser()
    p.add_argument("--out-dir", default=DEFAULT_OUT)
    p.add_argument("--mock-csv", default=None, help="CSV file with assets (must contain _key, naampad_parts, assettype_key)")
    p.add_argument("--assettypes-csv", default=None, help="CSV file with assettypes (must contain _key and short_uri)")
    p.add_argument("--debug-beheer", default=None, help="Write debug JSON for a specific beheerobject and exit")
    p.add_argument("--env", default="prd")
    p.add_argument("--lsdeel-short-uri", default="lgc:installatie#LSDeel")
    args = p.parse_args(argv)

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    assettype_map = {}
    db = None

    if args.mock_csv:
        assets = _read_assets_from_csv(Path(args.mock_csv))
        # read assettypes csv if provided
        if args.assettypes_csv:
            ats = _read_assettypes_from_csv(Path(args.assettypes_csv))
            assettype_map = build_assettype_map(ats)
        else:
            assettype_map = {}
    else:
        if not SETTINGS_PATH.exists():
            print("Settings file not found and not in mock mode")
            return 2
        settings = _load_settings(SETTINGS_PATH)
        db = _create_db_from_settings(settings, env=Environment.PRD)
        # fetch assettypes
        try:
            assettypes_cursor = db.collection("assettypes").all()
            assettypes = list(assettypes_cursor)
        except Exception:
            assettypes = []
        assettype_map = build_assettype_map(assettypes)

        # stream assets with naampad_parts
        AQL = """
        FOR a IN assets
          FILTER HAS(a, "naampad_parts") && IS_ARRAY(a.naampad_parts) && LENGTH(a.naampad_parts) > 0
          FILTER (NOT HAS(a, "AIMDBStatus_isActief") OR a.AIMDBStatus_isActief == true)
          RETURN { _key: a._key, assettype_key: a.assettype_key, naampad_parts: a.naampad_parts, AIMDBStatus_isActief: a.AIMDBStatus_isActief }
        """
        try:
            cursor = db.aql.execute(AQL, batch_size=2000, ttl=600, stream=True)
            assets = list(cursor)
        except Exception as e:
            print("Error executing AQL:", e)
            return 3

    # If debug-beheer is requested, produce debug info and exit
    if args.debug_beheer:
        beheer = args.debug_beheer
        debug_out = out_dir / f"debug_beheer_{beheer}.json"
        debug_rows = []
        if args.mock_csv:
            # scan mock assets and also include assets that might be missing naampad_parts
            for a in assets:
                parts = a.get("naampad_parts") or []
                first = parts[0] if parts else None
                is_active = True
                if "AIMDBStatus_isActief" in a:
                    v = a.get("AIMDBStatus_isActief")
                    is_active = (v.lower() == "true") if isinstance(v, str) else bool(v)
                debug_rows.append(
                    {
                        "_key": a["_key"],
                        "assettype_key": a.get("assettype_key"),
                        "beheerobject": beheer,
                        "first_naampad_part": first,
                        "is_active": is_active,
                    }
                )
        else:
            # query active beheerobject with matching short_uri
            AQL = """
            FOR b IN beheerobject
              FILTER b.short_uri == @beheer
              RETURN b
            """
            try:
                cursor = db.aql.execute(AQL, bind_vars={"beheer": beheer}, batch_size=2000, ttl=600, stream=True)
                beheer_objects = list(cursor)
            except Exception as e:
                print("Error executing AQL:", e)
                return 3
            # enrich with asset info
            for b in beheer_objects:
                # find latest asset with this beheerobject as top part
                parts = b.get("naampad_parts") or []
                if not parts:
                    continue
                top_part = parts[0]
                asset = next((a for a in assets if a["_key"] == top_part), None)
                debug_rows.append(
                    {
                        "_key": b["_key"],
                        "assettype_key": asset.get("assettype_key") if asset else None,
                        "beheerobject": beheer,
                        "first_naampad_part": top_part,
                        "is_active": asset.get("AIMDBStatus_isActief") if asset else None,
                    }
                )
        # write debug output
        with debug_out.open("w", encoding="utf-8") as f:
            json.dump(debug_rows, f, ensure_ascii=False, indent=2)
        print(f"Wrote debug info to {debug_out}")
        return 0

    # regular run: build structures and instances
    try:
        structures, instances = build_structures_and_instances(assets, assettype_map, args.lsdeel_short_uri)
    except Exception as e:
        print("Error building structures and instances:", e)
        return 4

    # Annotate structures with count (total assets) and occurrence (# unique beheerobjects)
    assets_by_id: dict[str, int] = {}
    occurrence_by_id: dict[str, int] = {}
    for beheer, inst in instances.items():
        sid = inst.get("structure_id")
        if not sid:
            continue
        try:
            na = int(inst.get("num_assets", 0) or 0)
        except Exception:
            na = 0
        assets_by_id[sid] = assets_by_id.get(sid, 0) + na
        occurrence_by_id[sid] = occurrence_by_id.get(sid, 0) + 1

    # structures is a dict id -> struct
    structures_list = []
    for sid, s in structures.items():
        # ensure label string
        if s.get("label") is None:
            s["label"] = ""
        s["count"] = int(assets_by_id.get(sid, 0))
        s["occurrence"] = int(occurrence_by_id.get(sid, 0))
        structures_list.append(s)

    structures_list = sorted(structures_list, key=lambda x: x.get("count", 0), reverse=True)

    # write output files
    try:
        with (out_dir / "tree_structures.json").open("w", encoding="utf-8") as f:
            json.dump(structures_list, f, ensure_ascii=False, indent=2)
        with (out_dir / "tree_instances.json").open("w", encoding="utf-8") as f:
            json.dump(instances, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print("Error writing output files:", e)
        return 5

    print(f"Output written to {out_dir}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
