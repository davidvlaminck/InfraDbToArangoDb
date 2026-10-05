#!/usr/bin/env python3
"""
Bouw een uuid -> label mapping vanuit de tree structures en vertaal daarna de
LSDeel-uuid's naar de bijbehorende LSBord-uuid's met behulp van het
GemigreerdNaar-relatiemap.

Stap 1: lees tree_structures.json en bouw { lsdeel_uuid: label } uit het json-array "lsdeel_uuids" en het veld "label" van elke structuur.
        -> mapping_lsdeel_label_202610.json

Stap 2: vervang elke lsdeel_uuid door de lsbord_uuid uit
        mapping_gemigreerdnaar_20260929.json ({"_from": lsdeel, "_to": lsbord}).
        -> mapping_lsbord_label_202610.json

Gebruik:
    python Analysis/TreeAnalysis/lgc_2_otl/mapping_lsdeel_label.py
    python Analysis/TreeAnalysis/lgc_2_otl/mapping_lsdeel_label.py --prefer-pre-migration
    python Analysis/TreeAnalysis/lgc_2_otl/mapping_lsdeel_label.py \
        --tree-structures Analysis/TreeAnalysis/output/tree_structures_original.json \
        --migration-mapping Analysis/TreeAnalysis/lgc_2_otl/mapping_gemigreerdnaar_20260929.json
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# Paths (relative to this script so it can be run from anywhere)
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
TREE_ANALYSIS_DIR = SCRIPT_DIR.parent
OUTPUT_DIR = TREE_ANALYSIS_DIR / "output"

PRE_MIGRATION_TREE_STRUCTURES = OUTPUT_DIR / "tree_structures_original.json"
DEFAULT_TREE_STRUCTURES = PRE_MIGRATION_TREE_STRUCTURES
DEFAULT_MIGRATION_MAPPING = SCRIPT_DIR / "mapping_gemigreerdnaar_20260929.json"

DEFAULT_LSDEEL_OUT = SCRIPT_DIR / "mapping_lsdeel_label_202610.json"
DEFAULT_LSBORD_OUT = SCRIPT_DIR / "mapping_lsbord_label_202610.json"

DEFAULT_UUID_KEY = "lsdeel_uuids"
DEFAULT_LABEL_KEY = "label"

FROM_KEYS = ("_from", "from", "lsdeel_uuid", "old", "source")
TO_KEYS = ("_to", "to", "lsbord_uuid", "new", "target")


# ---------------------------------------------------------------------------
# IO helpers
# ---------------------------------------------------------------------------
def load_json(path: Path) -> Any:
    """Load a JSON file and return the parsed object."""
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def write_json(path: Path, payload: Any, indent: int = 2) -> None:
    """Write a JSON file (deterministic key order for mappings) and add a trailing newline."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=indent, ensure_ascii=False, sort_keys=isinstance(payload, dict))
        fh.write("\n")


# ---------------------------------------------------------------------------
# Normalisation
# ---------------------------------------------------------------------------
def normalize_label(value: Any) -> Any:
    """Normalise a label: None stays None, strings are stripped, '' becomes None."""
    if value is None:
        return None
    if isinstance(value, str):
        stripped = value.strip()
        return stripped or None
    return value


def iter_structures(node: Any):
    """Yield every structure dict, also when the document is a single object."""
    if isinstance(node, list):
        for item in node:
            if isinstance(item, dict):
                yield item
    elif isinstance(node, dict):
        yield node


def extract_label(tree_structure: dict, label_key: str, fallback_keys: tuple[str, ...] = ()) -> Any:
    """Read the label, optionally falling back to other keys."""
    if label_key in tree_structure:
        return normalize_label(tree_structure.get(label_key))
    for key in fallback_keys:
        if key in tree_structure:
            return normalize_label(tree_structure.get(key))
    return None


def build_label_mapping(
    tree_structures: Any,
    uuid_key: str = DEFAULT_UUID_KEY,
    label_key: str = DEFAULT_LABEL_KEY,
    skip_unlabeled: bool = False,
) -> dict[str, Any]:
    """
    Build { uuid: label } from the uuid arrays of every tree structure.

    - uuids are de-duplicated, the file order is preserved for reporting
    - a non-empty label always wins over an empty/None label
    - conflicting non-empty labels are reported and the first one is kept
    """
    mapping: dict[str, Any] = {}
    seen_structures = 0

    for structure in iter_structures(tree_structures):
        seen_structures += 1
        uuids = structure.get(uuid_key)
        label = extract_label(structure, label_key, fallback_keys=("tree_label", "name"))

        if not isinstance(uuids, list):
            if uuids is not None:
                print("WARNING: '{}' is not a list in structure {} - skipped".format(uuid_key, structure.get("id")))
            continue

        if skip_unlabeled and label is None:
            continue

        for uuid in uuids:
            if not isinstance(uuid, str) or not uuid:
                continue
            if uuid not in mapping:
                mapping[uuid] = label
                continue
            existing = mapping[uuid]
            if existing is None and label is not None:
                mapping[uuid] = label
            elif existing is not None and label is not None and existing != label:
                print("WARNING: uuid {} has conflicting labels ('{}' vs '{}') - kept '{}'".format(
                    uuid, existing, label, existing))

    unlabeled = sum(1 for value in mapping.values() if value is None)
    print("Read {} tree structures -> {} unique uuids in '{}' ({} without label)".format(
        seen_structures, len(mapping), uuid_key, unlabeled))
    return mapping


# ---------------------------------------------------------------------------
# Migration mapping
# ---------------------------------------------------------------------------
def build_migration_mapping(raw_mapping: Any) -> dict[str, str]:
    """Build { old_uuid: new_uuid } from either a dict or a list of relation objects."""
    if isinstance(raw_mapping, dict):
        if not all(isinstance(k, str) and isinstance(v, str) for k, v in raw_mapping.items()):
            raise ValueError("Migration mapping dict must map string uuid -> string uuid")
        return dict(raw_mapping)

    if not isinstance(raw_mapping, list):
        raise ValueError("Unsupported migration mapping format: {}".format(type(raw_mapping).__name__))

    mapping: dict[str, str] = {}
    duplicates = 0
    for entry in raw_mapping:
        if not isinstance(entry, dict):
            continue
        source = next((entry[k] for k in FROM_KEYS if k in entry), None)
        target = next((entry[k] for k in TO_KEYS if k in entry), None)
        if not isinstance(source, str) or not isinstance(target, str):
            continue
        if source in mapping and mapping[source] != target:
            duplicates += 1
            continue
        mapping[source] = target

    if duplicates:
        print("WARNING: {} duplicate _from entries in the migration mapping - kept the first".format(duplicates))
    return mapping


def translate_to_lsbord(
    label_mapping: dict[str, Any],
    migration_mapping: dict[str, str],
    keep_unmapped: bool = False,
) -> tuple[dict[str, Any], list[str]]:
    """Replace every lsdeel uuid with its lsbord uuid; returns (mapping, unmapped_uuids)."""
    result: dict[str, Any] = {}
    unmapped: list[str] = []
    collisions = 0

    for lsdeel_uuid, label in label_mapping.items():
        lsbord_uuid = migration_mapping.get(lsdeel_uuid)
        if lsbord_uuid is None:
            unmapped.append(lsdeel_uuid)
            if keep_unmapped:
                lsbord_uuid = lsdeel_uuid
            else:
                continue
        if lsbord_uuid in result and result[lsbord_uuid] != label:
            collisions += 1
            print("WARNING: lsbord uuid {} got conflicting labels - kept '{}'".format(lsbord_uuid, result[lsbord_uuid]))
            continue
        result[lsbord_uuid] = label

    return result, unmapped


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--tree-structures", type=Path, default=None,
                        help="Input tree structures JSON (default: output/tree_structures.json)")
    parser.add_argument("--prefer-pre-migration", action="store_true",
                        help="Use output/tree_structures_original.json when it exists (uuids before the GemigreerdNaar migration)")
    parser.add_argument("--migration-mapping", type=Path, default=DEFAULT_MIGRATION_MAPPING,
                        help="GemigreerdNaar mapping file (default: %(default)s)")
    parser.add_argument("--lsdeel-out", type=Path, default=DEFAULT_LSDEEL_OUT,
                        help="Output {lsdeel_uuid: label} (default: %(default)s)")
    parser.add_argument("--lsbord-out", type=Path, default=DEFAULT_LSBORD_OUT,
                        help="Output {lsbord_uuid: label} (default: %(default)s)")
    parser.add_argument("--uuid-key", default=DEFAULT_UUID_KEY, help="UUID array key (default: %(default)s)")
    parser.add_argument("--label-key", default=DEFAULT_LABEL_KEY, help="Label key (default: %(default)s)")
    parser.add_argument("--indent", type=int, default=2, help="JSON indent (default: %(default)s)")
    parser.add_argument("--skip-unlabeled", action="store_true",
                        help="Skip uuids whose structure has an empty label")
    parser.add_argument("--keep-unmapped", action="store_true",
                        help="Keep lsdeel uuids without a GemigreerdNaar relation in the lsbord file under their original uuid")
    parser.add_argument("--report-unmapped", type=Path, default=None,
                        help="Write the unmapped lsdeel uuids (and their labels) to this JSON file")
    return parser.parse_args(argv)


def resolve_tree_structures(args: argparse.Namespace) -> Path:
    if args.tree_structures is not None:
        return args.tree_structures
    if args.prefer_pre_migration and PRE_MIGRATION_TREE_STRUCTURES.exists():
        return PRE_MIGRATION_TREE_STRUCTURES
    return DEFAULT_TREE_STRUCTURES


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    tree_path = resolve_tree_structures(args)
    if not tree_path.exists():
        print("ERROR: tree structures not found at {}".format(tree_path), file=sys.stderr)
        return 1
    if not args.migration_mapping.exists():
        print("ERROR: migration mapping not found at {}".format(args.migration_mapping), file=sys.stderr)
        return 1

    tree_structures = load_json(tree_path)
    migration_mapping = build_migration_mapping(load_json(args.migration_mapping))
    print("Loaded tree structures from {}".format(tree_path))
    print("Loaded {} migration relations from {}".format(len(migration_mapping), args.migration_mapping))

    # Stap 1: lsdeel_uuid -> label
    lsdeel_mapping = build_label_mapping(
        tree_structures,
        uuid_key=args.uuid_key,
        label_key=args.label_key,
        skip_unlabeled=args.skip_unlabeled,
    )
    write_json(args.lsdeel_out, lsdeel_mapping, indent=args.indent)
    print("Wrote {} lsdeel uuid -> label mappings to {}".format(len(lsdeel_mapping), args.lsdeel_out))

    # Stap 2: lsdeel_uuid -> lsbord_uuid -> label
    lsbord_mapping, unmapped = translate_to_lsbord(lsdeel_mapping, migration_mapping, keep_unmapped=args.keep_unmapped)
    replaced = len(lsbord_mapping) - len([u for u in unmapped if not args.keep_unmapped])
    write_json(args.lsbord_out, lsbord_mapping, indent=args.indent)
    print("Replaced {} lsdeel uuids by lsbord uuids ({} unmapped)".format(replaced, len(unmapped)))
    print("Wrote {} lsbord uuid -> label mappings to {}".format(len(lsbord_mapping), args.lsbord_out))

    if unmapped:
        print("WARNING: {} lsdeel uuids have no GemigreerdNaar relation{}".format(
            len(unmapped), "" if args.keep_unmapped else " and were left out of the lsbord file"))
        if args.report_unmapped:
            payload = {uuid: lsdeel_mapping.get(uuid) for uuid in unmapped}
            write_json(args.report_unmapped, payload, indent=args.indent)
            print("Wrote unmapped uuids to {}".format(args.report_unmapped))

    return 0


if __name__ == "__main__":
    sys.exit(main())