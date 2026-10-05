#!/usr/bin/env python3
"""
Append the label in the file tree_structures.json, based on the mapping-file mapping_lsdeel_label_202610.json
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

DEFAULT_TREE_STRUCTURES = OUTPUT_DIR / "tree_structures.json"
PRE_MIGRATION_TREE_STRUCTURES = OUTPUT_DIR / "tree_structures_original.json"

DEFAULT_LSDEEL_LABEL = SCRIPT_DIR / "mapping_lsdeel_label_202610.json"

DEFAULT_UUID_KEY = "lsb_uuids"
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


def append_label(
    tree_structures: Any,
    mapping_dict: dict,
    uuid_key: str = DEFAULT_UUID_KEY,
    label_key: str = DEFAULT_LABEL_KEY,
) -> dict[str, Any]:
    """
    Append the label to the tree_structure based on the mapping file.
    Returns the tree_structure, with the completed label.

    Arguments
    :param tree_structures
    :type tree_structures
    :param mapping_dict
    :type mapping_dict: dict[str, Any]
    :param uuid_key: key of the Laagspanningsbord uuid
    :type uuid_key: str
    :param label_key: key of the label
    :type label_key: str
    """
    seen_structures = 0

    # Loop over each structure
    for structure in iter_structures(tree_structures):
        seen_structures += 1
        uuids = structure.get(uuid_key)
        label = extract_label(structure, label_key, fallback_keys=("tree_label", "name"))

        if not isinstance(uuids, list):
            if uuids is not None:
                print("WARNING: '{}' is not a list in structure {} - skipped".format(uuid_key, structure.get("id")))
            continue

        if label is not None:
            print('INFO: Label is filled, skip this instance.')
            continue

        for uuid in uuids:
            if not isinstance(uuid, str) or not uuid:
                continue
            if uuid not in mapping_dict: # test. uuid not in mapping_dict or uuid not in mapping_dict.keys()
                continue
            else:
                new_label = mapping_dict[uuid]
                structure[label_key] = new_label
                break

    return tree_structures


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
    parser.add_argument("--lsdeel-out", type=Path, default=DEFAULT_LSDEEL_LABEL,
                        help="Output {lsdeel_uuid: label} (default: %(default)s)")
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

    tree_structures = load_json(tree_path)
    mapping_dict_lsdeel_label = load_json(args.lsdeel_out)

    # Fill in the label
    tree_structures_with_label = append_label(tree_structures, mapping_dict=mapping_dict_lsdeel_label, uuid_key=args.uuid_key, label_key=args.label_key)

    # Rewrite the tree_structures (original and with label)
    write_json(PRE_MIGRATION_TREE_STRUCTURES, tree_structures, indent=args.indent)
    print("Wrote a copy of the file to {}.".format(PRE_MIGRATION_TREE_STRUCTURES))

    write_json(DEFAULT_TREE_STRUCTURES, tree_structures_with_label, indent=args.indent)
    print("Overwrite the file {} with a label.".format(DEFAULT_TREE_STRUCTURES))

    return 0


if __name__ == "__main__":
    sys.exit(main())