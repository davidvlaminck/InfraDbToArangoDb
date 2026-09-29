#!/usr/bin/env python3
"""
Update UUIDs in tree_structures.json using the gemigreerdnaar mapping file.

Reads each 36-character UUID found in the "lsdeel_uuids" arrays of
tree_structures.json and replaces it with the corresponding new UUID from
the mapping file (20260929_gemigreerdnaar.json), which contains _from/_to
pairs.

Only UUIDs inside "lsdeel_uuids" arrays are modified. All other fields
(e.g. id, example, label, tree, count, occurrence) are left untouched.
"""

import json
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths (relative to this script so it can be run from anywhere)
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
TREE_ANALYSIS_DIR = SCRIPT_DIR.parent
OUTPUT_DIR = TREE_ANALYSIS_DIR / "output"

TREE_STRUCTURES_PATH = OUTPUT_DIR / "tree_structures.json"
MAPPING_PATH = SCRIPT_DIR / "20260929_gemigreerdnaar.json"


def load_json(path):
    """Load a JSON file and return the parsed object."""
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def build_mapping(raw_mapping):
    """
    Build a dict mapping old UUID (_from) -> new UUID (_to).

    The mapping file is a list of objects with "_from" and "_to" keys.
    """
    mapping = {}
    for entry in raw_mapping:
        old_uuid = entry["_from"]
        new_uuid = entry["_to"]
        mapping[old_uuid] = new_uuid
    return mapping


def replace_uuids_in_lsdeel_uuids(tree_structure, mapping):
    """
    Replace UUIDs only inside the "lsdeel_uuids" array of a single tree
    structure object. Returns the (possibly modified) object.
    """
    if not isinstance(tree_structure, dict):
        return tree_structure

    lsdeel_uuids = tree_structure.get("lsdeel_uuids")
    if not isinstance(lsdeel_uuids, list):
        return tree_structure

    updated_uuids = []
    for uuid_value in lsdeel_uuids:
        if isinstance(uuid_value, str) and uuid_value in mapping:
            # map to new uuid
            updated_uuids.append(mapping[uuid_value])
        else:
            # keep original uuid
            updated_uuids.append(uuid_value)

    tree_structure["lsdeel_uuids"] = updated_uuids
    return tree_structure


def main():
    if not TREE_STRUCTURES_PATH.exists():
        raise FileNotFoundError("tree_structures.json not found at {}".format(TREE_STRUCTURES_PATH))
    if not MAPPING_PATH.exists():
        raise FileNotFoundError("Mapping file not found at {}".format(MAPPING_PATH))

    # Load inputs
    tree_structures = load_json(TREE_STRUCTURES_PATH)
    raw_mapping = load_json(MAPPING_PATH)
    mapping = build_mapping(raw_mapping)

    print("Loaded {} tree structures from {}".format(len(tree_structures), TREE_STRUCTURES_PATH))
    print("Loaded {} UUID mappings from {}".format(len(mapping), MAPPING_PATH))

    # Replace UUIDs only inside "lsdeel_uuids" arrays
    if isinstance(tree_structures, list):
        updated = [replace_uuids_in_lsdeel_uuids(item, mapping) for item in tree_structures]
    else:
        updated = replace_uuids_in_lsdeel_uuids(tree_structures, mapping)

    # Write the result back to the same file
    with open(str(TREE_STRUCTURES_PATH), "w", encoding="utf-8") as fh:
        json.dump(updated, fh, indent=2, ensure_ascii=False)
        fh.write("\n")

    print("Updated UUIDs written to {}".format(TREE_STRUCTURES_PATH))


if __name__ == "__main__":
    main()

    # TODO
    # lanceer AQL-query vanuit Python-script en bouw de mapping file op.

