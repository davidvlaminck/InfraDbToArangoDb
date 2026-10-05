#!/usr/bin/env python3
"""
Update UUID and typeURI na de verweving.
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
MAPPING_PATH_ASSET_UUID = SCRIPT_DIR / "mapping_gemigreerdnaar_20260929.json"
MAPPING_PATH_ASSETTYPE_URI = SCRIPT_DIR / "mapping_verweving_202609.json"

UUID_ARRAY_KEYS = ("lsb_uuids", "hscabine_uuids")


def load_json(path):
    """Load a JSON file and return the parsed object."""
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def build_mapping(raw_mapping):
    """Build a dict mapping old -> new from either file format."""
    if isinstance(raw_mapping, dict):
        return dict(raw_mapping)

    mapping = {}
    for entry in raw_mapping:
        mapping[entry["_from"]] = entry["_to"]
    return mapping


def replace_uuids_in_uuid_arrays(tree_structure, mapping, keys=UUID_ARRAY_KEYS):
    """
    Replace UUIDs inside the uuid arrays ("lsdeel_uuids" and
    "hscabine_uuids") of a single tree structure object. All other fields
    (id, example, label, tree, count, occurrence) are left untouched.
    Returns the (possibly modified) object.
    """
    if not isinstance(tree_structure, dict):
        return tree_structure

    for key in keys:
        uuids = tree_structure.get(key)
        if not isinstance(uuids, list):
            continue

        tree_structure[key] = [
            mapping[value] if isinstance(value, str) and value in mapping else value
            for value in uuids
        ]

    return tree_structure


def replace_typeuri(node, mapping):
    """
    Recursively replace asset-type URIs (typeURI) anywhere in the parsed
    tree_structures.json document. Matches are replaced both as object keys
    and as scalar values, at any nesting depth (dicts, lists, strings).

    Non-string scalars, and strings absent from the mapping, are kept as-is.
    Returns the (new) node; the input is not mutated.
    """
    if isinstance(node, dict):
        replaced = {}
        for key, value in node.items():
            new_key = mapping.get(key, key) if isinstance(key, str) else key
            if new_key in replaced:
                raise ValueError("key collision {} while mapping ({})".format(new_key, key))
            replaced[new_key] = replace_typeuri(value, mapping)
        return replaced

    if isinstance(node, list):
        return [replace_typeuri(item, mapping) for item in node]

    if isinstance(node, str):
        return mapping.get(node, node)

    return node


def main():
    if not TREE_STRUCTURES_PATH.exists():
        raise FileNotFoundError("tree_structures.json not found at {}".format(TREE_STRUCTURES_PATH))
    if not MAPPING_PATH_ASSET_UUID.exists():
        raise FileNotFoundError("Mapping file not found at {}".format(MAPPING_PATH_ASSET_UUID))
    if not MAPPING_PATH_ASSETTYPE_URI.exists():
        raise FileNotFoundError("Mapping file not found at {}".format(MAPPING_PATH_ASSETTYPE_URI))

    # Load inputs
    tree_structures = load_json(TREE_STRUCTURES_PATH)
    raw_mapping_path_asset_uuid = load_json(MAPPING_PATH_ASSET_UUID)
    mapping_asset_uuid = build_mapping(raw_mapping_path_asset_uuid)
    raw_mapping_path_assettype_uri = load_json(MAPPING_PATH_ASSETTYPE_URI)
    mapping_assettype_uri = build_mapping(raw_mapping_path_assettype_uri)

    print("Loaded {} tree structures from {}".format(len(tree_structures), TREE_STRUCTURES_PATH))
    print("Loaded {} asset UUID mappings from {}".format(len(mapping_asset_uuid), MAPPING_PATH_ASSET_UUID))
    print("Loaded {} assettype URI mappings (verweving) from {}".format(len(mapping_assettype_uri), MAPPING_PATH_ASSETTYPE_URI))

    # Replace UUIDs
    if isinstance(tree_structures, list):
        updated = [
            replace_uuids_in_uuid_arrays(item, mapping_asset_uuid) for item in tree_structures
        ]
    else:
        updated = replace_uuids_in_uuid_arrays(tree_structures, mapping_asset_uuid)

    # Replace assettype URI (keys and values, any depth)
    updated = replace_typeuri(updated, mapping_assettype_uri)

    # Write the result back to the same file
    with open(str(TREE_STRUCTURES_PATH), "w", encoding="utf-8") as fh:
        json.dump(updated, fh, indent=2, ensure_ascii=False)
        fh.write("\n")

    print("Updated UUIDs written to {}".format(TREE_STRUCTURES_PATH))


if __name__ == "__main__":
    main()

    # TODO
    # lanceer AQL-query vanuit Python-script en bouw de mapping file van de verweven uuid's op.

