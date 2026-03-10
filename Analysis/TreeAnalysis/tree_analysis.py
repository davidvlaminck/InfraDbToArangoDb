"""Core tree analysis utilities.

This module provides functions to build canonical tree structures and map instances
from a list of asset dicts. It expects each asset to have:
- _key
- assettype_key
- naampad_parts (non-empty list)

It also expects an assettype_map: dict assettype_key -> short_uri

Functions are pure and easy to unit-test.
"""
from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from typing import Dict, Iterable, List, Set, Tuple, Any
from pathlib import Path

# default short URI for LSDeel type; make configurable globally
DEFAULT_LSDEEL_SHORT_URI = "lgc:installatie#LSDeel"


def _beheer_from_parts(parts: List[str]) -> str | None:
    if not parts:
        return None
    first = parts[0]
    return first if first and first.strip() else None


def _canonicalize_structure(level_sets: Dict[int, Set[str]]) -> List[List[str]]:
    """Produce canonical ordered list-of-lists for a structure.

    level_sets: mapping depth->set(short_uri)
    Returns ordered list from level 0 upward, each entry is a sorted list of short_uris.
    """
    if not level_sets:
        return []
    max_depth = max(level_sets.keys())
    result: List[List[str]] = []
    for depth in range(0, max_depth + 1):
        s = level_sets.get(depth, set())
        result.append(sorted(s))
    return result


def _structure_key(canonical: List[List[str]]) -> str:
    # deterministic JSON dump
    return json.dumps(canonical, ensure_ascii=False, sort_keys=True)


def _structure_id_from_key(key: str) -> str:
    # produce a short stable id from sha1 of key
    return hashlib.sha1(key.encode("utf-8")).hexdigest()


def build_structures_and_instances(
    assets: Iterable[Dict[str, Any]],
    assettype_map: Dict[str, str],
    lsdeel_short_uri: str | None = DEFAULT_LSDEEL_SHORT_URI,
) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]]]:
    """Build canonical structures and instances from assets.

    Args:
      assets: iterable of asset dicts with keys: _key, assettype_key, naampad_parts
      assettype_map: mapping assettype_key -> short_uri
      lsdeel_short_uri: optional short_uri used to mark LSDeel assets

    Returns:
      (structures, instances)
      structures: mapping structure_id -> {id, structure, example}
      instances: mapping beheerobject -> {structure_id, asset_keys, lsdeel_keys, num_assets}
    """
    # group assets by beheerobject
    groups: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for a in assets:
        # ignore inactive assets: if AIMDBStatus_isActief is present and False => skip
        is_active = True
        if "AIMDBStatus_isActief" in a:
            val = a.get("AIMDBStatus_isActief")
            # accept boolean or string values
            if isinstance(val, str):
                is_active = val.lower() == "true"
            else:
                is_active = bool(val)
        if not is_active:
            continue

        parts = a.get("naampad_parts") or []
        if not parts:
            # per spec ignore assets without naampad_parts
            continue
        beheer = _beheer_from_parts(parts)
        if not beheer:
            continue
        groups[beheer].append(a)

    structures_by_key: Dict[str, Dict[str, Any]] = {}
    instances: Dict[str, Dict[str, Any]] = {}

    for beheer, items in groups.items():
        # collect level -> set of short_uris
        level_sets: Dict[int, Set[str]] = defaultdict(set)
        lsdeel_keys: List[str] = []
        asset_keys: List[str] = []
        # collect exact path -> types so we can infer parent-child relationships
        exact_map: Dict[str, Set[str]] = defaultdict(set)
        asset_records: List[Tuple[str, List[str], str]] = []
        for it in items:
            parts = it.get("naampad_parts") or []
            depth = max(0, len(parts) - 1)
            at_key = it.get("assettype_key")
            short = assettype_map.get(at_key, at_key) if at_key is not None else None
            if short:
                level_sets[depth].add(short)
            ak = it.get("_key")
            if ak:
                asset_keys.append(ak)
            if lsdeel_short_uri and short == lsdeel_short_uri and ak:
                lsdeel_keys.append(ak)
            path = "/".join(parts) if parts else ""
            if short:
                exact_map[path].add(short)
            asset_records.append((path, parts, short))

        canonical = _canonicalize_structure(level_sets)
        key = _structure_key(canonical)
        sid = _structure_id_from_key(key)
        if key not in structures_by_key:
            structures_by_key[key] = {
                "id": sid,
                "structure": canonical,
                "example": {"beheerobject": beheer, "sample_asset_key": asset_keys[0] if asset_keys else None},
                "label": "",
                # adjacency tree: parent_short_uri -> sorted list of child short_uris
                "tree": {},
            }
        # Build adjacency using exact parent path match constrained by canonical levels
        adjacency: Dict[str, Set[str]] = defaultdict(set)
        # map type to its canonical level(s)
        type_to_levels: Dict[str, Set[int]] = defaultdict(set)
        for lvl_idx, lvl in enumerate(canonical):
            for t in lvl:
                type_to_levels[t].add(lvl_idx)

        for path, parts, short in asset_records:
            if not parts or short is None:
                continue
            # child's canonical level(s)
            child_levels = type_to_levels.get(short, set())
            if not child_levels:
                continue
            # parent path (immediate parent)
            parent_path = "/".join(parts[:-1])
            if not parent_path:
                continue
            # parents at that exact parentPath
            parents = exact_map.get(parent_path, set())
            for p_short in parents:
                if p_short == short:
                    continue
                parent_levels = type_to_levels.get(p_short, set())
                # require that parent level is immediately above one of the child's levels
                matched = any((pl + 1) in child_levels for pl in parent_levels)
                if matched:
                    adjacency[p_short].add(short)

        # ensure parent keys exist even if they have no children (empty list)
        for i in range(0, max(0, len(canonical) - 1)):
            for p_short in canonical[i]:
                adjacency.setdefault(p_short, set())

        # merge with any existing tree for this canonical key
        existing = structures_by_key[key].get("tree") or {}
        # defensive: existing might be a list (older format) or dict. Normalize to dict[str, set]
        existing_sets: Dict[str, Set[str]] = {}
        if isinstance(existing, dict):
            for k, v in existing.items():
                # if v is already a list or set, coerce to set
                if isinstance(v, (list, set)):
                    existing_sets[k] = set(v)
                else:
                    # unexpected type, try to coerce via str
                    try:
                        existing_sets[k] = set(v)
                    except Exception:
                        existing_sets[k] = set()
        elif isinstance(existing, list):
            # older format: list of pairs? try to interpret as list of [parent, [children]] items
            try:
                for item in existing:
                    if isinstance(item, dict):
                        for k, v in item.items():
                            existing_sets[k] = set(v if isinstance(v, (list, set)) else [])
            except Exception:
                existing_sets = {}
        else:
            existing_sets = {}

        for p, children in adjacency.items():
            existing_sets.setdefault(p, set()).update(children)
        # write back sorted lists
        structures_by_key[key]["tree"] = {p: sorted(list(c)) for p, c in existing_sets.items()}

        # map instance
        instances[beheer] = {
            "structure_id": structures_by_key[key]["id"],
            "asset_keys": asset_keys,
            "lsdeel_keys": lsdeel_keys,
            "num_assets": len(asset_keys),
        }

    # create final structures dict keyed by id for output convenience
    structures: Dict[str, Dict[str, Any]] = {}
    for val in structures_by_key.values():
        # ensure label is a string (default to empty string)
        if val.get("label") is None:
            val["label"] = ""
        structures[val["id"]] = val

    return structures, instances


# small helper to resolve assettype map from a cursor/iterable of assettype docs
def build_assettype_map(assettypes: Iterable[Dict[str, Any]]) -> Dict[str, str]:
    """Return mapping assettype_key->_key OR short uri

    We prefer mapping from _key -> short_uri and also attempt mapping from provided key strings.
    """
    m: Dict[str, str] = {}
    for at in assettypes:
        k = at.get("_key")
        short = at.get("short_uri") or at.get("short")
        if k and short:
            m[k] = short
    return m


def run_and_persist_structures(
    assets: Iterable[Dict[str, Any]],
    assettype_map: Dict[str, str],
    out_dir: Path,
    lsdeel_short_uri: str | None = DEFAULT_LSDEEL_SHORT_URI,
    omit_structure: bool = False,
) -> Tuple[List[Dict[str, Any]], Dict[str, Dict[str, Any]]]:
    """Run structure extraction and persist JSON files.

    This function encapsulates the same logic used by the CLI runner. It returns
    the structures list and the instances dict for further programmatic use.

    Args:
      assets: iterable of asset dicts
      assettype_map: mapping key -> short_uri
      out_dir: Path where outputs will be written (directory will be created if needed)
      lsdeel_short_uri: optional short uri used to mark LSDeel assets
      omit_structure: if True, do not include the 'structure' attribute in the persisted JSON

    Returns (structures_list, instances)
    """
    out_dir.mkdir(parents=True, exist_ok=True)

    structures, instances = build_structures_and_instances(assets, assettype_map, lsdeel_short_uri)

    # Annotate structures with count (total assets), occurrence (# unique beheerobjects)
    # and lsdeel_uuids (list of UUIDs of LSDeel assets in that structure)
    assets_by_id: dict[str, int] = {}
    occurrence_by_id: dict[str, int] = {}
    lsdeel_by_id: dict[str, set] = {}
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
        # collect lsdeel keys
        lks = inst.get("lsdeel_keys") or []
        if lks:
            sset = lsdeel_by_id.setdefault(sid, set())
            for v in lks:
                sset.add(v)

    structures_list = []
    for sid, s in structures.items():
        if s.get("label") is None:
            s["label"] = ""
        s_copy = dict(s)
        # optionally remove the canonical 'structure' field when it's redundant
        if omit_structure and "structure" in s_copy:
            s_copy.pop("structure")
        # add lsdeel UUIDs list (sorted) for this structure
        s_copy["lsdeel_uuids"] = sorted(list(lsdeel_by_id.get(sid, set())))
        s_copy["count"] = int(assets_by_id.get(sid, 0))
        s_copy["occurrence"] = int(occurrence_by_id.get(sid, 0))
        structures_list.append(s_copy)

    structures_list = sorted(structures_list, key=lambda x: x.get("count", 0), reverse=True)

    # write output files
    with (out_dir / "tree_structures.json").open("w", encoding="utf-8") as f:
        json.dump(structures_list, f, ensure_ascii=False, indent=2)
    with (out_dir / "tree_instances.json").open("w", encoding="utf-8") as f:
        json.dump(instances, f, ensure_ascii=False, indent=2)

    return structures_list, instances

