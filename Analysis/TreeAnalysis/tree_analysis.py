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
    lsdeel_short_uri: str | None = None,
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

        canonical = _canonicalize_structure(level_sets)
        key = _structure_key(canonical)
        sid = _structure_id_from_key(key)
        if key not in structures_by_key:
            structures_by_key[key] = {
                "id": sid,
                "structure": canonical,
                "example": {"beheerobject": beheer, "sample_asset_key": asset_keys[0] if asset_keys else None},
                "label": None,
            }
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

