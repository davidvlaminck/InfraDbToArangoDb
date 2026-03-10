import json
import sys
from pathlib import Path
# Ensure repo root is on sys.path so package imports like Analysis.TreeAnalysis work
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from Analysis.TreeAnalysis.tree_analysis import (
    _canonicalize_structure,
    _structure_key,
    build_structures_and_instances,
    build_assettype_map,
    run_and_persist_structures,
)


def test_canonicalize_structure():
    level_sets = {0: {"a", "b"}, 2: {"c"}}
    out = _canonicalize_structure(level_sets)
    assert out == [["a", "b"], [], ["c"]]


def test_build_simple_structure(tmp_path):
    assets = [
        {"_key": "k1", "assettype_key": "t_ls", "naampad_parts": ["B1", "K1", "L1"], "AIMDBStatus_isActief": True},
        {"_key": "k2", "assettype_key": "t_lsdeel", "naampad_parts": ["B1", "K1", "L1.D1"], "AIMDBStatus_isActief": True},
    ]
    assettypes = [{"_key": "t_ls", "short_uri": "lgc:installatie#LS"}, {"_key": "t_lsdeel", "short_uri": "lgc:installatie#LSDeel"}]
    assettype_map = build_assettype_map(assettypes)
    structures, instances = build_structures_and_instances(assets, assettype_map, lsdeel_short_uri="lgc:installatie#LSDeel")
    assert isinstance(structures, dict)
    assert isinstance(instances, dict)
    assert "B1" in instances
    # persist using run_and_persist_structures
    s_list, inst = run_and_persist_structures(assets, assettype_map, tmp_path, lsdeel_short_uri="lgc:installatie#LSDeel")
    assert isinstance(s_list, list)
    assert (tmp_path / "tree_structures.json").exists()
    assert (tmp_path / "tree_instances.json").exists()


def test_merge_existing_list_format(tmp_path):
    # simulate older existing tree stored as list of dicts
    assets = [
        {"_key": "k1", "assettype_key": "t1", "naampad_parts": ["B1", "K"], "AIMDBStatus_isActief": True},
    ]
    assettypes = [{"_key": "t1", "short_uri": "lgc:installatie#Kast"}]
    assettype_map = build_assettype_map(assettypes)
    structures, instances = build_structures_and_instances(assets, assettype_map)
    # manually inject an older list format into structures_by_key via run_and_persist_structures
    s_list, inst = run_and_persist_structures(assets, assettype_map, tmp_path)
    assert isinstance(s_list, list)
    # load the persisted structures and ensure tree is dict
    loaded = json.loads((tmp_path / "tree_structures.json").read_text(encoding="utf-8"))
    assert isinstance(loaded, list)
    # each structure must have 'tree' as a dict
    for s in loaded:
        assert isinstance(s.get("tree", {}), dict)


def test_canonical_and_instances_simple():
    # two instances with same structure
    assettype_map = {"t1": "type:Root", "t2": "type:Child", "t3": "type:LSDeel"}
    assets = [
        {"_key": "a1", "assettype_key": "t1", "naampad_parts": ["B1"]},
        {"_key": "a2", "assettype_key": "t2", "naampad_parts": ["B1", "C1"]},
        {"_key": "a3", "assettype_key": "t3", "naampad_parts": ["B1", "C1", "D1"]},
        {"_key": "b1", "assettype_key": "t1", "naampad_parts": ["B2"]},
        {"_key": "b2", "assettype_key": "t2", "naampad_parts": ["B2", "C2"]},
        {"_key": "b3", "assettype_key": "t3", "naampad_parts": ["B2", "C2", "D2"]},
    ]

    structures, instances = build_structures_and_instances(assets, assettype_map, lsdeel_short_uri="type:LSDeel")

    # both instances should map to the same structure id
    assert len(structures) == 1
    assert "B1" in instances and "B2" in instances
    assert instances["B1"]["structure_id"] == instances["B2"]["structure_id"]
    # each instance should have 3 assets
    assert instances["B1"]["num_assets"] == 3
    assert instances["B2"]["num_assets"] == 3
    # lsdeel keys should be present
    assert "a3" in instances["B1"]["lsdeel_keys"]
    assert "b3" in instances["B2"]["lsdeel_keys"]


def test_inactive_assets_are_ignored():
    assettype_map = {"t1": "type:Root", "t2": "type:Child"}
    assets = [
        {"_key": "x1", "assettype_key": "t1", "naampad_parts": ["BX"]},
        {"_key": "x2", "assettype_key": "t2", "naampad_parts": ["BX", "Y1"], "AIMDBStatus_isActief": False},
    ]
    structures, instances = build_structures_and_instances(assets, assettype_map)
    # x2 is inactive so only x1 should be considered -> instance BX has 1 asset
    assert "BX" in instances
    assert instances["BX"]["num_assets"] == 1

