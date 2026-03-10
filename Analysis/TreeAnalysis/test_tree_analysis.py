from Analysis.TreeAnalysis.tree_analysis import (
    build_structures_and_instances,
    build_assettype_map,
)


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
