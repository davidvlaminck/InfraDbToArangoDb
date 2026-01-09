"""Pytest unit tests for the keuringsinfo AQL matching & routing logic.

These tests don't require a running ArangoDB.
They verify the **intent** of the AQL builder:
- Matching LS -> LSDeel happens **only** via the Voedt edge (OUTBOUND).
- No naampad-based fallback is used.
"""

from __future__ import annotations

from Analysis.export_keuringsinfo import KeuringsRecord, _is_not_included, _sheet_name, build_aql


def test_parent_match_is_not_used():
    q = build_aql(ls_short_uri="x", lsdeel_short_uri="y")
    assert "naampad_parent" not in q
    assert "pairs_parent" not in q


def test_limit_clause_is_optional():
    q1 = build_aql(ls_short_uri="x", lsdeel_short_uri="y")
    assert "LIMIT @limit" not in q1

    q2 = build_aql(ls_short_uri="x", lsdeel_short_uri="y", limit=10)
    assert "LIMIT @limit" in q2


def test_outbound_traversal_is_used_for_ls_to_lsdeel():
    q = build_aql(ls_short_uri="x", lsdeel_short_uri="y")
    assert "FOR lsdeel, e IN OUTBOUND ls assetrelaties" in q


def test_ranked_dedup_per_chosen_object():
    q = build_aql(ls_short_uri="x", lsdeel_short_uri="y")
    assert "LET all_candidates" in q
    assert "COLLECT k = chosen._key" in q
    assert "SORT g.rank ASC" in q


def test_no_large_intermediate_ls_assets_arrays():
    q = build_aql(ls_short_uri="x", lsdeel_short_uri="y")
    assert "LET ls_assets" not in q
    assert "LET lsdeel_assets" not in q
    assert "STARTS_WITH(" not in q


def test_tov_has_own_sheet():
    assert _sheet_name("Tunnel Organ. VL.") == "Tunnel Organ. VL."


def test_aql_includes_isActief_and_toestand_columns():
    q = build_aql(ls_short_uri="x", lsdeel_short_uri="y")
    assert '"isActief": chosen.AIMDBStatus_isActief' in q
    assert '"toestand": chosen.toestand' in q


def test_not_included_when_removed_or_transferred():
    for toestand in ["verwijderd", "overgedragen"]:
        r = KeuringsRecord(
            toezichtgroep="V&W-WL",
            type="LS",
            match="single_ls",
            uuid="u",
            naam="n",
            naampad="p",
            isActief=True,
            toestand=toestand,
            datum_laatste_keuring=None,
            resultaat_keuring=None,
        )
        assert _is_not_included(r) is True


def test_included_when_active_and_not_removed():
    r = KeuringsRecord(
        toezichtgroep="V&W-WL",
        type="LS",
        match="single_ls",
        uuid="u",
        naam="n",
        naampad="p",
        isActief=True,
        toestand="in-gebruik",
        datum_laatste_keuring=None,
        resultaat_keuring=None,
    )
    assert _is_not_included(r) is False
