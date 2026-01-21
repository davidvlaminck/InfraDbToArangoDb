import re

from pyproj import Transformer

from InitialFillStep import InitialFillStep


def _scrub_uuid(value: str) -> str:
    """Replace UUID-like patterns with fixed zeros."""
    return re.sub(
        r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}",
        "00000000-0000-0000-0000-000000000000",
        value,
    )


def _scrub_id(value: str) -> str:
    """Scrub UUID-like patterns in an @id to avoid leaking real identifiers."""
    return _scrub_uuid(value)


def test_transform_keys_emson_asset_shape_matches_written_asset_minimal():
    """Given an EMSON asset payload (with namespaced & dotted keys), we should store a normalized asset doc.

    This test validates the contract for a single asset transformation:
    - `_transform_keys` converts dotted keys to nested dicts (e.g. tz:Toezicht.toezichtgroep -> tz.Toezicht_toezichtgroep)
    - `_insert_assets`-equivalent logic enriches: _key, wkt, geometry, toestand, naampad_parts/parent,
      toezichtgroep_key, toezichter_key, beheerder_key and bestekkoppelingen edges.

    NOTE: We don't hit ArangoDB here; we only validate the in-memory transformation.
    """

    # --- Given: EMSON raw payload (scrubbed) ---
    # Use a non-realistic naampad and coordinates (repo is public)
    fake_naampad = "X9Y8Z7/X9Y8Z7.K"
    fake_wkt = "SRID=3812;POINT Z(1000.123 2000.456 0.0)"

    fake_asset_id = "https://data.awvvlaanderen.be/id/asset/00000000-0000-0000-0000-000000000000-FAKEASSET"
    fake_toezichtgroep_id = "11111111-1111-1111-1111-111111111111"
    fake_toezichter_id = "00000000-0000-0000-0000-000000000000"

    # keep shape, scrub identifiers
    fake_beheerder_ref = "BEH-000"

    raw = {
        "@id": fake_asset_id,
        "@type": "https://lgc.data.wegenenverkeer.be/ns/installatie#Kast",
        "AIMDBStatus.isActief": True,
        "AIMToestand.toestand": "https://wegenenverkeer.data.vlaanderen.be/id/concept/KlAIMToestand/in-gebruik",
        "NaampadObject.naampad": fake_naampad,
        "loc:Locatie.geometrie": fake_wkt,
        "tz:Toezicht.toezichter": {
            "tz:DtcToezichter.id": fake_toezichter_id,
        },
        "tz:Toezicht.toezichtgroep": {
            "tz:DtcToezichtGroep.id": fake_toezichtgroep_id,
        },
        "tz:Schadebeheerder.schadebeheerder": {
            "tz:DtcBeheerder.referentie": fake_beheerder_ref,
        },
        "bs:Bestek.bestekkoppeling": [
            {
                "bs:DtcBestekkoppeling.status": "https://bs.data.wegenenverkeer.be/id/concept/KlBestekKoppelingStatus/actief",
                "bs:DtcBestekkoppeling.bestekId": {
                    "DtcIdentificator.identificator": _scrub_uuid(
                        "5f2f7424-32f7-4559-b950-4be9113886bf-YnM6aW1wbGVtZW50YXRpZWVsZW1lbnQjQmVzdGVr"
                    )
                },
            }
        ],
    }

    # --- When: normalize keys ---
    normalized = InitialFillStep._transform_keys(raw)

    # --- Then: verify namespace grouping & recursive namespace removal ---
    # Top-level namespaces should be created as separate dict keys
    assert "tz" in normalized
    assert "bs" in normalized
    assert "loc" in normalized

    # Keys inside namespace groups should NOT contain ':' anymore and '.' should be '_' (recursively)
    assert "Toezicht_toezichtgroep" in normalized["tz"]
    assert "Toezicht_toezichter" in normalized["tz"]
    assert "Schadebeheerder_schadebeheerder" in normalized["tz"]

    assert "DtcToezichtGroep_id" in normalized["tz"]["Toezicht_toezichtgroep"]
    assert "DtcToezichter_id" in normalized["tz"]["Toezicht_toezichter"]
    assert "DtcBeheerder_referentie" in normalized["tz"]["Schadebeheerder_schadebeheerder"]

    assert "Bestek_bestekkoppeling" in normalized["bs"]
    assert isinstance(normalized["bs"]["Bestek_bestekkoppeling"], list)
    assert "DtcBestekkoppeling_bestekId" in normalized["bs"]["Bestek_bestekkoppeling"][0]
    assert "DtcBestekkoppeling_status" in normalized["bs"]["Bestek_bestekkoppeling"][0]

    assert "Locatie_geometrie" in normalized["loc"]

    # --- Continue with enrichment checks ---
    # We replicate the logic inline here to avoid refactoring production code just for the test.
    # If you prefer, we can factor this into a dedicated helper method later.

    assettype_lookup = {"https://lgc.data.wegenenverkeer.be/ns/installatie#Kast": "10377658"}
    beheerders_lookup = {fake_beheerder_ref: "4e77efda"}

    obj = normalized
    obj["_key"] = obj.get("@id", "").split("/")[-1][:36]
    obj["assettype_key"] = assettype_lookup[obj.get("@type")]

    # WKT -> geometry
    wkt_string = InitialFillStep._extract_wkt_from_obj(obj)
    assert wkt_string == fake_wkt
    obj["wkt"] = wkt_string

    # geometry conversion should produce a 2D point
    transformer = Transformer.from_crs("EPSG:3812", "EPSG:4326", always_xy=True)
    geojson = InitialFillStep._fast_point_wgs84_from_wkt3812(wkt_string, transformer)
    assert geojson is not None
    assert geojson["type"] == "Point"
    assert len(geojson["coordinates"]) == 2
    obj["geometry"] = geojson

    # toestand derived
    toestand_uri = obj.get("AIMToestand_toestand")
    assert toestand_uri == "https://wegenenverkeer.data.vlaanderen.be/id/concept/KlAIMToestand/in-gebruik"
    obj["toestand"] = toestand_uri.split("/")[-1]

    # naampad derived
    naampad = obj.get("NaampadObject_naampad")
    assert naampad == fake_naampad
    parts = naampad.split("/")
    obj["naampad_parts"] = parts
    obj["naampad_parent"] = "/".join(parts[:-1])

    # tz derived
    tg_id = obj.get("tz", {}).get("Toezicht_toezichtgroep", {}).get("DtcToezichtGroep_id")
    assert tg_id == fake_toezichtgroep_id
    obj["toezichtgroep_key"] = tg_id[:8]

    toez_id = obj.get("tz", {}).get("Toezicht_toezichter", {}).get("DtcToezichter_id")
    assert toez_id == fake_toezichter_id
    obj["toezichter_key"] = toez_id[:8]

    sb_ref = obj.get("tz", {}).get("Schadebeheerder_schadebeheerder", {}).get("DtcBeheerder_referentie")
    assert sb_ref == fake_beheerder_ref
    obj["beheerder_key"] = beheerders_lookup[sb_ref]

    # bestek koppeling enrichment: verify _from/_to are well-formed and status normalized
    bestek_koppelingen = obj.get("bs", {}).get("Bestek_bestekkoppeling")
    assert isinstance(bestek_koppelingen, list)
    assert len(bestek_koppelingen) == 1

    kopp = bestek_koppelingen[0]
    kopp["_from"] = "assets/" + obj["_key"]
    kopp["_to"] = "bestekken/" + kopp["DtcBestekkoppeling_bestekId"]["DtcIdentificator_identificator"][:8]
    assert kopp["_to"] == "bestekken/00000000"

    # --- Then: verify final fields expected in stored asset ---
    assert obj["assettype_key"] == "10377658"
    assert obj["toestand"] == "in-gebruik"
    assert obj["naampad_parts"] == ["X9Y8Z7", "X9Y8Z7.K"]
    assert obj["naampad_parent"] == "X9Y8Z7"
    assert obj["toezichtgroep_key"] == fake_toezichtgroep_id[:8]
    assert obj["toezichter_key"] == fake_toezichter_id[:8]
    assert obj["beheerder_key"] == "4e77efda"
