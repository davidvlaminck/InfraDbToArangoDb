from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from API.APIEnums import AuthType, Environment
from DBPipelineController import DBPipelineController
from InitialFillStep import InitialFillStep


def _has_bad_keys(obj: Any) -> bool:
    """Return True if any dict key contains ':' or '.' anywhere in the document tree."""
    if isinstance(obj, dict):
        for k, v in obj.items():
            if ":" in k or "." in k:
                return True
            if _has_bad_keys(v):
                return True
        return False
    if isinstance(obj, list):
        return any(_has_bad_keys(i) for i in obj)
    return False


@pytest.mark.integration
def test_assets_written_to_arango_have_no_namespaced_or_dotted_keys():
    """Integration test against PRD Arango + API.

    WARNING: This truncates the `assets` collection.

    Run explicitly:
        pytest -m integration -s
    """

    settings_path = Path("/home/davidlinux/Documenten/AWV/resources/settings_SyncToArangoDB.json")
    controller = DBPipelineController(settings_path=settings_path, env=Environment.PRD, auth_type=AuthType.JWT)
    db = controller.factory.create_connection()

    # Start from clean assets collection
    db.collection("assets").truncate()

    # Insert one API page
    step = InitialFillStep(controller.factory, controller.eminfra_client, controller.emson_client)
    cursor, items = next(controller.emson_client.get_resource_by_cursor("assets", cursor=None, page_size=1000))
    assert items, "API returned no assets"

    step._insert_assets(db, items)

    # Validate a small sample from Arango
    docs = list(db.aql.execute("FOR d IN assets LIMIT 50 RETURN d"))
    assert docs, "No documents written to Arango"

    # No top-level tz:... keys and no ':'/'.' keys anywhere in doc tree
    for d in docs:
        assert not any(k.startswith("tz:") for k in d.keys())
        assert not _has_bad_keys(d)
