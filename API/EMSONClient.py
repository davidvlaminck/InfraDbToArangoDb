from collections.abc import Generator
from dataclasses import dataclass
from typing import Any, Optional

from API.APIEnums import AuthType, Environment
from API.EMInfraDomain import BaseDataclass
from API.RequesterFactory import RequesterFactory


@dataclass()
class Query(BaseDataclass):
    """DTO for EMSON cursor-based search endpoints."""

    size: int
    filters: dict
    orderByProperty: str | None = None
    fromCursor: str | None = None
    crs: str | None = '3812'  # Default to Belgian Lambert 2008


class EMSONClient:
    """Small client for the EMSON OTL endpoints.

    This wraps the HTTP requester and exposes a couple of convenience helpers.

    Note: most high-volume ingestion uses `get_resource_by_cursor()`.
    """

    def __init__(self, auth_type: AuthType, env: Environment, settings: dict | None = None, cookie: str | None = None):
        self.requester = RequesterFactory.create_requester(auth_type=auth_type, env=env, settings=settings, cookie=cookie)
        self.requester.first_part_url += 'emson/'

    def test_connection(self) -> dict[str, Any]:
        """Sanity check: fetch a small endpoint."""
        return self.requester.get("api/otl/assetrelaties").json()

    def get_resource_by_cursor(
        self,
        resource: str,
        cursor: Optional[str] = None,
        page_size: int = 100,
    ) -> Generator[tuple[Optional[str], list[dict[str, Any]]], None, None]:
        """Iterate over an EMSON resource using cursor-based pagination.

        Yields `(next_cursor, items)`.
        When `next_cursor` becomes None, there are no more pages.
        """
        query = Query(filters={}, size=page_size, fromCursor=cursor)
        while True:
            response = self.requester.post(url=f'api/otl/{resource}/search', data=query.json())
            if response.status_code != 200:
                raise ProcessLookupError(
                    f"EMSON request failed ({response.status_code}) for resource '{resource}': {response.content.decode('utf-8')}"
                )

            cursor = response.headers.get('em-paging-next-cursor')
            yield cursor, response.json().get('@graph', [])

            if cursor is None:
                break
            query.fromCursor = cursor

    def get_asset_by_uuid(self, uuid: str) -> dict[str, Any]:
        response = self.requester.get(url=f'api/otl/assets/{uuid}')
        if response.status_code != 200:
            raise ProcessLookupError(
                f"EMSON request failed ({response.status_code}) for asset '{uuid}': {response.content.decode('utf-8')}"
            )
        return response.json()

    def get_assetrelatie_by_uuid(self, uuid: str) -> dict[str, Any]:
        response = self.requester.get(url=f'api/otl/assetrelaties/{uuid}')
        if response.status_code != 200:
            raise ProcessLookupError(
                f"EMSON request failed ({response.status_code}) for assetrelatie '{uuid}': {response.content.decode('utf-8')}"
            )
        return response.json()

    def get_assets_by_filter(
        self,
        filter: dict,
        size: int = 100,
        order_by_property: str | None = None,
    ) -> Generator[dict[str, Any], None, None]:
        """Query assets via EMSON search endpoint.

        Docs: https://apps.mow.vlaanderen.be/emson/docs/#_post_emsonapiotlassetssearch
        """
        query = Query(filters=filter, size=size, orderByProperty=order_by_property)
        while True:
            response = self.requester.post(url='api/otl/assets/search', data=query.json())
            if response.status_code != 200:
                raise ProcessLookupError(response.content.decode("utf-8"))

            yield from response.json().get('@graph', [])

            paging_cursor = response.headers.get('em-paging-next-cursor')
            if paging_cursor is None:
                break
            query.fromCursor = paging_cursor

    def get_assetrelaties_by_filter(
        self,
        filter: dict,
        size: int = 100,
        order_by_property: str | None = None,
    ) -> Generator[dict[str, Any], None, None]:
        """Query assetrelaties via EMSON search endpoint."""
        query = Query(filters=filter, size=size, orderByProperty=order_by_property)
        while True:
            response = self.requester.post(url='api/otl/assetrelaties/search', data=query.json())
            if response.status_code != 200:
                raise ProcessLookupError(response.content.decode("utf-8"))

            yield from response.json().get('@graph', [])

            paging_cursor = response.headers.get('em-paging-next-cursor')
            if paging_cursor is None:
                break
            query.fromCursor = paging_cursor
