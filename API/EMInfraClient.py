from collections.abc import Generator
from typing import Any, Optional

from API.EMInfraDomain import Query, TermDTO, OperatorEnum, QueryDTO, PagingModeEnum, ExpansionsDTO, SelectionDTO, \
    ExpressionDTO
from API.APIEnums import AuthType, Environment
from API.RequesterFactory import RequesterFactory


class EMInfraClient:
    """Client for the EMInfra endpoints.

    This covers:
    - feedproxy
    - core/api resources (offset paging)
    - identiteit/api resources (offset paging)
    - cursor based OTL endpoints under core/api/otl
    """

    def __init__(self, auth_type: AuthType, env: Environment, settings: dict | None = None, cookie: str | None = None):
        self.requester = RequesterFactory.create_requester(auth_type=auth_type, env=env, settings=settings, cookie=cookie)
        self.requester.first_part_url += 'eminfra/'

    def get_last_feedproxy_page(self, feed_name: str) -> dict[str, Any]:
        url = f"feedproxy/feed/{feed_name}"
        return self.requester.get(url).json()

    def get_feedproxy_page(self, feed_name: str, page_num: int, page_size: int = 1) -> dict[str, Any]:
        url = f"feedproxy/feed/{feed_name}/{page_num}/{page_size}"
        return self.requester.get(url).json()

    def get_resource_page(self, resource: str, page_size: int, start_from: Optional[int]):
        """Offset-based paging for core/api/<resource>."""
        if not start_from:
            start_from = 0

        while True:
            url = f"core/api/{resource}?from={start_from}&pagingMode=OFFSET&size={page_size}"
            json_dict = self.requester.get(url).json()
            start_from = json_dict['from'] + json_dict['size']
            if start_from >= json_dict['totalCount']:
                yield None, json_dict['data']
                break
            else:
                yield start_from, json_dict['data']

    def get_kenmerktypes_by_asettype_uuid(self, assettype_uuid: str) -> list[dict[str, Any]]:
        url = f"core/api/assettypes/{assettype_uuid}/kenmerktypes"
        return self.requester.get(url).json()['data']

    def get_vplannen_by_asset_uuid(self, asset_uuid: str) -> list[dict[str, Any]]:
        url = f"core/api/assets/{asset_uuid}/kenmerken/9f12fd85-d4ae-4adc-952f-5fa6e9d0ffb7/vplannen"
        return self.requester.get(url).json()['data']

    def get_identity_resource_page(self, resource: str, page_size: int, start_from: Optional[int]):
        """Offset-based paging for identiteit/api/<resource>."""
        if not start_from:
            start_from = 0

        while True:
            url = f"identiteit/api/{resource}?from={start_from}&pagingMode=OFFSET&size={page_size}"
            json_dict = self.requester.get(url).json()
            start_from = json_dict['from'] + json_dict['size']
            if start_from >= json_dict['totalCount']:
                yield None, json_dict['data']
                break
            else:
                yield start_from, json_dict['data']

    def get_resource_by_cursor(
        self,
        resource: str,
        cursor: Optional[str],
        page_size: int = 100,
        expansion_strings: Optional[list[str]] = None,
    ) -> Generator[tuple[Optional[str], list[dict[str, Any]]], None, None]:
        """Cursor-based paging for core/api/otl/<resource>/search."""
        query = Query(filters={}, size=page_size, fromCursor=cursor)
        if expansion_strings:
            query.add_expansions(expansion_strings)
        while True:
            response = self.requester.post(url=f'core/api/otl/{resource}/search', data=query.json())
            if response.status_code != 200:
                raise ProcessLookupError(response.content.decode("utf-8"))
            cursor = response.headers.get('em-paging-next-cursor')
            yield cursor, response.json().get('@graph', [])
            if cursor is None:
                break
            query.fromCursor = cursor

    def get_assets_by_assettype_uuids(
        self,
        assettype_uuids: list[str],
        cursor: str | None = None,
        page_size: int = 100,
        expansion_strings: list[str] | None = None,
    ) -> Generator[tuple[Optional[str], list[dict[str, Any]]], None, None]:
        type_term = TermDTO(property='type', operator=OperatorEnum.IN, value=assettype_uuids)
        query_dto = QueryDTO(
            size=page_size,
            pagingMode=PagingModeEnum.CURSOR,
            fromCursor=cursor,
            expansions=ExpansionsDTO(fields=expansion_strings),
            selection=SelectionDTO(expressions=[ExpressionDTO(terms=[type_term])]),
        )
        while True:
            response = self.requester.post(url=f'core/api/assets/search', data=query_dto.json())
            if response.status_code != 200:
                raise ProcessLookupError(response.content.decode("utf-8"))
            json_response = response.json()
            cursor = json_response.get('next')
            yield cursor, json_response['data']
            if cursor is None:
                break
            query_dto.fromCursor = cursor

    def test_connection(self) -> dict[str, Any]:
        return self.requester.get("core/api/gebruikers/ik").json()
