from collections.abc import Generator

from API.EMInfraDomain import Query
from API.Enums import AuthType, Environment
from API.RequesterFactory import RequesterFactory


class EMInfraClient:
    def __init__(self, auth_type: AuthType, env: Environment, settings: dict = None, cookie: str = None):
        self.requester = RequesterFactory.create_requester(auth_type=auth_type, env=env, settings=settings,
                                                           cookie=cookie)
        self.requester.first_part_url += 'eminfra/'

    def get_last_feedproxy_page(self, feed_name: str) -> dict:
        url = f"feedproxy/feed/{feed_name}"
        json_dict = self.requester.get(url).json()
        print(json_dict)
        return json_dict

    def get_feedproxy_page(self, feed_name: str, page_num: int, page_size: int = 1) -> dict:
        url = f"feedproxy/feed/{feed_name}/{page_num}/{page_size}"
        return self.requester.get(url).json()

    def get_resource_page(self, resource: str, page_size: int, start_from: int):
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

    def get_kenmerktypes_by_asettype_uuid(self, assettype_uuid: str):
        url = f"core/api/assettypes/{assettype_uuid}/kenmerktypes"
        return self.requester.get(url).json()['data']

    def get_vplannen_by_asset_uuid(self, asset_uuid: str):
        url = f"core/api/assets/{asset_uuid}/kenmerken/9f12fd85-d4ae-4adc-952f-5fa6e9d0ffb7/vplannen"
        return self.requester.get(url).json()['data']

    def get_aansluiting_by_asset_uuid(self, asset_uuid: str):
        url = f"core/api/assets/{asset_uuid}/kenmerken/87dff279-4162-4031-ba30-fb7ffd9c014b"
        return self.requester.get(url).json()

    def get_identity_resource_page(self, resource: str, page_size: int, start_from: int):
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

    def get_resource_by_cursor(self, resource: str, cursor: str, page_size: int = 100, expansion_strings: list[str] = None) -> Generator[tuple[str, dict]]:
        query = Query(filters={}, size=page_size, fromCursor=cursor)
        if expansion_strings:
            query.add_expansions(expansion_strings)
        while True:
            response = self.requester.post(url=f'core/api/otl/{resource}/search', data=query.json())
            if response.status_code != 200:
                print(response)
                raise ProcessLookupError(response.content.decode("utf-8"))
            cursor = response.headers.get('em-paging-next-cursor')
            yield cursor, response.json()['@graph']
            if cursor is None:
                break
            query.fromCursor = cursor

    def test_connection(self):
        return self.requester.get("core/api/gebruikers/ik").json()
