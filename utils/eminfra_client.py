from API.clients.eminfra.EMInfraClient import EMInfraClient as _Base


class EMInfraClient(_Base):


    def get_resource_by_cursor(self, resource, cursor=None, page_size=100, expansion_strings=None):
        from API.clients.eminfra.EMInfraDomain import QueryDTO, PagingModeEnum, ExpansionsDTO
        query = QueryDTO(size=page_size, from_=0, fromCursor=cursor, pagingMode=PagingModeEnum.CURSOR)
        if expansion_strings:
            query.expansions = ExpansionsDTO(fields=expansion_strings)
        while True:
            response = self.requester.post(url=f'core/api/otl/{resource}/search', data=query.json())
            if response.status_code != 200:
                raise Exception(response.content.decode("utf-8"))
            cursor = response.headers.get('em-paging-next-cursor')
            yield cursor, response.json().get('@graph', [])
            if cursor is None:
                break
            query.fromCursor = cursor
