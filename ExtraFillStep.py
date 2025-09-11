from API.EMInfraClient import EMInfraClient


class ExtraFillStep:
    def __init__(self, factory, eminfra_client: EMInfraClient):
        self.factory = factory
        self.eminfra_client: EMInfraClient = eminfra_client

    def execute(self):
        db = self.factory.create_connection()
        resources_to_fill = ['assettypes', 'vplankoppelingen', 'aansluitingen']
        for resource in resources_to_fill:
            params_resource = db.collection('params').get(f'fill_{resource}')
            if params_resource is None:
                db.collection('params').insert({'_key': f'fill_{resource}', 'fill': True, 'from': None})

        for resource in resources_to_fill:
            params_resource = db.collection('params').get(f'fill_{resource}')
            if not params_resource['fill']:
                continue
            self.fill_resource(resource, start_from=params_resource['from'])

    def fill_resource(self, resource: str, start_from):
        if resource == 'assettypes':
            db = self.factory.create_connection()
            query = """
            FOR ast IN assettypes
              SORT ast.uuid ASC
              RETURN ast.uuid
            """

            cursor = db.aql.execute(query)
            uuids_sorted = list(cursor)

            for ast_uuid in uuids_sorted:
                if start_from is not None and ast_uuid < start_from:
                    print(f'skipping {ast_uuid}')
                    continue
                ast_info = self.eminfra_client.get_kenmerktypes_by_asettype_uuid(ast_uuid)
                print(ast_info)

                continue
                db.aql.execute("""UPDATE @key WITH { from: @start_from } IN params""",
                               bind_vars={"key": f"fill_{resource}", "start_from": ast_uuid})