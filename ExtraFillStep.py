import logging
from API.EMInfraClient import EMInfraClient

class ExtraFillStep:
    def __init__(self, factory, eminfra_client: EMInfraClient):
        self.factory = factory
        self.eminfra_client = eminfra_client

    def execute(self):
        db = self.factory.create_connection()
        resources_to_fill = ['assettypes', 'vplankoppelingen', 'aansluitingrefs', 'aansluitingen']

        # Ensure fill params exist
        params = db.collection('params')
        for resource in resources_to_fill:
            if params.get(f'fill_{resource}') is None:
                params.insert({'_key': f'fill_{resource}', 'fill': True, 'from': None})

        # Fill each resource if needed
        for resource in resources_to_fill:
            params_resource = params.get(f'fill_{resource}')
            if params_resource and params_resource['fill']:
                self.fill_resource(resource, start_from=params_resource['from'])

    def fill_resource(self, resource: str, start_from):
        db = self.factory.create_connection()
        params = db.collection('params')

        # Map resource names to their fill functions
        fill_functions = {
            'assettypes': self.fill_assettypes,
            'vplankoppelingen': self.fill_vplankoppelingen,
            'aansluitingrefs': self.fill_aansluitingrefs,
            'aansluitingen': self.fill_aansluitingen,
        }

        if resource in fill_functions:
            fill_functions[resource](start_from, db, params)
        else:
            raise NotImplementedError(f"Resource '{resource}' not implemented for insertion.")

    def fill_assettypes(self, start_from, db, params):
        query = "FOR ast IN assettypes RETURN ast.uuid"
        uuids_sorted = sorted(db.aql.execute(query))
        for ast_uuid in uuids_sorted:
            if start_from and ast_uuid < start_from:
                logging.info(f"⏭️ Skipping {ast_uuid}")
                continue
            ast_info = self.eminfra_client.get_kenmerktypes_by_asettype_uuid(ast_uuid)
            logging.info(f"🔄 Updating {ast_uuid}")
            vplan_kenmerk = next((k for k in ast_info if k['kenmerkType']['naam'] == 'Vplan'), None)
            ean_kenmerk = next((k for k in ast_info if k['kenmerkType']['naam'] == 'Elektrisch aansluitpunt'), None)
            db.aql.execute(
                "UPDATE @key WITH { vplan_kenmerk: @vplan_kenmerk } IN assettypes",
                bind_vars={"key": ast_uuid[:8], "vplan_kenmerk": vplan_kenmerk is not None}
            )
            db.aql.execute(
                "UPDATE @key WITH { aansluitpunt_kenmerk: @aansluitpunt_kenmerk } IN assettypes",
                bind_vars={"key": ast_uuid[:8], "aansluitpunt_kenmerk": ean_kenmerk is not None}
            )
            db.aql.execute(
                "UPDATE @key WITH { from: @start_from } IN params",
                bind_vars={"key": f"fill_assettypes", "start_from": ast_uuid}
            )
        logging.info("✅ No more data for assettypes. Marking as filled.")
        db.aql.execute(
            "UPDATE @key WITH { from: @start_from, fill: @fill} IN params",
            bind_vars={"key": "fill_assettypes", "start_from": None, "fill": False}
        )

    def fill_vplankoppelingen(self, start_from, db, params):
        query = """
          FOR asset IN assets
            FOR atype IN assettypes
              FILTER asset.assettype_key == atype._key
              FILTER atype.vplan_kenmerk == true
              RETURN asset._key
        """
        uuids_sorted = sorted(db.aql.execute(query))
        for asset_uuid in uuids_sorted:
            if start_from and asset_uuid < start_from:
                logging.info(f"⏭️ Skipping vplankoppelingen for {asset_uuid}")
                continue
            logging.info(f"🔄 Updating vplankoppelingen for {asset_uuid}")
            vplan_info = self.eminfra_client.get_vplannen_by_asset_uuid(asset_uuid)
            koppelingen_to_add = [
                {
                    "asset_key": asset_uuid,
                    "vplankoppeling_uuid": v['uuid'],
                    "vplan_uuid": v['vplanRef']['uuid'],
                    "vplan_nummer": v['vplanRef']['nummer'],
                    "inDienstDatum": v.get('inDienstDatum'),
                    "uitDienstDatum": v.get('uitDienstDatum')
                }
                for v in vplan_info
            ]
            if koppelingen_to_add:
                db.aql.execute(
                    """
                    FOR koppeling IN @koppelingen
                      UPSERT { _key: koppeling.vplankoppeling_uuid }
                      INSERT {
                        _key: koppeling.vplankoppeling_uuid,
                        asset_key: koppeling.asset_key,
                        vplankoppeling_uuid: koppeling.vplankoppeling_uuid,
                        vplan_uuid: koppeling.vplan_uuid,
                        vplan_nummer: koppeling.vplan_nummer,
                        inDienstDatum: koppeling.inDienstDatum,
                        uitDienstDatum: koppeling.uitDienstDatum
                      }
                      UPDATE {}
                      IN vplankoppelingen
                    """,
                    bind_vars={"koppelingen": koppelingen_to_add}
                )
            db.aql.execute(
                "UPDATE @key WITH { from: @start_from } IN params",
                bind_vars={"key": "fill_vplankoppelingen", "start_from": asset_uuid}
            )
        logging.info("✅ No more data for vplankoppelingen. Marking as filled.")
        db.aql.execute(
            "UPDATE @key WITH { from: @start_from, fill: @fill} IN params",
            bind_vars={"key": "fill_vplankoppelingen", "start_from": None, "fill": False}
        )

    def fill_aansluitingrefs(self, start_from, db, params):
        logging.info("No more data for aansluitingrefs. Marking as filled.")
        db.aql.execute(
            "UPDATE @key WITH { from: @start_from, fill: @fill} IN params",
            bind_vars={"key": "fill_aansluitingrefs", "start_from": None, "fill": False}
        )

    def fill_aansluitingen(self, start_from, db, params):
        logging.info("No more data for aansluitingen. Marking as filled.")
        db.aql.execute(
            "UPDATE @key WITH { from: @start_from, fill: @fill} IN params",
            bind_vars={"key": "fill_aansluitingen", "start_from": None, "fill": False}
        )
