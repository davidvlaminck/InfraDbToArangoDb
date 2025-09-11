import logging

from API.EMInfraClient import EMInfraClient


class ExtraFillStep:
    def __init__(self, factory, eminfra_client: EMInfraClient):
        self.factory = factory
        self.eminfra_client: EMInfraClient = eminfra_client

    def execute(self):
        db = self.factory.create_connection()
        resources_to_fill = ['assettypes', 'vplankoppelingen', 'aansluitingrefs', 'aansluitingen']
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
              RETURN ast.uuid
            """

            cursor = db.aql.execute(query)
            uuids_sorted = sorted(cursor)

            for ast_uuid in uuids_sorted:
                if start_from is not None and ast_uuid < start_from:
                    print(f'⏭️ Skipping {ast_uuid}')
                    continue
                ast_info = self.eminfra_client.get_kenmerktypes_by_asettype_uuid(ast_uuid)
                print(f'🔄 Updating {ast_uuid}')
                vplan_kenmerk = next((filter(lambda k: k['kenmerkType']['naam'] == 'Vplan', ast_info)), None)
                ean_kenmerk = next((filter(lambda k: k['kenmerkType']['naam'] == 'Elektrisch aansluitpunt', ast_info)), None)
                db.aql.execute("""UPDATE @key WITH { vplan_kenmerk: @vplan_kenmerk } IN assettypes""",
                               bind_vars={"key": ast_uuid[:8], "vplan_kenmerk": vplan_kenmerk is not None})
                db.aql.execute("""UPDATE @key WITH { aansluitpunt_kenmerk: @aansluitpunt_kenmerk } IN assettypes""",
                               bind_vars={"key": ast_uuid[:8], "aansluitpunt_kenmerk": ean_kenmerk is not None})
                db.aql.execute("""UPDATE @key WITH { from: @start_from } IN params""",
                               bind_vars={"key": f"fill_{resource}", "start_from": ast_uuid})
            logging.info(f"✅ No more data for {resource}. Marking as filled.")
            db.aql.execute("""UPDATE @key WITH { from: @start_from, fill: @fill} IN params""",
                           bind_vars={"key": f"fill_{resource}", "start_from": None, "fill": False})

        elif resource == 'vplankoppelingen':
            db = self.factory.create_connection()
            query = """
              FOR asset IN assets
                FOR atype IN assettypes
                  FILTER asset.assettype_key == atype._key
                  FILTER atype.vplan_kenmerk == true
                  RETURN asset._key
            """

            cursor = db.aql.execute(query)
            uuids_sorted = list(cursor)

            for asset_uuid in sorted(uuids_sorted):
                if start_from is not None and asset_uuid < start_from:
                    print(f'⏭️ Skipping vplankoppelingen for {asset_uuid}')
                    continue

                print(f'🔄 Updating vplankoppelingen for {asset_uuid}')
                vplan_info = self.eminfra_client.get_vplannen_by_asset_uuid(asset_uuid)

                koppelingen_to_add = [{"asset_key": asset_uuid,
                       "vplankoppeling_uuid": v['uuid'],
                       "vplan_uuid": v['vplanRef']['uuid'],
                       "vplan_nummer": v['vplanRef']['nummer'],
                       "inDienstDatum": v.get('inDienstDatum', None),
                       "uitDienstDatum": v.get('uitDienstDatum', None)
                       } for v in vplan_info]


                query = """
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
                """

                if len(koppelingen_to_add) > 0:
                    db.aql.execute(
                        query,
                        bind_vars={"koppelingen": koppelingen_to_add}
                    )

                db.aql.execute("""UPDATE @key WITH { from: @start_from } IN params""",
                               bind_vars={"key": f"fill_{resource}", "start_from": asset_uuid})
            logging.info(f"✅ No more data for {resource}. Marking as filled.")
            db.aql.execute("""UPDATE @key WITH { from: @start_from, fill: @fill} IN params""",
                           bind_vars={"key": f"fill_{resource}", "start_from": None, "fill": False})
        elif resource == 'aansluitingrefs':
            db = self.factory.create_connection()
            params_resource = db.collection('params').get(f'fill_{resource}')
            if params_resource is None:
                db.collection('params').insert({'_key': f'fill_{resource}', 'fill': True, 'from': None})
            params_resource = db.collection('params').get(f'fill_{resource}')
            if not params_resource['fill']:
                logging.info(f"Skipping {resource}, already filled.")
                return

            start_from = params_resource.get('from')
            generator = self.eminfra_client.get_resource_page("elektriciteitsaansluitingrefs", 1000, start_from)

            while params_resource['fill']:
                for cursor, dicts in generator:
                    if not dicts:
                        continue
                    collection = db.collection('aansluitingrefs')
                    docs_to_insert = [
                        {
                            "_key": record["uuid"][:8],
                            "uuid": record["uuid"],
                            "amid": record["amid"],
                            "aansluitnummer": record["aansluitnummer"],
                            "ean": record.get("ean", None)
                        }
                        for record in dicts
                    ]

                    collection.import_bulk(docs_to_insert, overwrite=False, on_duplicate="update")

                    start_from = cursor
                    db.aql.execute("""UPDATE @key WITH { from: @start_from } IN params""",
                                   bind_vars={"key": f"fill_{resource}", "start_from": start_from})
                    result = db.aql.execute(f"""RETURN LENGTH({resource})""")
                    count = list(result)[0]
                    logging.debug(f"Total records in {resource} collection: {count}")

                    logging.info(f"Inserted {len(dicts)} records for {resource}. Next cursor: {cursor}")
                if start_from is None:
                    logging.info(f"No more data for {resource}. Marking as filled.")
                    db.aql.execute("""UPDATE @key WITH { from: @start_from, fill: @fill} IN params""",
                                   bind_vars={"key": f"fill_{resource}", "start_from": None, "fill": False})
                    return
        elif resource == 'aansluitingen':
            # TODO refactor to use /assets endpoint filtering on assettypes with aansluitpunt_kenmerk as expansion
            db = self.factory.create_connection()
            query = """
              FOR asset IN assets
                FOR atype IN assettypes
                  FILTER asset.assettype_key == atype._key
                  FILTER atype.aansluitpunt_kenmerk == true
                  RETURN asset._key
            """

            cursor = db.aql.execute(query)
            uuids_sorted = list(cursor)

            for asset_uuid in sorted(uuids_sorted):
                if start_from is not None and asset_uuid < start_from:
                    print(f'⏭️ Skipping aansluitingen for {asset_uuid}')
                    continue

                aansluiting_info = self.eminfra_client.get_aansluiting_by_asset_uuid(asset_uuid)
                if 'elektriciteitsAansluitingRef' not in aansluiting_info:
                    print(f'⚠️ No aansluiting info for {asset_uuid}, skipping')
                    db.aql.execute("""UPDATE @key WITH { from: @start_from } IN params""",
                                   bind_vars={"key": f"fill_{resource}", "start_from": asset_uuid})
                    continue
                print(f'🔄 Updating aansluitingen for {asset_uuid}')
                query = """
                INSERT {_key: @prim_key, asset_key: @asset_key, aansluiting_key: @aansluiting_key} IN aansluitingen
                """
                aansluiting_key = aansluiting_info['elektriciteitsAansluitingRef']['uuid'][:8]
                db.aql.execute(query,
                               bind_vars={"prim_key": f'{asset_uuid}_{aansluiting_key}', "asset_key": asset_uuid,
                                          "aansluiting_key": aansluiting_key})

                db.aql.execute("""UPDATE @key WITH { from: @start_from } IN params""",
                               bind_vars={"key": f"fill_{resource}", "start_from": asset_uuid})
            logging.info(f"✅ No more data for {resource}. Marking as filled.")
            db.aql.execute("""UPDATE @key WITH { from: @start_from, fill: @fill} IN params""",
                           bind_vars={"key": f"fill_{resource}", "start_from": None, "fill": False})
        else:
            raise NotImplementedError(f"Resource '{resource}' not implemented for insertion.")