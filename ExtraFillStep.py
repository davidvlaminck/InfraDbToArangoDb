import logging
from API.EMInfraClient import EMInfraClient


class ExtraFillStep:
    def __init__(self, factory, eminfra_client: EMInfraClient):
        self.factory = factory
        self.eminfra_client = eminfra_client

    def execute(self):
        db = self.factory.create_connection()
        resources_to_fill = [
            'assettypes', 'vplankoppelingen', 'aansluitingrefs', 'aansluitingen',
            # derived edge collections
            'voedt_relaties', 'sturing_relaties', 'bevestiging_relaties', 'hoortbij_relaties'
        ]

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
            'voedt_relaties': self.fill_voedt_relaties,
            'sturing_relaties': self.fill_sturing_relaties,
            'bevestiging_relaties': self.fill_bevestiging_relaties,
            'hoortbij_relaties': self.fill_hoortbij_relaties,
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

    def _ensure_edge_collection(self, db, name: str):
        if not db.has_collection(name):
            db.create_collection(name, edge=True)

    def _mark_filled(self, db, params_key: str):
        db.aql.execute(
            "UPDATE @key WITH { from: @start_from, fill: @fill} IN params",
            bind_vars={"key": params_key, "start_from": None, "fill": False}
        )

    def _fill_derived_edges(self, db, params_key: str, edge_collection: str, relatietype_short: str):
        """(Re)build derived edges in edge_collection for a given relatietype.short.

        - Source edge: assetrelaties
        - Filters: correct relatietype, edge active, both endpoints active
        - Write: derived edge with _from/_to and source edge metadata
        """
        self._ensure_edge_collection(db, edge_collection)

        # Clear existing derived edges
        db.collection(edge_collection).truncate()

        rt_key_cursor = db.aql.execute(
            'FOR rt IN relatietypes FILTER rt.short == @short LIMIT 1 RETURN rt._key',
            bind_vars={'short': relatietype_short}
        )
        rt_key = next(iter(rt_key_cursor), None)
        if rt_key is None:
            logging.warning("⚠️ Could not find relatietype '%s'. Leaving %s empty.", relatietype_short, edge_collection)
            self._mark_filled(db, params_key)
            return

        logging.info("🔄 Building %s derived edges for relatietype '%s'...", edge_collection, relatietype_short)
        db.aql.execute(
            f"""
            LET rt_key = FIRST(FOR rt IN relatietypes FILTER rt.short == @short LIMIT 1 RETURN rt._key)
            FOR e IN assetrelaties
              FILTER e.relatietype_key == rt_key
              FILTER e.AIMDBStatus_isActief == true
              LET a_from = DOCUMENT(e._from)
              LET a_to   = DOCUMENT(e._to)
              FILTER a_from != null && a_to != null
              FILTER a_from.AIMDBStatus_isActief == true
              FILTER a_to.AIMDBStatus_isActief == true
              INSERT {{
                _from: e._from,
                _to: e._to,
                source_edge_id: e._id,
                source_edge_key: e._key
              }} INTO {edge_collection}
            """,
            bind_vars={'short': relatietype_short}
        )

        count = next(iter(db.aql.execute(f'RETURN LENGTH({edge_collection})')), None)
        logging.info("✅ %s built. Edge count: %s", edge_collection, count)
        self._mark_filled(db, params_key)

    def fill_voedt_relaties(self, start_from, db, params):
        """(Re)build derived Voedt-only edges between active assets.

        - Source edge: `assetrelaties`
        - Target edge collection: `voedt_relaties`
        - Filter: relationtype == Voedt AND edge active AND both endpoints active
        """
        # This fill step is intentionally not incremental: it rebuilds to keep it consistent
        # with assets/assetrelaties.
        self._ensure_edge_collection(db, 'voedt_relaties')

        # Clear existing derived edges
        db.collection('voedt_relaties').truncate()

        # Resolve Voedt relatietype key
        voedt_key_cursor = db.aql.execute(
            'FOR rt IN relatietypes FILTER rt.short == "Voedt" LIMIT 1 RETURN rt._key'
        )
        voedt_key = next(iter(voedt_key_cursor), None)
        if voedt_key is None:
            logging.warning("⚠️ Could not find relatietype 'Voedt'. Leaving voedt_relaties empty.")
            self._mark_filled(db, "fill_voedt_relaties")
            return

        logging.info("🔄 Building voedt_relaties derived edges...")
        db.aql.execute(
            """
            LET voedt_key = FIRST(FOR rt IN relatietypes FILTER rt.short == "Voedt" LIMIT 1 RETURN rt._key)
            FOR e IN assetrelaties
              FILTER e.relatietype_key == voedt_key
              FILTER e.AIMDBStatus_isActief == true
              LET a_from = DOCUMENT(e._from)
              LET a_to   = DOCUMENT(e._to)
              FILTER a_from != null && a_to != null
              FILTER a_from.AIMDBStatus_isActief == true
              FILTER a_to.AIMDBStatus_isActief == true
              INSERT {
                _from: e._from,
                _to: e._to,
                source_edge_id: e._id,
                source_edge_key: e._key
              } INTO voedt_relaties
            """,
            bind_vars={"voedt_key": voedt_key}
        )

        count = next(iter(db.aql.execute('RETURN LENGTH(voedt_relaties)')), None)
        logging.info("✅ voedt_relaties built. Edge count: %s", count)

        self._mark_filled(db, "fill_voedt_relaties")

    def fill_sturing_relaties(self, start_from, db, params):
        self._fill_derived_edges(db, 'fill_sturing_relaties', 'sturing_relaties', 'Sturing')

    def fill_bevestiging_relaties(self, start_from, db, params):
        self._fill_derived_edges(db, 'fill_bevestiging_relaties', 'bevestiging_relaties', 'Bevestiging')

    def fill_hoortbij_relaties(self, start_from, db, params):
        self._fill_derived_edges(db, 'fill_hoortbij_relaties', 'hoortbij_relaties', 'HoortBij')
