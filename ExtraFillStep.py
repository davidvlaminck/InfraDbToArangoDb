import logging
from typing import Callable, Optional

from API.EMInfraClient import EMInfraClient


class ExtraFillStep:
    """Extra (post) fill step.

    This step runs after the main initial fill. It enriches existing collections and builds a few
    derived edge collections.

    It is resumable: progress is stored in the `params` collection (`fill_<resource>` docs).
    """

    # resources that can be executed by this step (in order)
    RESOURCES_TO_FILL = [
        'assettypes',
        'vplankoppelingen',
        'aansluitingrefs',
        'aansluitingen',
        # derived edge collections
        'voedt_relaties',
        'sturing_relaties',
        'bevestiging_relaties',
        'hoortbij_relaties',
    ]

    def __init__(self, factory, eminfra_client: EMInfraClient):
        self.factory = factory
        self.eminfra_client = eminfra_client

        # Map resource names to their fill functions
        self._fill_functions: dict[str, Callable[[Optional[str], object, object], None]] = {
            'assettypes': self.fill_assettypes,
            'vplankoppelingen': self.fill_vplankoppelingen,
            'aansluitingrefs': self.fill_aansluitingrefs,
            'aansluitingen': self.fill_aansluitingen,
            'voedt_relaties': self.fill_voedt_relaties,
            'sturing_relaties': self.fill_sturing_relaties,
            'bevestiging_relaties': self.fill_bevestiging_relaties,
            'hoortbij_relaties': self.fill_hoortbij_relaties,
        }

    def execute(self):
        """Run all configured extra resources once.

        Each resource has an entry in `params` named `fill_<resource>` which contains:
        - fill: bool (whether the resource still needs to run)
        - from: last processed key/cursor (optional)
        """
        db = self.factory.create_connection()
        self._ensure_fill_params(db)

        params = db.collection('params')
        for resource in self.RESOURCES_TO_FILL:
            params_doc = params.get(f'fill_{resource}')
            if not params_doc or not params_doc.get('fill', True):
                continue

            start_from = params_doc.get('from')
            self.fill_resource(resource, start_from=start_from)

    def _ensure_fill_params(self, db) -> None:
        """Make sure `params/fill_<resource>` documents exist."""
        params = db.collection('params')
        for resource in self.RESOURCES_TO_FILL:
            if params.get(f'fill_{resource}') is None:
                params.insert({'_key': f'fill_{resource}', 'fill': True, 'from': None})

    def fill_resource(self, resource: str, start_from):
        """Dispatch to the correct fill_... method."""
        db = self.factory.create_connection()
        params = db.collection('params')

        func = self._fill_functions.get(resource)
        if func is None:
            raise NotImplementedError(f"Resource '{resource}' not implemented for insertion.")

        func(start_from, db, params)

    def _update_progress(self, db, params_key: str, start_from) -> None:
        db.aql.execute(
            "UPDATE @key WITH { from: @start_from } IN params",
            bind_vars={"key": params_key, "start_from": start_from},
        )

    def _mark_filled(self, db, params_key: str) -> None:
        db.aql.execute(
            "UPDATE @key WITH { from: @start_from, fill: @fill} IN params",
            bind_vars={"key": params_key, "start_from": None, "fill": False},
        )

    def fill_assettypes(self, start_from, db, params):
        """Update `assettypes` docs with two boolean flags.

        For each assettype uuid we call EMInfra to see if it has these kenmerken:
        - Vplan
        - Elektrisch aansluitpunt

        Progress is stored as the last processed assettype uuid.
        """
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
                bind_vars={"key": ast_uuid[:8], "vplan_kenmerk": vplan_kenmerk is not None},
            )
            db.aql.execute(
                "UPDATE @key WITH { aansluitpunt_kenmerk: @aansluitpunt_kenmerk } IN assettypes",
                bind_vars={"key": ast_uuid[:8], "aansluitpunt_kenmerk": ean_kenmerk is not None},
            )

            self._update_progress(db, "fill_assettypes", ast_uuid)

        logging.info("✅ No more data for assettypes. Marking as filled.")
        self._mark_filled(db, "fill_assettypes")

    def fill_vplankoppelingen(self, start_from, db, params):
        """Fill collection `vplankoppelingen` by asking EMInfra per eligible asset.

        Eligibility: asset's type has `assettypes.vplan_kenmerk == true`.

        Progress is stored as the last processed asset _key.
        """
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
                    "uitDienstDatum": v.get('uitDienstDatum'),
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
                    bind_vars={"koppelingen": koppelingen_to_add},
                )

            self._update_progress(db, "fill_vplankoppelingen", asset_uuid)

        logging.info("✅ No more data for vplankoppelingen. Marking as filled.")
        self._mark_filled(db, "fill_vplankoppelingen")

    def fill_aansluitingrefs(self, start_from, db, params):
        """Placeholder: nothing to do yet."""
        logging.info("No more data for aansluitingrefs. Marking as filled.")
        self._mark_filled(db, "fill_aansluitingrefs")

    def fill_aansluitingen(self, start_from, db, params):
        """Placeholder: nothing to do yet."""
        logging.info("No more data for aansluitingen. Marking as filled.")
        self._mark_filled(db, "fill_aansluitingen")

    def _ensure_edge_collection(self, db, name: str):
        """Create an edge collection if needed and ensure basic traversal indexes."""
        if not db.has_collection(name):
            db.create_collection(name, edge=True)

        col = db.collection(name)
        # add_persistent_index is idempotent; Arango will ignore duplicates
        col.add_persistent_index(fields=['_from'], unique=False, sparse=False)
        col.add_persistent_index(fields=['_to'], unique=False, sparse=False)

    def _fill_derived_edges(self, db, params_key: str, edge_collection: str, relatietype_short: str):
        """(Re)build derived edges in edge_collection for a given relatietype.short.

        This is a set-based rebuild:
        - truncate edge collection
        - insert all matching edges from `assetrelaties`

        We'll only include edges where both endpoints still exist and are active.
        """
        self._ensure_edge_collection(db, edge_collection)

        # Clear existing derived edges
        db.collection(edge_collection).truncate()

        rt_key = next(
            iter(
                db.aql.execute(
                    'FOR rt IN relatietypes FILTER rt.short == @short LIMIT 1 RETURN rt._key',
                    bind_vars={'short': relatietype_short},
                    batch_size=1,
                    stream=True,
                )
            ),
            None,
        )
        if rt_key is None:
            logging.warning("⚠️ Could not find relatietype '%s'. Leaving %s empty.", relatietype_short, edge_collection)
            self._mark_filled(db, params_key)
            return

        logging.info("🔄 Building %s derived edges for relatietype '%s'...", edge_collection, relatietype_short)

        db.aql.execute(
            """
            LET rt_key = @rt_key
            FOR e IN assetrelaties
              FILTER e.relatietype_key == rt_key
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
              } INTO @@edge_collection
              OPTIONS { ignoreErrors: true }
            """,
            bind_vars={'rt_key': rt_key, '@edge_collection': edge_collection},
            batch_size=5000,
            stream=True,
        )

        count = next(iter(db.aql.execute('RETURN COUNT(FOR x IN @@c RETURN 1)', bind_vars={'@c': edge_collection})), None)
        logging.info("✅ %s built. Edge count: %s", edge_collection, count)
        self._mark_filled(db, params_key)

    def fill_voedt_relaties(self, start_from, db, params):
        self._fill_derived_edges(db, 'fill_voedt_relaties', 'voedt_relaties', 'Voedt')

    def fill_sturing_relaties(self, start_from, db, params):
        self._fill_derived_edges(db, 'fill_sturing_relaties', 'sturing_relaties', 'Sturing')

    def fill_bevestiging_relaties(self, start_from, db, params):
        self._fill_derived_edges(db, 'fill_bevestiging_relaties', 'bevestiging_relaties', 'Bevestiging')

    def fill_hoortbij_relaties(self, start_from, db, params):
        self._fill_derived_edges(db, 'fill_hoortbij_relaties', 'hoortbij_relaties', 'HoortBij')
