import logging
from datetime import datetime, timezone

from API.EMInfraClient import EMInfraClient
from API.EMSONClient import EMSONClient
from Enums import ResourceEnum
from ResourceEnum import colorama_table

from shapely import wkt
from shapely.ops import transform
from shapely.geometry import mapping
from pyproj import Transformer
import json


class InitialFillStep:
    def __init__(self, factory, eminfra_client: EMInfraClient, emson_client: EMSONClient):
        self.factory = factory
        self.eminfra_client = eminfra_client
        self.emson_client = emson_client
        self.default_page_size = 100
        self.assettype_lookup = None
        self.transformer = Transformer.from_crs("EPSG:31370", "EPSG:4326", always_xy=True)

    def execute(self, fill_resources: set[ResourceEnum]):
        db = self.factory.create_connection()
        docs = self._get_docs_to_update(db)
        if any(docs):
            docs_to_update = self._build_docs_to_update(docs)
            self._update_params_collection(db, docs_to_update)
        self.fill_tables(fill_resources=fill_resources)

    def _get_docs_to_update(self, db):
        """Fetches documents from the 'params' collection where page == -1."""
        cursor = db.aql.execute("""
            FOR doc IN params
                FILTER doc.page == -1
                RETURN doc
        """)
        return list(cursor)

    def _build_docs_to_update(self, docs):
        """Builds a list of documents to update in the 'params' collection."""
        docs_to_update = []
        for feed_name in (d['_key'][5:] for d in docs):
            logging.debug(feed_name)
            resource_page = self.eminfra_client.get_last_feedproxy_page(feed_name)
            self_page = next(p for p in resource_page['links'] if p['rel'] == 'self')
            page_number = self_page['href'].split('/')[1]
            last_entry = sorted(
                iter(resource_page['entries']),
                key=lambda p: datetime.fromisoformat(p['updated']).astimezone(
                    timezone.utc))[-1]
            logging.debug(last_entry['id'])
            docs_to_update.append({'_id': f'params/feed_{feed_name}', 'page': int(page_number),
                                   'event_uuid': last_entry['id']})
        return docs_to_update

    def _update_params_collection(self, db, docs_to_update):
        """Updates the 'params' collection with the provided documents."""
        db.collection("params").update_many(docs_to_update)

    def fill_tables(self, fill_resources):
        for resource in fill_resources:
            color = colorama_table[resource]
            logging.info(f'{color}Filling {resource.value} table')
            try:
                self._fill_resource(resource.value)
            except Exception as e:
                logging.error(f'{color}Error filling {resource.value}: {e}')
                raise

    def _fill_resource(self, resource: str):
        if resource in {ResourceEnum.assettypes.value}:
            self._fill_resource_using_em_infra(resource)
        else:
            self._fill_resource_using_emson(resource)

    def _fill_resource_using_emson(self, resource: str):
        color = colorama_table[resource]
        logging.info(f"{color}Filling resource: {resource}")
        db = self.factory.create_connection()
        params_resource = db.collection('params').get(f'fill_{resource}')
        if params_resource is None:
            db.collection('params').insert({'_key': f'fill_{resource}', 'fill': True, 'from': None})
        params_resource = db.collection('params').get(f'fill_{resource}')
        if not params_resource['fill']:
            logging.info(f"{color}Skipping {resource}, already filled.")
            return

        cursor = params_resource['from']
        while params_resource['fill']:

            for cursor, dicts in self.emson_client.get_resource_by_cursor(resource, cursor=cursor):
                if dicts:
                    self._insert_resource_data(db, resource, dicts)
                    db.aql.execute("""UPDATE @key WITH { from: @start_from } IN params""",
                                   bind_vars={"key": f"fill_{resource}", "start_from": cursor})
                    logging.info(f"{color}Inserted {len(dicts)} records for {resource}. Next cursor: {cursor}")

            if cursor is None:
                logging.info(f"{color}No more data for {resource}. Marking as filled.")
                db.aql.execute("""UPDATE @key WITH { from: @start_from, fill: @fill} IN params""",
                               bind_vars={"key": f"fill_{resource}", "start_from": None, "fill": False})
                break
        if cursor is None:
            logging.info(f"{color}No more data for {resource}. Marking as filled.")
            db.aql.execute("""UPDATE @key WITH { from: @start_from, fill: @fill} IN params""",
                           bind_vars={"key": f"fill_{resource}", "start_from": None, "fill": False})



    def _fill_resource_using_em_infra(self, resource: str):
        color = colorama_table[resource]
        logging.info(f"{color}Filling resource: {resource}")
        db = self.factory.create_connection()
        params_resource = db.collection('params').get(f'fill_{resource}')
        if params_resource is None:
            db.collection('params').insert({'_key': f'fill_{resource}', 'fill': True, 'from': None})
        params_resource = db.collection('params').get(f'fill_{resource}')
        if not params_resource['fill']:
            logging.info(f"{color}Skipping {resource}, already filled.")
            return

        start_from = params_resource.get('from')
        while params_resource['fill']:
            page_size = self.default_page_size
            for cursor, dicts in self.eminfra_client.get_resource_page(resource, page_size, start_from):
                if dicts:
                    self._insert_resource_data(db, resource, dicts)
                    start_from = cursor
                    db.aql.execute("""UPDATE @key WITH { from: @start_from } IN params""",
                                   bind_vars={"key": f"fill_{resource}", "start_from": start_from})
                    # TODO remove after done
                    result = db.aql.execute(f"""RETURN LENGTH({resource})""")
                    count = list(result)[0]
                    logging.debug(f"{color}Total records in {resource} collection: {count}")

                    logging.info(f"{color}Inserted {len(dicts)} records for {resource}. Next cursor: {cursor}")
                if cursor is None:
                    logging.info(f"{color}No more data for {resource}. Marking as filled.")
                    db.aql.execute("""UPDATE @key WITH { from: @start_from, fill: @fill} IN params""",
                                   bind_vars={"key": f"fill_{resource}", "start_from": None, "fill": False})
                    return


    def _insert_resource_data(self, db, resource, dicts):
        if resource == ResourceEnum.assettypes.value:
            collection = db.collection('assettypes')
            docs_to_insert = [
                {
                    "_key": record["uuid"][:8],
                    "uuid": record["uuid"],
                    "name": record["naam"],
                    "label": record["afkorting"],
                    "uri": record["uri"],
                    "definitie": record["definitie"],
                    "actief": record["actief"]
                }
                for record in dicts
            ]

            # Bulk insert with overwrite (optional)
            collection.import_bulk(docs_to_insert, overwrite=False, on_duplicate="update")
        elif resource == ResourceEnum.assets.value:
            collection = db.collection('assets')
            if self.assettype_lookup is None:
                self.assettype_lookup = {
                    at["uri"]: at["_key"]
                    for at in db.collection('assettypes')  # your list of 100 assettype dicts
                }


            docs_to_insert = []
            for obj in dicts:
                try:
                    obj = self._transform_keys(obj)
                    uri = obj["@type"]


                    obj["_key"] = obj.get("@id").split("/")[-1][:36]

                    wkt_string = None
                    # extract wkt string
                    if 'geo' in obj and obj['geo']['Geometrie_log']:
                        geometrie_dict = obj['geo']['Geometrie_log'][0]['DtcLog_geometrie']
                        wkt_string = next(iter(geometrie_dict.values()))
                    elif 'loc' in obj:
                        if 'Locatie_geometrie' in obj['loc'] and obj['loc']['Locatie_geometrie'] != '':
                            wkt_string = obj['loc']['Locatie_geometrie']
                        elif 'Locatie_puntlocatie' in obj['loc'] and obj['loc']['Locatie_puntlocatie'] != '' and '3Dpunt_puntgeometrie' in obj['loc']['Locatie_puntlocatie'] and obj['loc']['Locatie_puntlocatie']['3Dpunt_puntgeometrie'] != '':
                            coords = obj['loc']['Locatie_puntlocatie']['3Dpunt_puntgeometrie']['DtcCoord.lambert72']
                            wkt_string = f"POINT Z ({coords['DtcCoordLambert72.xcoordinaat']} {coords['DtcCoordLambert72.ycoordinaat']} {coords['DtcCoordLambert72.zcoordinaat']})"
                    if wkt_string is not None:
                        obj['wkt'] = wkt_string
                        geom = wkt.loads(wkt_string)
                        geom_wgs84 = transform(self.transformer.transform, geom)
                        geojson = mapping(geom_wgs84)
                        obj['geometry'] = geojson

                    asset_key = self.assettype_lookup.get(uri)
                    if asset_key:
                        obj["assettype_key"] = asset_key
                        docs_to_insert.append(obj)
                    else:
                        print(f"⚠️ No matching assettype for URI: {uri}")
                        # retry by raising a specific exception
                        # check if assettypes is still filling here, so you can wait for it to finish
                except Exception as e:
                    logging.error(f"Error processing asset {obj.get('@id', 'unknown')}: {e}")
                    raise e

            # for assets:
            # make ns a separate nested field and remove it from the attributes
            # replace "." with "_"
            collection.import_bulk(docs_to_insert, overwrite=False, on_duplicate="update")

    @staticmethod
    def _transform_keys(data):
        def process(obj, depth=0):
            if isinstance(obj, dict):
                result = {}
                for key, value in obj.items():
                    value = process(value, depth + 1)

                    # Only split namespace at top level
                    if depth == 0 and ":" in key:
                        ns, field = key.split(":", 1)
                        field = field.replace(".", "_")
                        if ns not in result:
                            result[ns] = {}
                        result[ns][field] = value
                    else:
                        clean_key = key.split(":", 1)[-1].replace(".", "_")
                        result[clean_key] = value
                return result

            elif isinstance(obj, list):
                return [process(item, depth) for item in obj]

            else:
                return obj

        return process(data)
