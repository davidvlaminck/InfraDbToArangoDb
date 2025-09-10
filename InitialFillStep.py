import logging
import time
import uuid
from concurrent.futures import ProcessPoolExecutor, as_completed, ThreadPoolExecutor
from datetime import datetime, timezone, date

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
    default_page_size = 1000

    def __init__(self, factory, eminfra_client: EMInfraClient, emson_client: EMSONClient):
        self.factory = factory
        self.eminfra_client: EMInfraClient = eminfra_client
        self.emson_client: EMSONClient = emson_client

        self.assettype_lookup: dict = None
        self.relatietype_lookup: dict = None
        self.beheerders_lookup: dict = None

        self.transformer: Transformer = Transformer.from_crs("EPSG:31370", "EPSG:4326", always_xy=True)

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

    def _fill_resource_worker(self, resource):
        """Worker function to run in a separate process."""
        color = colorama_table[resource]
        logging.info(f'{color}Filling {resource.value} table')
        self._fill_resource(resource.value)  # may raise
        return resource

    def fill_tables(self, fill_resources):
        """
        Run all fill tasks in parallel. Retry failed ones indefinitely until all succeed.
        Wait 30 seconds between retry batches if any fail.
        """
        remaining = list(fill_resources)
        attempt = 1

        while remaining:
            logging.info(f"=== Batch attempt {attempt} with {len(remaining)} task(s) ===")
            failed = []

            max_workers = min(len(remaining), 8)
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {executor.submit(self._fill_resource_worker, r): r for r in remaining}

                for future in as_completed(futures):
                    resource = futures[future]
                    color = colorama_table[resource]
                    try:
                        res = future.result()
                        logging.info(f'{color}Finished filling {res.value} table')
                    except Exception as e:
                        logging.error(f'{color}Error filling {resource.value}: {e}')
                        failed.append(resource)

            if failed:
                logging.warning(f"{len(failed)} task(s) failed in attempt {attempt}. Retrying in 30 seconds...")
                time.sleep(30)
                remaining = failed
                attempt += 1
            else:
                logging.info("✅ All tasks completed successfully!")
                break

    def _fill_resource(self, resource: str):
        if resource in {ResourceEnum.assettypes.value, ResourceEnum.relatietypes.value, ResourceEnum.agents.value,
                        ResourceEnum.assets.value, ResourceEnum.betrokkenerelaties.value, ResourceEnum.beheerders.value,
                        ResourceEnum.toezichtgroepen.value, ResourceEnum.identiteiten.value, ResourceEnum.bestekken.value}:
            self._fill_resource_using_em_infra(resource)
        else:
            self._fill_resource_using_emson(resource)

    def _fill_resource_using_emson(self, resource: str):
        # TODO use transaction
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

            for cursor, dicts in self.emson_client.get_resource_by_cursor(
                    resource, cursor=cursor, page_size=self.default_page_size):
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

    @staticmethod
    def to_short_uri(object_uri: str):
        if object_uri == 'http://purl.org/dc/terms/Agent':
            return 'dcmi:Agent'
        shorter_uri = object_uri.split('/ns/')[1]
        if object_uri.startswith('https://wegenenverkeer.'):
            return shorter_uri
        prefix = object_uri.split('://')[1].split('.')[0]
        return f'{prefix}:{shorter_uri}'


    def _fill_resource_using_em_infra(self, resource: str):
        # TODO use transaction
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
        page_size = self.default_page_size

        generator = self.eminfra_client.get_resource_page(resource, page_size, start_from)
        if resource in {ResourceEnum.agents.value, ResourceEnum.betrokkenerelaties.value}:
            generator = self.eminfra_client.get_resource_by_cursor(resource, start_from, page_size,
                                                                            expansion_strings=['contactInfo'])
        elif resource in {ResourceEnum.toezichtgroepen.value, ResourceEnum.identiteiten.value}:
            generator = self.eminfra_client.get_identity_resource_page(resource, page_size, start_from)
        elif resource == ResourceEnum.bestekken.value:
            generator = self.eminfra_client.get_resource_page("bestekrefs", page_size, start_from)

        while params_resource['fill']:
            for cursor, dicts in generator:
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
                    "naam": record["naam"],
                    "label": record["afkorting"],
                    "uri": record["uri"],
                    "short_uri": record['korteUri'],
                    "definitie": record["definitie"],
                    "actief": record["actief"]
                }
                for record in dicts
            ]

            # Bulk insert with overwrite (optional)
            collection.import_bulk(docs_to_insert, overwrite=False, on_duplicate="update")
        elif resource == ResourceEnum.assets.value:
            collection = db.collection('assets')
            collection2 = db.collection('bestekkoppelingen')
            if self.assettype_lookup is None:
                self.assettype_lookup = {
                    at["uri"]: at["_key"]
                    for at in db.collection('assettypes')
                }
            if self.beheerders_lookup is None:
                self.beheerders_lookup = {
                    at["ref"]: at["_key"]
                    for at in db.collection('beheerders')
                }
            docs_to_insert = []
            docs2_to_insert = []
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

                    if assettype_key := self.assettype_lookup.get(uri):
                        obj["assettype_key"] = assettype_key
                    else:
                        print(f"⚠️ No matching assettype for URI: {uri}")
                        continue

                    if tzg_id := obj.get('tz', {}).get("Toezichtgroep_toezichtgroep", {}).get('DtcToezichtGroep_id'):
                        obj["toezichtgroep_key"] = tzg_id[:8]

                    if tz_id := obj.get('tz', {}).get("Toezicht_toezichter", {}).get('DtcToezichter_id'):
                        obj["toezichter_key"] = tz_id[:8]

                    sb_ref = obj.get('tz', {}).get("Schadebeheerder_schadebeheerder", {}).get('DtcBeheerder_referentie')
                    if sb_key := self.assettype_lookup.get(sb_ref):
                        obj["beheerder_key"] = sb_key

                    if 'bs' in obj and 'Bestek_bestekkoppeling' in obj['bs'] and obj['bs']['Bestek_bestekkoppeling']:
                        bestek_koppelingen = obj['bs']['Bestek_bestekkoppeling']
                        for koppeling in bestek_koppelingen:
                            koppeling["_from"] = 'assets/' + obj['_key']
                            koppeling["_to"] = 'bestekken/' + koppeling['DtcBestekkoppeling_bestekId'].get("@DtcIdentificator_identificator")[:8]
                            koppeling['_key'] = str(uuid.uuid4())
                            koppeling['status'] = koppeling.get('status').split('/')[-1] if koppeling.get('status') else None
                            docs2_to_insert.append(koppeling)
                    docs_to_insert.append(obj)
                except Exception as e:
                    logging.error(f"Error processing asset {obj.get('@id', 'unknown')}: {e}")
                    raise e

            # for assets:
            # make ns a separate nested field and remove it from the attributes
            # replace "." with "_"
            collection.import_bulk(docs_to_insert, overwrite=False, on_duplicate="update")
            collection2.import_bulk(docs2_to_insert, overwrite=False, on_duplicate="update")
        elif resource == ResourceEnum.relatietypes.value:
            collection = db.collection('relatietypes')
            docs_to_insert = [
                {
                    "_key": record["uuid"][:4],
                    "uuid": record["uuid"],
                    "naam": record["naam"],
                    "label": record.get("label", None),
                    "uri": record.get("uri", None),
                    "short": None if record.get("uri", None) is None else record["uri"].split('#')[-1],
                    "definitie": record["definitie"],
                    "actief": record.get("actief", True),
                    "gericht": record.get('gericht', None)
                }
                for record in dicts
            ]
            collection.import_bulk(docs_to_insert, overwrite=False, on_duplicate="update")
        elif resource == ResourceEnum.assetrelaties.value:
            collection = db.collection('assetrelaties')
            if self.relatietype_lookup is None:
                self.relatietype_lookup = {
                    at["uri"]: at["_key"]
                    for at in db.collection('relatietypes')
                }
            docs_to_insert = []
            for obj in dicts:
                try:
                    obj = self._transform_keys(obj)
                    uri = obj["@type"]

                    obj["_key"] = obj.get("@id").split("/")[-1][:36]

                    obj["_from"] = 'assets/' + obj['RelatieObject_bron'].get("@id").split("/")[-1][:36]
                    obj["_to"] = 'assets/' + obj['RelatieObject_doel'].get("@id").split("/")[-1][:36]

                    if "AIMDBStatus_isActief" not in obj:
                        obj["AIMDBStatus_isActief"] = True

                    if relatietype_key := self.relatietype_lookup.get(uri):
                        obj["relatietype_key"] = relatietype_key
                        docs_to_insert.append(obj)
                    else:
                        print(f"⚠️ No matching relatietype for URI: {uri}")
                except Exception as e:
                    logging.error(f"Error processing asset {obj.get('@id', 'unknown')}: {e}")
                    raise e

            collection.import_bulk(docs_to_insert, overwrite=False, on_duplicate="update")
        elif resource == ResourceEnum.agents.value:
            collection = db.collection('agents')
            docs_to_insert = []
            for obj in dicts:
                try:
                    obj = self._transform_keys(obj)
                    obj["_key"] = obj.get("@id").split("/")[-1][:13]
                    obj["uuid"] = obj.get("@id").split("/")[-1][:36]
                    docs_to_insert.append(obj)
                except Exception as e:
                    logging.error(f"Error processing agent {obj.get('@id', 'unknown')}: {e}")
                    raise e
            collection.import_bulk(docs_to_insert, overwrite=False, on_duplicate="update")
        elif resource == ResourceEnum.betrokkenerelaties.value:
            collection = db.collection('betrokkenerelaties')
            docs_to_insert = []
            for obj in dicts:
                try:
                    obj = self._transform_keys(obj)

                    obj["_key"] = obj.get("@id").split("/")[-1][:36]

                    if obj['RelatieObject_bron']['@type'] == 'http://purl.org/dc/terms/Agent':
                        obj["_from"] = 'agents/' + obj['RelatieObject_bron'].get("@id").split("/")[-1][:13]
                    else:
                        obj["_from"] = 'assets/' + obj['RelatieObject_bron'].get("@id").split("/")[-1][:36]
                    obj["_to"] = 'assets/' + obj['RelatieObject_doel'].get("@id").split("/")[-1][:13]

                    if "AIMDBStatus_isActief" not in obj:
                        obj["AIMDBStatus_isActief"] = True

                    if 'HeeftBetrokkene_rol' in obj and '/' in obj['HeeftBetrokkene_rol']:
                        obj["rol"] = obj["HeeftBetrokkene_rol"].split('/')[-1]

                except Exception as e:
                    logging.error(f"Error processing asset {obj.get('@id', 'unknown')}: {e}")
                    raise e

            collection.import_bulk(docs_to_insert, overwrite=False, on_duplicate="update")
        elif resource == ResourceEnum.toezichtgroepen.value:
            collection = db.collection('toezichtgroepen')
            docs_to_insert = [
                {
                    "_key": record["uuid"][:8],
                    "uuid": record["uuid"],
                    "naam": record["naam"],
                    'actiefInterval': record['actiefInterval'],
                    'actief': self.actief_interval_to_actief(record['actiefInterval']),
                    'contactFiche': record['contactFiche'],
                    "omschrijving": record.get("omschrijving"),
                    "type": record['_type'],
                }
                for record in dicts
            ]

            # Bulk insert with overwrite (optional)
            collection.import_bulk(docs_to_insert, overwrite=False, on_duplicate="update")
        elif resource == ResourceEnum.identiteiten.value:
            collection = db.collection('identiteiten')
            docs_to_insert = [
                {
                    "_key": record["uuid"][:8],
                    "uuid": record["uuid"],
                    "type": record['_type'],
                    "naam": record["naam"],
                    "voornaam": record["voornaam"],
                    "gebruikersnaam": record["gebruikersnaam"],
                    "systeem": record["systeem"],
                    "voId": record.get("voId"),
                    "bron": record.get("bron"),
                    'actief': record['actief'],
                    'contactFiche': record['contactFiche'],
                    'gebruikersrechtOrganisaties': record.get('gebruikersrechtOrganisaties'),
                }
                for record in dicts
            ]

            # Bulk insert with overwrite (optional)
            collection.import_bulk(docs_to_insert, overwrite=False, on_duplicate="update")
        elif resource == ResourceEnum.beheerders.value:
            collection = db.collection('beheerders')
            docs_to_insert = [
                {
                    "_key": record["uuid"][:8],
                    "uuid": record["uuid"],
                    "type": record['_type'],
                    "naam": record["naam"],
                    "referentie": record["referentie"],
                    'actiefInterval': record['actiefInterval'],
                    'actief': self.actief_interval_to_actief(record['actiefInterval']),
                    'contactFiche': record['contactFiche']
                }
                for record in dicts
            ]

            # Bulk insert with overwrite (optional)
            collection.import_bulk(docs_to_insert, overwrite=False, on_duplicate="update")
        elif resource == ResourceEnum.bestekken.value:
            collection = db.collection('bestekken')
            docs_to_insert = [
                {
                    "_key": record["uuid"][:8],
                    "uuid": record["uuid"],
                    "type": record['type'],
                    "awvId": record.get("awvId"),
                    'eDeltaDossiernummer': record.get('eDeltaDossiernummer'),
                    'eDeltaBesteknummer': record.get('eDeltaBesteknummer'),
                    'aannemerNaam': record.get('aannemerNaam'),
                    'aannemerReferentie': record.get('aannemerReferentie'),
                    'actief': record.get('actief')
                }
                for record in dicts
            ]

            # Bulk insert with overwrite (optional)
            collection.import_bulk(docs_to_insert, overwrite=False, on_duplicate="update")
        else:
            raise NotImplementedError(f"Resource '{resource}' not implemented for insertion.")

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

    def actief_interval_to_actief(self, actief_interval: dict):
        van = actief_interval.get('van')
        tot = actief_interval.get('tot')
        if van is None:
            return False
        van_date = datetime.fromisoformat(van).astimezone(timezone.utc)
        if van_date < datetime.now(timezone.utc):
            if tot is None:
                return True
            tot_date = datetime.fromisoformat(tot).astimezone(timezone.utc)
            if tot_date > datetime.now(timezone.utc):
                return True
        return False