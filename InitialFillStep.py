import logging
from datetime import datetime, timezone

from API.EMInfraClient import EMInfraClient
from API.EMSONClient import EMSONClient
from Enums import ResourceEnum
from ResourceEnum import colorama_table


class InitialFillStep:
    def __init__(self, factory, eminfra_client: EMInfraClient, emson_client: EMSONClient):
        self.factory = factory
        self.eminfra_client = eminfra_client
        self.emson_client = emson_client
        self.default_page_size = 100

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

        # for assets:
        # make ns a seperate nested field and remove it from the attributes
        # replace "." with "_"

    @staticmethod
    def _transform_keys(data):
        def process(obj):
            if isinstance(obj, dict):
                result = {}
                for key, value in obj.items():
                    # Recursively process the value
                    value = process(value)

                    # Split namespace if present
                    if ":" in key:
                        ns, field = key.split(":", 1)
                        field = field.replace(".", "_")
                        if ns not in result:
                            result[ns] = {}
                        result[ns][field] = value
                    else:
                        clean_key = key.replace(".", "_")
                        result[clean_key] = value
                return result

            elif isinstance(obj, list):
                return [process(item) for item in obj]

            else:
                return obj

        return process(data)

