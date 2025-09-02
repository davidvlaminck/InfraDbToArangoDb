import logging
from datetime import datetime, timezone
from pathlib import Path

from API.EMInfraClient import EMInfraClient
from API.EMSONClient import EMSONClient
from API.Enums import AuthType, Environment
from ArangoDBConnectionFactory import ArangoDBConnectionFactory
from Enums import DBStep, ResourceEnum


class DBPipelineController:
    """Manages the linear DB pipeline from initial fill to final syncing process."""
    def __init__(self, settings_path: Path, auth_type=AuthType.JWT, env=Environment.PRD):
        self.settings = self.load_settings(settings_path)
        factory, eminfra_client, emson_client = self.settings_to_clients(auth_type=auth_type, env=env)

        self.factory = factory
        self.eminfra_client = eminfra_client
        self.emson_client = emson_client
        self.test_connection()

        self.feed_resources = {ResourceEnum.assets, ResourceEnum.assetrelaties,
                               ResourceEnum.betrokkenerelaties, ResourceEnum.agents}
        self.fill_resources = {ResourceEnum.assettypes}

    def settings_to_clients(self, auth_type, env) -> tuple[ArangoDBConnectionFactory, EMInfraClient, EMSONClient]:
        db_settings = self.settings['databases'][str(env.value[0])]

        eminfra_client = EMInfraClient(env=env, auth_type=auth_type, settings=self.settings)
        emson_client = EMSONClient(env=Environment.PRD, auth_type=auth_type, settings=self.settings)

        db_name = db_settings['database']
        username = db_settings['user']
        password = db_settings['password']
        factory = ArangoDBConnectionFactory(db_name, username, password)

        return factory, eminfra_client, emson_client

    @staticmethod
    def load_settings(settings_path: Path) -> dict[str, object]:
        import json
        with open(settings_path, 'r') as file:
            return json.load(file)

    def run(self):
        # TODO make this a while True loop when everything else is written
        db = self.factory.create_connection()
        for i in range(10):
            current_step = get_db_step(db)
            logging.info(f"Current DB step: {current_step}")
            if current_step is None or current_step == DBStep.CREATE_DB:
                logging.info("Creating the database...")
                self._create_db()
            elif current_step == DBStep.INITIAL_FILL:
                logging.info("Filling the database...")
                self._run_fill()
            self._run_extra_fill()
            self._run_indexes()
            self._run_constraints()
            self._run_syncing()

    def _create_db(self):
        step_runner = CreateDBStep(self.factory)
        step_runner.execute()

    def _run_fill(self):
        step_runner = InitialFillStep(self.factory, self.eminfra_client)
        step_runner.execute()

    def _run_extra_fill(self):
        pass

    def _run_indexes(self):
        pass

    def _run_constraints(self):
        pass

    def _run_syncing(self):
        pass

    def test_connection(self):
        try:
            db = self.factory.create_connection()
            logging.info(f"✅ Successfully connected to database: {db.name}. Interact with it at http://localhost:8529")
        except Exception as e:
            logging.error(f"❌ Failed to connect to database: {e}")
            raise


def set_db_step(db, step: DBStep):
    params = db.collection('params')
    params.insert({"_key": "db_step", "value": step.name}, overwrite=True)
    logging.info(f"🔄 db_step updated to: {step.name}")


def get_db_step(db) -> DBStep | None:
    params = db.collection('params')
    if not params.has("db_step"):
        return None
    doc = params.get("db_step")
    return DBStep[doc['value']] if doc else None


class InitialFillStep:
    def __init__(self, factory, eminfra_client: EMInfraClient):
        self.factory = factory
        self.eminfra_client = eminfra_client

    def execute(self):
        # AQL-query: filter direct op de server
        db = self.factory.create_connection()
        cursor = db.aql.execute("""
            FOR doc IN params
                FILTER doc.page == -1
                RETURN doc
        """)

        # Resultaten ophalen
        docs = list(cursor)
        if any(docs):
            docs_to_update = []
            for feed_name in (d['_key'][5:] for d in docs):
                print(feed_name)

                resource_page = self.eminfra_client.get_last_feedproxy_page(feed_name)
                self_page = next(p for p in resource_page['links'] if p['rel'] == 'self')
                page_number = self_page['href'].split('/')[1]
                last_entry = sorted(
                    iter(resource_page['entries']),
                    key=lambda p: datetime.fromisoformat(p['updated']).astimezone(
                        timezone.utc))[-1]
                print(last_entry['id'])
                docs_to_update.append({'_id': f'params/feed_{feed_name}', 'page': int(page_number),
                                       'event_uuid': last_entry['id']})
            # save the last feedpage to the params collection

            db.collection("params").update_many(docs_to_update)


class CreateDBStep:
    def __init__(self, factory):
        self.factory = factory

    def execute(self):
        db = self.factory.create_connection()

        # 🔍 Check if 'params' exists
        if not db.has_collection("params"):
            logging.info("⚠️ 'params' collection not found. Resetting database...")

            # 🧹 Drop all non-system collections
            for col in db.collections():
                name = col["name"]
                if not name.startswith("_"):
                    db.delete_collection(name, ignore_missing=True)
                    logging.info(f"🗑️ Dropped collection: {name}")

            # 🆕 Create required collections
            for name in ["params", "assets", "assettypes"]:
                db.create_collection(name)
                logging.info(f"✅ Created collection: {name}")

            params = db.collection('params')

            # Define default documents
            # TODO refactor to use the feed set
            default_docs = [
                {"_key": "feed_assetrelaties", "page": -1, "event_uuid": None},
                {"_key": "feed_betrokkenerelaties", "page": -1, "event_uuid": None},
                {"_key": "feed_agents", "page": -1, "event_uuid": None},
                {"_key": "feed_assets", "page": -1, "event_uuid": None},
            ]

            # Insert documents
            for doc in default_docs:
                params.insert(doc)
                logging.info(f"✅ Inserted default for '{doc['_key']}'")

        else:
            logging.info("✅ 'params' collection exists. No changes made.")

        logging.info("✅ Database setup complete. Setting step to INITIAL_FILL.")
        set_db_step(db, DBStep.INITIAL_FILL)