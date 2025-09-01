import logging

from ArangoDBConnectionFactory import ArangoDBConnectionFactory
from Enums import DBStep


class DBPipelineController:
    """Manages the linear DB pipeline from initial fill to final syncing process."""
    def __init__(self, db_name, username, password):
        self.factory = ArangoDBConnectionFactory(db_name, username, password)
        self.test_connection()

    def run(self):
        db = self.factory.create_connection()
        current_step = get_db_step(db)
        logging.info("Current DB step: {}".format(current_step))
        if current_step is None:
            logging.info("Starting initial fill...")
            self._run_fill()
        elif current_step == DBStep.INITIAL_FILL:
            logging.info("Continuing with initial fill...")
            self._run_fill()
        self._run_extra_fill()
        self._run_indexes()
        self._run_constraints()
        self._run_syncing()

    def _run_fill(self):
        step_runner = InitialFillStep(self.factory)
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
            logging.info(f"❌ Failed to connect to database: {e}")
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
    if doc:
        return DBStep[doc['value']]
    return None


class InitialFillStep:
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
            default_docs = [
                {"_key": "feed_assetrelaties", "page": -1, "event_uuid": None},
                {"_key": "feed_betrokkenerelaties", "page": -1, "event_uuid": None},
                {"_key": "feed_agents", "page": -1, "event_uuid": None},
                {"_key": "feed_assets", "page": -1, "event_uuid": None},
            ]
            set_db_step(db, DBStep.INITIAL_FILL)

            # Insert documents
            for doc in default_docs:
                params.insert(doc)
                logging.info(f"✅ Inserted default for '{doc['_key']}'")

        else:
            logging.info("✅ 'params' collection exists. No changes made.")
