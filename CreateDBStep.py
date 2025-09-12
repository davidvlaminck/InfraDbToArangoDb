import logging

from Enums import DBStep
from GenericDbFunctions import set_db_step


class CreateDBStep:
    def __init__(self, factory):
        self.factory = factory

    def execute(self):
        db = self.factory.create_connection()

        # 🔍 Check if 'params' exists
        if not db.has_collection("params"):
            logging.info("⚠️ 'params' collection not found. Resetting database...")

            # 🧹 Drop all graphs in the database
            for graph in db.graphs():
                name = graph["name"]
                db.delete_graph(name, ignore_missing=True, drop_collections=True)
                logging.info(f"🗑️ Dropped graph: {name}")

            # 🧹 Drop all non-system collections
            for col in db.collections():
                name = col["name"]
                if not name.startswith("_"):
                    db.delete_collection(name, ignore_missing=True)
                    logging.info(f"🗑️ Dropped collection: {name}")

            # 🆕 Create document collections
            for name in ["params", "assets", "assettypes", 'relatietypes', "agents", "toezichtgroepen", "identiteiten",
                         "beheerders", "bestekken", 'vplankoppelingen', 'aansluitingrefs']:
                db.create_collection(name)
                logging.info(f"✅ Created document collection: {name}")

            # 🆕 Create edge collections
            for name in ["assetrelaties",  "betrokkenerelaties", "bestekkoppelingen", 'aansluitingen']:
                db.create_collection(name, edge=True)
                logging.info(f"✅ Created edge collection: {name}")

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
