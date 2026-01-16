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

            # 🆕 Create document and edge collections
            doc_collections = [
                "params", "assets", "assettypes", "relatietypes", "agents", "toezichtgroepen", "identiteiten",
                "beheerders", "bestekken", "vplankoppelingen", "aansluitingrefs"]
            edge_collections = [
                "assetrelaties", "betrokkenerelaties", "bestekkoppelingen", "aansluitingen",
                # Derived edge collections
                "voedt_relaties", "sturing_relaties", "bevestiging_relaties", "hoortbij_relaties"
            ]

            for name in doc_collections:
                db.create_collection(name)
                logging.info(f"✅ Created document collection: {name}")

            for name in edge_collections:
                db.create_collection(name, edge=True)
                logging.info(f"✅ Created edge collection: {name}")

            # Insert default documents in bulk
            default_docs = [
                {"_key": "feed_assetrelaties", "page": -1, "event_uuid": None},
                {"_key": "feed_betrokkenerelaties", "page": -1, "event_uuid": None},
                {"_key": "feed_agents", "page": -1, "event_uuid": None},
                {"_key": "feed_assets", "page": -1, "event_uuid": None},
            ]
            params = db.collection('params')
            params.insert_many(default_docs)
            for doc in default_docs:
                logging.info(f"✅ Inserted default for '{doc['_key']}'")

        else:
            logging.info("✅ 'params' collection exists. No changes made.")

        logging.info("✅ Database setup complete. Setting step to INITIAL_FILL.")
        set_db_step(db, DBStep.INITIAL_FILL)
