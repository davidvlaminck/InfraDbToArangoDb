import logging

from Enums import DBStep
from GenericDbFunctions import set_db_step


class CreateDBStep:
    def __init__(self, factory):
        self.factory = factory

    def execute(self):
        db = self.factory.create_connection()

        # Define required collections
        doc_collections = [
            "params", "assets", "assettypes", "relatietypes", "agents", "toezichtgroepen", "identiteiten",
            "beheerders", "bestekken", "vplankoppelingen", "aansluitingrefs"
        ]
        edge_collections = [
            "assetrelaties", "betrokkenerelaties", "bestekkoppelingen", "aansluitingen",
            # Derived edge collections
            "voedt_relaties", "sturing_relaties", "bevestiging_relaties", "hoortbij_relaties"
        ]

        # If params doesn't exist -- full reset and create
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

            # 🆕 Create required document and edge collections
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
            # params exists: ensure required collections are present; create any that are missing
            missing_docs = [n for n in doc_collections if not db.has_collection(n)]
            missing_edges = [n for n in edge_collections if not db.has_collection(n)]
            if not missing_docs and not missing_edges:
                logging.info("✅ 'params' collection exists and required collections present. No changes made.")
            else:
                logging.warning(f"'params' exists but some required collections are missing. Creating {len(missing_docs)} doc(s) and {len(missing_edges)} edge(s).")
                for name in missing_docs:
                    db.create_collection(name)
                    logging.info(f"✅ Created missing document collection: {name}")
                for name in missing_edges:
                    db.create_collection(name, edge=True)
                    logging.info(f"✅ Created missing edge collection: {name}")

                # Ensure default param docs exist
                params = db.collection('params')
                default_docs = [
                    {"_key": "feed_assetrelaties", "page": -1, "event_uuid": None},
                    {"_key": "feed_betrokkenerelaties", "page": -1, "event_uuid": None},
                    {"_key": "feed_agents", "page": -1, "event_uuid": None},
                    {"_key": "feed_assets", "page": -1, "event_uuid": None},
                ]
                for doc in default_docs:
                    if not params.has(doc['_key']):
                        params.insert(doc)
                        logging.info(f"✅ Inserted missing default for '{doc['_key']}'")

        logging.info("✅ Database setup complete. Setting step to INITIAL_FILL.")
        set_db_step(db, DBStep.INITIAL_FILL)
