from arango import ArangoClient

# 🔐 Connection details
DB_NAME = "infra_db"
USERNAME = "sync_user"
PASSWORD = ""

# 🚀 Connect to ArangoDB
client = ArangoClient()
db = client.db(DB_NAME, username=USERNAME, password=PASSWORD)

# for db_step instead define an enum
from enum import Enum


class DBStep(Enum):
    ONE_FILL = "1_fill"
    TWO_EXTRA_FILL = "2_extra_fill"
    THREE_INDEXES = "3_indexes"
    FOUR_CONSTRAINTS = "4_constraints"
    FIVE_SYNCING = "5_syncing"


def set_db_step(step: DBStep):
    params = db.collection('params')
    params.insert({"_key": "db_step", "value": step.name}, overwrite=True)
    print(f"🔄 db_step updated to: {step.name}")

# 🔍 Check if 'params' exists
if not db.has_collection("params"):
    print("⚠️ 'params' collection not found. Resetting database...")

    # 🧹 Drop all non-system collections
    for col in db.collections():
        name = col["name"]
        if not name.startswith("_"):
            db.delete_collection(name, ignore_missing=True)
            print(f"🗑️ Dropped collection: {name}")

    # 🆕 Create required collections
    for name in ["params", "assets", "assettypes"]:
        db.create_collection(name)
        print(f"✅ Created collection: {name}")

    params = db.collection('params')

    # Define default documents
    default_docs = [
        {"_key": "feed_assetrelaties", "page": -1, "event_uuid": None},
        {"_key": "feed_betrokkenerelaties", "page": -1, "event_uuid": None},
        {"_key": "feed_agents", "page": -1, "event_uuid": None},
        {"_key": "feed_assets", "page": -1, "event_uuid": None},
    ]
    set_db_step(DBStep.ONE_FILL)

    # Insert documents
    for doc in default_docs:
        params.insert(doc)
        print(f"✅ Inserted default for '{doc['_key']}'")

else:
    print("✅ 'params' collection exists. No changes made.")