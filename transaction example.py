from arango import ArangoClient
from arango.database import StandardDatabase
from concurrent.futures import ThreadPoolExecutor, as_completed

class ArangoDBConnectionFactory:
    def __init__(self, db_name: str, username: str, password: str):
        self.client = ArangoClient()  # reuse one client
        self.db_name = db_name
        self.username = username
        self.password = password

    def create_connection(self) -> StandardDatabase:
        # Each call returns a fresh Database handle (thread-safe)
        return self.client.db(self.db_name, username=self.username, password=self.password)

# Global factory instance
FACTORY = ArangoDBConnectionFactory("mydb", "user", "pass")

def transaction_task(thread_id: int):
    db = FACTORY.create_connection()
    txn = None
    try:
        txn = db.begin_transaction(write="params")
        col = txn.collection("params")
        doc = col.get("feed_assets")
        doc["page"] = thread_id  # make it unique per thread
        col.replace(doc)
        txn.commit()
        return f"✅ Thread {thread_id}: committed"
    except Exception as e:
        if txn:
            txn.abort()
        return f"⚠️ Thread {thread_id}: failed — {e}"

if __name__ == "__main__":
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(transaction_task, i) for i in range(10)]
        for future in as_completed(futures):
            print(future.result())