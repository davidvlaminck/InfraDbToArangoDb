from arango import ArangoClient


class ArangoDBConnectionFactory:
    def __init__(self, db_name, username, password, hosts: list[str] = ['http://127.0.0.1:8529']):
        # Increase request timeout to allow long-running server operations (index builds, large AQL)
        # Default was 360s; bump to 1200s to avoid ReadTimeout during heavy operations.
        self.client = ArangoClient(hosts=hosts, request_timeout=1200)
        self.db_name = db_name
        self.username = username
        self.password = password

    def create_connection(self):
        return self.client.db(self.db_name, username=self.username, password=self.password)

    def close(self) -> None:
        """Close the underlying ArangoClient HTTP sessions."""
        if hasattr(self.client, "close"):
            self.client.close()
