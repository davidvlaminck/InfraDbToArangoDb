from arango import ArangoClient


class ArangoDBConnectionFactory:
    def __init__(self, db_name, username, password):
        # Increase request timeout to allow long-running server operations (index builds, large AQL)
        # Default was 360s; bump to 1200s to avoid ReadTimeout during heavy operations.
        self.client = ArangoClient(request_timeout=1200)
        self.db_name = db_name
        self.username = username
        self.password = password

    def create_connection(self):
        return self.client.db(self.db_name, username=self.username, password=self.password)
