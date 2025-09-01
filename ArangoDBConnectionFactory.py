from arango import ArangoClient


class ArangoDBConnectionFactory:
    def __init__(self, db_name, username, password):
        self.client = ArangoClient()
        self.db_name = db_name
        self.username = username
        self.password = password

    def create_connection(self):
        return self.client.db(self.db_name, username=self.username, password=self.password)
