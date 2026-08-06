from typing import Optional

from arango import ArangoClient


class ArangoDBConnectionFactory:
    def __init__(self, db_name: str, username: str, password: str, hosts: Optional[list[str]] = None):
        # Increase request timeout to allow long-running server operations (index builds, large AQL)
        # Default was 360s; bump to 1200s to avoid ReadTimeout during heavy operations.
        if hosts:
            self.client = ArangoClient(hosts=hosts, request_timeout=1200)
        else:
            self.client = ArangoClient(request_timeout=1200)
        self.db_name = db_name
        self.username = username
        self.password = password

    def create_connection(self):
        return self.client.db(self.db_name, username=self.username, password=self.password)

    def close(self) -> None:
        """Close the underlying ArangoClient HTTP sessions."""
        if hasattr(self.client, "close"):
            self.client.close()
