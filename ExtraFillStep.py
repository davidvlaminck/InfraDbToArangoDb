from API.EMInfraClient import EMInfraClient


class ExtraFillStep:
    def __init__(self, factory, eminfra_client: EMInfraClient):
        self.factory = factory
        self.eminfra_client: EMInfraClient = eminfra_client

    def execute(self):
        pass
