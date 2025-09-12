import logging
from pathlib import Path

from API.EMInfraClient import EMInfraClient
from API.EMSONClient import EMSONClient
from API.APIEnums import AuthType, Environment
from ArangoDBConnectionFactory import ArangoDBConnectionFactory
from CreateDBStep import CreateDBStep
from Enums import DBStep, ResourceEnum
from ExtraFillStep import ExtraFillStep
from GenericDbFunctions import get_db_step, set_db_step
from InitialFillStep import InitialFillStep


class DBPipelineController:
    """Manages the linear DB pipeline from initial fill to final syncing process."""
    def __init__(self, settings_path: Path, auth_type=AuthType.JWT, env=Environment.PRD):
        self.settings = self.load_settings(settings_path)
        factory, eminfra_client, emson_client = self.settings_to_clients(auth_type=auth_type, env=env)

        self.factory = factory
        self.eminfra_client = eminfra_client
        self.emson_client = emson_client

        self.pipeline_connection = None
        self.test_connection()

        self.feed_resources = {ResourceEnum.assets, ResourceEnum.assetrelaties,
                               ResourceEnum.betrokkenerelaties, ResourceEnum.agents}

        self.fill_resource_groups = [
            [ResourceEnum.assettypes, ResourceEnum.relatietypes, ResourceEnum.toezichtgroepen, ResourceEnum.bestekken,
             ResourceEnum.identiteiten, ResourceEnum.beheerders],
            [ResourceEnum.assetrelaties, ResourceEnum.assets, ResourceEnum.agents, ResourceEnum.betrokkenerelaties,]
        ]

    def settings_to_clients(self, auth_type, env) -> tuple[ArangoDBConnectionFactory, EMInfraClient, EMSONClient]:
        db_settings = self.settings['databases'][str(env.value[0])]

        eminfra_client = EMInfraClient(env=env, auth_type=auth_type, settings=self.settings)
        emson_client = EMSONClient(env=Environment.PRD, auth_type=auth_type, settings=self.settings)

        db_name = db_settings['database']
        username = db_settings['user']
        password = db_settings['password']
        factory = ArangoDBConnectionFactory(db_name, username, password)

        return factory, eminfra_client, emson_client

    @staticmethod
    def load_settings(settings_path: Path) -> dict[str, object]:
        import json
        with open(settings_path, 'r') as file:
            return json.load(file)

    def run(self):
        # TODO make this a while True loop when everything else is written
        db = self.factory.create_connection()
        for _ in range(2):
            current_step = get_db_step(db)
            logging.info(f"Current DB step: {current_step}")
            if current_step is None or current_step == DBStep.CREATE_DB:
                logging.info("Creating the database...")
                self._create_db()
            elif current_step == DBStep.INITIAL_FILL:
                logging.info("Filling the database...")
                self._run_fill()
            elif current_step == DBStep.EXTRA_DATA_FILL:
                logging.info("Do some additional filling...")
                self._run_extra_fill()
            self._run_indexes()
            self._run_constraints()
            self._run_syncing()

    def _create_db(self):
        step_runner = CreateDBStep(self.factory)
        step_runner.execute()
        set_db_step(self.pipeline_connection, step=DBStep.INITIAL_FILL)

    def _run_fill(self):
        step_runner = InitialFillStep(self.factory, eminfra_client=self.eminfra_client, emson_client=self.emson_client)
        step_runner.execute(fill_resource_groups=self.fill_resource_groups)
        set_db_step(self.pipeline_connection, step=DBStep.EXTRA_DATA_FILL)

    def _run_extra_fill(self):
        step_runner = ExtraFillStep(self.factory, eminfra_client=self.eminfra_client)
        step_runner.execute()
        pass

    def _run_indexes(self):
        pass

    def _run_constraints(self):
        pass

    def _run_syncing(self):
        pass

    def test_connection(self):
        try:
            db = self.factory.create_connection()
            self.pipeline_connection = db
            logging.info(f"✅ Successfully connected to database: {db.name}. Interact with it at http://localhost:8529")
        except Exception as e:
            logging.error(f"❌ Failed to connect to database: {e}")
            raise e
