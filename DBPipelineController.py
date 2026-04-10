import logging
import time
from pathlib import Path
import datetime
from zoneinfo import ZoneInfo

from API.EMInfraClient import EMInfraClient
from API.EMSONClient import EMSONClient
from API.APIEnums import AuthType, Environment
from ArangoDBConnectionFactory import ArangoDBConnectionFactory
from CreateDBStep import CreateDBStep
from CreateIndicesStep import CreateIndicesStep
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
        # Environment.value is a string like 'prd' (historically this used to be a 1-tuple).
        env_key = env.value[0] if isinstance(env.value, tuple) else env.value
        db_settings = self.settings['databases'][str(env_key)]

        eminfra_client = EMInfraClient(env=env, auth_type=auth_type, settings=self.settings)
        emson_client = EMSONClient(env=env, auth_type=auth_type, settings=self.settings)

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
        db = self.factory.create_connection()
        while True:
            try:
                current_step = get_db_step(db)
                logging.info(f"Current DB step: {current_step}")
                if current_step is None or current_step == DBStep.CREATE_DB:
                    logging.info("[0] Creating the database...")
                    self._create_db()
                elif current_step == DBStep.INITIAL_FILL:
                    # honor configured time window: if outside window, sleep until window start
                    time_conf = self.settings.get("time") if isinstance(self.settings, dict) else None
                    if time_conf and not self._is_within_run_window(time_conf):
                        logging.info("Currently outside allowed run window; sleeping until next window start")
                        self._sleep_until_window_start(time_conf)
                    logging.info("[1] Filling the database...")
                    self._run_fill()
                elif current_step == DBStep.EXTRA_DATA_FILL:
                    logging.info("[2] Do some additional filling...")
                    self._run_extra_fill()
                elif current_step == DBStep.CREATE_INDEXES:
                    logging.info("[3] Adding indices and graphs...")
                    self._run_indices()
                elif current_step == DBStep.APPLY_CONSTRAINTS:
                    logging.info("[4] Applying constraints...")
                    self._run_constraints()
                elif current_step == DBStep.SYNC:
                    logging.info("[5] Synchronising...")
                    self._run_syncing()
                elif current_step == DBStep.STOP:
                    logging.info("[6] Stopping...")
                    # Add finished_at document to params collection
                    try:
                        params_col = db.collection('params')
                        # Use Europe/Brussels timezone for the finished timestamp (CET/CEST)
                        tz = ZoneInfo("Europe/Brussels")
                        now_dt = datetime.datetime.now(tz)

                        # ISO format with offset
                        now = now_dt.isoformat()
                        params_col.insert({'_key': 'finished_at', 'value': now}, overwrite=True)
                        try:
                            tzname = now_dt.tzname()
                        except Exception:
                            tzname = None
                        try:
                            offset = now_dt.utcoffset()
                            offset_s = str(offset)
                        except Exception:
                            offset_s = None
                        logging.info(f"Added finished_at to params collection: {now} (tz: {tzname}, offset: {offset_s})")
                    except Exception as e:
                        logging.error(f"Failed to add finished_at to params collection: {e}")
                        continue
                    break
            except Exception as e:
                logging.error(f"Error during DB pipeline execution: {e}", exc_info=True)
                time.sleep(10)

    def _create_db(self):
        step_runner = CreateDBStep(self.factory)
        step_runner.execute()
        set_db_step(self.pipeline_connection, step=DBStep.INITIAL_FILL)

    def _run_fill(self):
        # pass configured time window and optional max attempts into the step runner
        time_conf = self.settings.get("time") if isinstance(self.settings, dict) else None
        # determine max attempts (default 10)
        try:
            max_attempts = int(self.settings.get("max_group_attempts", 10)) if isinstance(self.settings, dict) else 10
        except Exception:
            max_attempts = 10

        step_runner = InitialFillStep(
            self.factory,
            eminfra_client=self.eminfra_client,
            emson_client=self.emson_client,
            run_window=time_conf,
            max_group_attempts=max_attempts,
        )
        # Let exceptions propagate so the outer run() loop can handle/backoff/retry
        step_runner.execute(fill_resource_groups=self.fill_resource_groups)
        set_db_step(self.pipeline_connection, step=DBStep.EXTRA_DATA_FILL)

    def _is_within_run_window(self, time_conf: dict) -> bool:
        """Check whether current Europe/Brussels time is within configured time window.

        time_conf must contain 'start' and 'end' strings like '06:00:00'. If invalid, return True.
        """
        try:
            tz = ZoneInfo("Europe/Brussels")
            now = datetime.datetime.now(tz).time()
            start_s = time_conf.get("start")
            end_s = time_conf.get("end")
            if not start_s or not end_s:
                return True
            fmt = "%H:%M:%S"
            start_t = datetime.datetime.strptime(start_s, fmt).time()
            end_t = datetime.datetime.strptime(end_s, fmt).time()
            if start_t <= end_t:
                return start_t <= now <= end_t
            return now >= start_t or now <= end_t
        except Exception:
            logging.exception("Failed to parse run window from settings; allowing run by default")
            return True

    def _sleep_until_window_start(self, time_conf: dict) -> None:
        try:
            tz = ZoneInfo("Europe/Brussels")
            now_dt = datetime.datetime.now(tz)
            fmt = "%H:%M:%S"
            start_s = time_conf.get("start")
            if not start_s:
                return
            start_time = datetime.datetime.strptime(start_s, fmt).time()
            # build next start datetime
            start_dt = now_dt.replace(hour=start_time.hour, minute=start_time.minute, second=start_time.second, microsecond=0)
            if start_dt <= now_dt:
                # next day's start
                start_dt = start_dt + datetime.timedelta(days=1)
            delta = (start_dt - now_dt).total_seconds()
            logging.info(f"Sleeping for {int(delta)} seconds until next run window start at {start_dt.isoformat()}")
            # cap sleep to avoid extremely long blocking in case of misconfiguration
            max_sleep = 60 * 60 * 6
            to_sleep = min(delta, max_sleep)
            time.sleep(to_sleep)
        except Exception:
            logging.exception("Failed to compute sleep until window start; sleeping 60s")
            time.sleep(60)

    def _run_extra_fill(self):
        step_runner = ExtraFillStep(self.factory, eminfra_client=self.eminfra_client)
        step_runner.execute()
        set_db_step(self.pipeline_connection, step=DBStep.CREATE_INDEXES)

    def _run_indices(self):
        step_runner = CreateIndicesStep(self.factory)
        step_runner.execute()
        set_db_step(self.pipeline_connection, step=DBStep.APPLY_CONSTRAINTS)

    def _run_constraints(self):
        logging.debug("Constraints are currently handled in the CreateIndicesStep.")
        set_db_step(self.pipeline_connection, step=DBStep.SYNC)

    def _run_syncing(self):
        logging.debug("Since the database is filling fast, skip the final syncing step for now.")
        set_db_step(self.pipeline_connection, step=DBStep.STOP)

    def test_connection(self):
        try:
            db = self.factory.create_connection()
            self.pipeline_connection = db
            logging.info(f"✅ Successfully connected to database: {db.name}. Interact with it at http://localhost:8529")
        except Exception as e:
            logging.error(f"❌ Failed to connect to database: {e}")
            raise e
