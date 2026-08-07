import os
import time as timer
import logging
import json
import subprocess
from datetime import datetime, timezone, timedelta
import pytz
from pathlib import Path

from utils.sqlite_queue_client import enqueue_sqlite_job

from API.APIEnums import Environment, AuthType
from DBPipelineController import DBPipelineController

PARAMS_COLLECTION_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'params')
SLEEP_TIME = 60
SCHEDULED_RUN_TIME = "03:00:00"

BRUSSELS = pytz.timezone("Europe/Brussels")


def seconds_until_time(target_time_str: str, *, now=None, timezone=BRUSSELS) -> float:
    target = datetime.strptime(target_time_str, "%H:%M:%S").time()
    current_dt = now.astimezone(timezone) if now is not None else datetime.now(timezone)
    current_trunc = current_dt.replace(microsecond=0)

    next_dt = current_trunc.replace(hour=target.hour, minute=target.minute, second=target.second)
    if next_dt < current_trunc:
        next_dt += timedelta(days=1)

    return max(0.0, (next_dt - current_trunc).total_seconds())


# --- Logging setup: both file and console ---
logging.basicConfig(
    filename='arangolooprunner.log',
    filemode='a',
    format='%(asctime)s %(levelname)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    level=logging.INFO
)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
console.setFormatter(formatter)
logging.getLogger().addHandler(console)
# --- End logging setup ---


def load_settings(settings_path: Path) -> dict:
    with settings_path.open('r', encoding='utf-8') as file:
        return json.load(file)


def get_health_db_path(settings: dict | None) -> str | None:
    """Return the health_db SQLite path from settings, or None when unset.

    When set, the runner enqueues pipeline_state write-jobs to the dedicated
    SQLite queue writer instead of writing to SQLite directly.
    """
    health_conf = settings.get("health_db") if isinstance(settings, dict) else None
    if not isinstance(health_conf, dict):
        return None
    return health_conf.get("path")


def update_pipeline_state(
    phase: str, status: str, message: str = "", health_db_path: str | None = None
) -> None:
    """Enqueue a pipeline_state update via the JSON file queue.

    Direct writes to SQLite are no longer allowed from producer processes.
    """
    if health_db_path is None:
        return
    logging.info("Enqueueing pipeline_state: %s / %s", phase, status)
    enqueue_sqlite_job(
        action="update_pipeline_state",
        payload={
            "phase": phase,
            "status": status,
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "message": message,
        },
    )


def delete_params_collection(settings_path, env, auth_type):
    """
    Connects to ArangoDB using the same settings as DBPipelineController and drops the 'params' collection if it exists.
    """
    controller = DBPipelineController(settings_path=settings_path, env=env, auth_type=auth_type)
    try:
        db = controller.factory.create_connection()
        if db.has_collection('params'):
            db.delete_collection('params')
            logging.info("Dropped 'params' collection.")
        else:
            logging.warning("'params' collection does not exist.")
    finally:
        controller.close()

def run_main_linux_arango(settings_path, env, auth_type, health_db_path=None):
    controller = None
    try:
        controller = DBPipelineController(settings_path=settings_path, auth_type=auth_type, env=env)
        controller.run()
        logging.info("main_linux_arango.py executed successfully.")
    except subprocess.CalledProcessError as e:
        logging.error("main_linux_arango.py failed!\n%s", e.stderr)
        update_pipeline_state("arango_sync", "failed", f"Script eindigde met fout: {e}", health_db_path)
    except Exception as e:
        logging.error("main_linux_arango.py failed with exception!\n%s", e)
        update_pipeline_state("arango_sync", "failed", f"Fout: {e}", health_db_path)
    finally:
        if controller is not None:
            controller.close()

def main():
    settings_path = Path(__file__).parent.parent / 'config' / 'settings_arangodb.json'
    settings = load_settings(settings_path)
    env = Environment.PRD
    auth_type = AuthType.JWT

    health_db_path = get_health_db_path(settings)

    while True:
        try:
            now = datetime.now(tz=pytz.timezone("Europe/Brussels"))
            delta = seconds_until_time(SCHEDULED_RUN_TIME, now=now, timezone=BRUSSELS)

            if delta > 0:
                logging.info(f"Not yet {SCHEDULED_RUN_TIME}, waiting {int(delta)} seconds.")
                timer.sleep(min(delta, SLEEP_TIME))
                continue

            logging.info(f"{SCHEDULED_RUN_TIME} reached, starting DBPipelineController run.")

            update_pipeline_state("arango_sync", "running", "Arango sync gestart", health_db_path)

            delete_params_collection(settings_path, env, auth_type)

            logging.info("First run_main_linux_arango call starting.")
            run_main_linux_arango(settings_path, env, auth_type, health_db_path=health_db_path)
            logging.info("First run_main_linux_arango call finished. Waiting 10 seconds before second call.")
            timer.sleep(10)

            logging.info("Second run_main_linux_arango call starting.")
            run_main_linux_arango(settings_path, env, auth_type, health_db_path=health_db_path)
            logging.info("Second run_main_linux_arango call finished.")

            update_pipeline_state("arango_sync", "completed", "Arango sync voltooid", health_db_path)

        except Exception as e:
            logging.error("Exception occurred:", exc_info=True)
            update_pipeline_state("arango_sync", "failed", f"Fout in loop: {e}", health_db_path)
        timer.sleep(SLEEP_TIME)

if __name__ == "__main__":
    main()