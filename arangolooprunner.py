import os
import time as timer
import logging
import json
from datetime import datetime
import pytz
import subprocess
from pathlib import Path

try:
    import sys
    from lib.pipeline_state import PipelineState
    _PS_AVAILABLE = True
except ImportError:
    _PS_AVAILABLE = False

from API.APIEnums import Environment, AuthType
from DBPipelineController import DBPipelineController
from utils.time_window import BRUSSELS, is_within_time_window, seconds_until_time

PARAMS_COLLECTION_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'params')
SLEEP_TIME = 60
SCHEDULED_RUN_TIME = "03:00:00"
SETTINGS_PATH_CANDIDATES = [
    Path('/home/davidlinux/Documenten/AWV/resources/settings_SyncToArangoDB.json'),
    Path('/home/david/Documents/AWV/resources/settings_ArangoDB.json'),
]

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

def resolve_settings_path() -> Path:
    env_value = os.getenv('SYNC_TO_ARANGO_SETTINGS')
    if env_value:
        return Path(env_value)

    for candidate in SETTINGS_PATH_CANDIDATES:
        if candidate.exists():
            return candidate

    return SETTINGS_PATH_CANDIDATES[0]


def load_settings(settings_path: Path) -> dict:
    with settings_path.open('r', encoding='utf-8') as file:
        return json.load(file)


def get_runner_time_conf(settings: dict | None) -> dict | None:
    return settings.get('time') if isinstance(settings, dict) else None


def get_pipeline_state(settings: dict | None):
    """Create a PipelineState from the settings' health_db config.

    Returns None when the RSA_Health pipeline_state module is unavailable.
    """
    if not _PS_AVAILABLE:
        return None

    health_conf = settings.get("health_db") if isinstance(settings, dict) else None
    if not isinstance(health_conf, dict):
        return None

    db_path = health_conf.get("path")
    project_path = health_conf.get("rsa_health_project_path")
    if not db_path or not project_path:
        return None

    if str(project_path) not in sys.path:
        sys.path.insert(0, str(project_path))

    return PipelineState(db_path)

def delete_params_collection(settings_path, env, auth_type):
    """
    Connects to ArangoDB using the same settings as DBPipelineController and drops the 'params' collection if it exists.
    """
    controller = DBPipelineController(settings_path=settings_path, env=env, auth_type=auth_type)
    db = controller.factory.create_connection()
    if db.has_collection('params'):
        db.delete_collection('params')
        logging.info("Dropped 'params' collection.")
    else:
        logging.warning("'params' collection does not exist.")

def run_main_linux_arango(settings_path, env, auth_type, ps=None):
    try:
        controller = DBPipelineController(settings_path=settings_path, auth_type=auth_type, env=env)
        controller.run()
        logging.info("main_linux_arango.py executed successfully.\n%s")
    except subprocess.CalledProcessError as e:
        logging.error("main_linux_arango.py failed!\n%s", e.stderr)
        if ps is not None:
            ps.update("arango_sync", "failed", f"Script eindigde met fout: {e}")
    except Exception as e:
        logging.error("main_linux_arango.py failed with exception!\n%s", e)
        if ps is not None:
            ps.update("arango_sync", "failed", f"Fout: {e}")

def main():
    settings_path = resolve_settings_path()
    settings = load_settings(settings_path)
    env = Environment.PRD
    auth_type = AuthType.JWT

    ps = get_pipeline_state(settings)
    if ps is not None:
        ps.ensure()

    while True:
        try:
            now = datetime.now(tz=pytz.timezone("Europe/Brussels"))
            delta = seconds_until_time(SCHEDULED_RUN_TIME, now=now, timezone=BRUSSELS)

            if delta > 0:
                logging.info(f"Not yet {SCHEDULED_RUN_TIME}, waiting {int(delta)} seconds.")
                timer.sleep(min(delta, SLEEP_TIME))
                continue

            logging.info(f"{SCHEDULED_RUN_TIME} reached, starting DBPipelineController run.")

            if ps is not None:
                ps.update("arango_sync", "running", "Arango sync gestart")

            delete_params_collection(settings_path, env, auth_type)

            logging.info("First run_main_linux_arango call starting.")
            run_main_linux_arango(settings_path, env, auth_type, ps=ps)
            logging.info("First run_main_linux_arango call finished. Waiting 10 seconds before second call.")
            timer.sleep(10)

            logging.info("Second run_main_linux_arango call starting.")
            run_main_linux_arango(settings_path, env, auth_type, ps=ps)
            logging.info("Second run_main_linux_arango call finished.")

            if ps is not None:
                ps.update("arango_sync", "completed", "Arango sync voltooid")

        except Exception as e:
            logging.error("Exception occurred:", exc_info=True)
            if ps is not None:
                ps.update("arango_sync", "failed", f"Fout in loop: {e}")
        timer.sleep(SLEEP_TIME)

if __name__ == "__main__":
    main()

# The execute_now function is not used in the main loop, but left for manual/interactive use if needed.
def execute_now():
    """
    Manually execute the pipeline only when the configured settings-based time window allows it.
    """
    now = datetime.now(tz=BRUSSELS)
    settings_path = resolve_settings_path()
    settings = load_settings(settings_path)
    time_conf = get_runner_time_conf(settings)
    env = Environment.PRD
    auth_type = AuthType.JWT

    if is_within_time_window(time_conf, now=now, timezone=BRUSSELS):
        ps = get_pipeline_state(settings)
        if ps is not None:
            ps.ensure()
            ps.update("arango_sync", "running", "Arango sync gestart (execute_now)")
        delete_params_collection(settings_path, env, auth_type)
        controller = DBPipelineController(settings_path=settings_path, auth_type=auth_type, env=env)
        controller.run()
        if ps is not None:
            ps.update("arango_sync", "completed", "Arango sync voltooid (execute_now)")
    print('exit')
