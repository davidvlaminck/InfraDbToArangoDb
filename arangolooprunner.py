import os
import time as timer
import logging
import json
from datetime import datetime
import pytz
import subprocess
from pathlib import Path
from API.APIEnums import Environment, AuthType
from DBPipelineController import DBPipelineController
from utils.time_window import BRUSSELS, get_time_window_label, is_within_time_window

PARAMS_COLLECTION_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'params')
SLEEP_TIME = 60
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

def run_main_linux_arango(settings_path, env, auth_type):
    try:
        controller = DBPipelineController(settings_path=settings_path, auth_type=auth_type, env=env)
        controller.run()
        logging.info("main_linux_arango.py executed successfully.\n%s")
    except subprocess.CalledProcessError as e:
        logging.error("main_linux_arango.py failed!\n%s", e.stderr)

def main():
    last_run_date = None
    settings_path = resolve_settings_path()
    env = Environment.PRD
    auth_type = AuthType.JWT
    while True:
        try:
            now = datetime.now(tz=pytz.timezone("Europe/Brussels"))
            settings = load_settings(settings_path)
            time_conf = get_runner_time_conf(settings)
            window_label = get_time_window_label(time_conf)
            if is_within_time_window(time_conf, now=now, timezone=BRUSSELS):
                if last_run_date != now.date():
                    logging.info(f"Within configured run window ({window_label}), starting job.")
                    delete_params_collection(settings_path, env, auth_type)
                    logging.info("First run_main_linux_arango call starting.")
                    run_main_linux_arango(settings_path, env, auth_type)
                    logging.info("First run_main_linux_arango call finished. Waiting 10 seconds before second call.")
                    timer.sleep(10)
                    logging.info("Second run_main_linux_arango call starting.")
                    run_main_linux_arango(settings_path, env, auth_type)
                    logging.info("Second run_main_linux_arango call finished.")
                    last_run_date = now.date()
                else:
                    logging.info("Already ran today, waiting for next window.")
            else:
                logging.info(f"Not within configured run window ({now} is not within {window_label}), sleeping.")
        except Exception as e:
            logging.error("Exception occurred:", exc_info=True)
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
        delete_params_collection(settings_path, env, auth_type)
        controller = DBPipelineController(settings_path=settings_path, auth_type=auth_type, env=env)
        controller.run()
    print('exit')
