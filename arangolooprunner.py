import os
import time as timer
import logging
from datetime import datetime
from zoneinfo import ZoneInfo
import pytz
import subprocess
from pathlib import Path
from datetime import time
from API.APIEnums import Environment, AuthType
from DBPipelineController import DBPipelineController

BRUSSELS = ZoneInfo("Europe/Brussels")
PARAMS_COLLECTION_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'params')

RUN_WINDOW_START = "03:00:01"
RUN_WINDOW_END = "05:00:00"
SLEEP_TIME = 60

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

def parse_hms_to_seconds(hms: str) -> int:
    parts = (hms or "").split(":")
    if len(parts) != 3:
        raise ValueError(f"Invalid time format '{hms}', expected HH:MM:SS")
    h, m, s = (int(p) for p in parts)
    if not (0 <= h <= 23 and 0 <= m <= 59 and 0 <= s <= 59):
        raise ValueError(f"Invalid time value '{hms}', expected HH:MM:SS within normal ranges")
    return h * 3600 + m * 60 + s

def is_within_run_window(now: datetime) -> bool:
    start_s = parse_hms_to_seconds(RUN_WINDOW_START)
    end_s = parse_hms_to_seconds(RUN_WINDOW_END)
    now_s = now.hour * 3600 + now.minute * 60 + now.second
    if start_s <= end_s:
        return start_s <= now_s <= end_s
    return now_s >= start_s or now_s <= end_s

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
    settings_path = Path('/home/david/Documents/AWV/resources/settings_ArangoDB.json')
    env = Environment.PRD
    auth_type = AuthType.JWT
    while True:
        try:
            now = datetime.now(tz=pytz.timezone("Europe/Brussels"))
            if is_within_run_window(now):
                if last_run_date != now.date():
                    logging.info(f"Within run window ({RUN_WINDOW_START} - {RUN_WINDOW_END}), starting job.")
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
                logging.info(f"Not within run window ({now} is not within {RUN_WINDOW_START} - {RUN_WINDOW_END}), sleeping.")
        except Exception as e:
            logging.error("Exception occurred:", exc_info=True)
        timer.sleep(SLEEP_TIME)

if __name__ == "__main__":
    main()

# The execute_now function is not used in the main loop, but left for manual/interactive use if needed.
def execute_now():
    """
    Script to run the ArangoDB pipeline at a scheduled time window. If run between 03:01 and 05:00, it will first clear the 'params' collection
    using the same credentials/settings as the main pipeline, then run the full pipeline. Uses the settings file and DBPipelineController logic.
    """
    now = datetime.now().time()
    start = time(3, 0, 0)
    end = time(5, 0)
    settings_path = Path('/home/david/Documents/AWV/resources/settings_ArangoDB.json')
    env = Environment.PRD
    auth_type = AuthType.JWT

    if start <= now <= end:
        delete_params_collection(settings_path, env, auth_type)
        controller = DBPipelineController(settings_path=settings_path, auth_type=auth_type, env=env)
        controller.run()
    print('exit')
