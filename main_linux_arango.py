import logging
from DBPipelineController import DBPipelineController

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')

def load_settings(file_path: str) -> dict:
    import json
    with open(file_path, 'r') as file:
        return json.load(file)


if __name__ == '__main__':
    settings = load_settings('settings.json')
    db_settings = settings['databases']['prd']

    # 🔐 Connection details
    DB_NAME = db_settings['database']
    USERNAME = db_settings['user']
    PASSWORD = db_settings['password']

    # 🚀 Connect to ArangoDB using DBPipelineController
    controller = DBPipelineController(DB_NAME, USERNAME, PASSWORD)
    controller.run()