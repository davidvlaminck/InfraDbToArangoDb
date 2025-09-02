import logging
from pathlib import Path

from API.EMInfraClient import EMInfraClient
from API.EMSONClient import EMSONClient
from API.Enums import Environment, AuthType
from DBPipelineController import DBPipelineController

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')


if __name__ == '__main__':
    settings_path = Path('/home/davidlinux/Documenten/AWV/resources/settings_SyncToArangoDB.json')

    # 🚀 Connect to ArangoDB using DBPipelineController
    controller = DBPipelineController(settings_path=settings_path, auth_type=AuthType.JWT, env=Environment.PRD)
    controller.run()