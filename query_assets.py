import json
import logging
from pathlib import Path

from API.APIEnums import Environment, AuthType
from API.EMInfraClient import EMInfraClient
from API.RequesterFactory import RequesterFactory

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')


if __name__ == '__main__':
    settings_path = Path(__file__).parent.parent / 'config' / 'settings_arangodb.json'
    with open(settings_path, 'r') as f:
        settings = json.load(f)

    env = Environment.PRD
    auth_type = AuthType.COOKIE

    requester = RequesterFactory.create_requester(auth_type=auth_type, env=env, settings=settings, cookie='4567ff1fce0c4017a3a933d28bc33b47')
    requester.first_part_url += 'eminfra/'
    client = EMInfraClient(auth_type=auth_type, env=env, settings=settings)
    client.requester = requester

    page_size = 100
    start_from = 0
    assets = []
    for start, data in client.get_resource_page('assets', page_size=page_size, start_from=start_from):
        assets.extend(data)
        logging.info(f"Fetched {len(data)} assets (total so far: {len(assets)})")
        if start is None:
            break
        if len(assets) >= 1000:
            break

    logging.info(f"Done. Retrieved {len(assets)} assets.")
    print(json.dumps(assets, indent=2))

    requester.close()