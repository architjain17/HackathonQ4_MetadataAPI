import os
import json
from sqlalchemy import create_engine


def read_config():
    config_file = os.path.join(os.path.dirname(os.path.realpath(__file__)), '../config/database_config.json')
    with open(config_file) as cf:
        config = json.load(cf)

    return config

config = read_config()
engine = create_engine('postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(config['username'], config['password'], config['server'], config['port'], config['database']),
                       connect_args={'client_encoding': 'utf8'})
