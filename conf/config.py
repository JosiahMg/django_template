"""
开发dev/测试test/生产prod环境
"""
import os

import yaml

from conf.path_config import project_dir

ENV = 'dev'
# ENV = 'test'
# ENV = 'prod'

config_file = os.path.join(project_dir, f'conf/config_{ENV}.yaml')

with open(config_file, encoding='utf-8') as f:
    config = yaml.load(f, Loader=yaml.FullLoader)

PostgresqlConfig = config['postgresql']
TDengineConfig = config['tdengine']
