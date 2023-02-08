import os
import sys
import logging.handlers
from conf.path_config import log_dir, log_file
TRACEBACK_LOGGER_ERRORS = True


__all__ = ['logger']

logger = logging.getLogger('DT')

# 控制台日志输出
_stream_handler = logging.StreamHandler(sys.stdout)
_stream_handler.setLevel(logging.DEBUG)
_stream_handler.setFormatter(logging.Formatter('%(message)s'))
logger.addHandler(_stream_handler)

# 文件日志输出
try:
    _file_handler = logging.handlers.TimedRotatingFileHandler(log_file, when='D', interval=1, backupCount=5)
except FileNotFoundError:
    log_file = open(os.path.join(log_dir, 'load_forcasting.log'), mode='w')
    _file_handler = logging.handlers.TimedRotatingFileHandler(log_file, when='D', interval=1, backupCount=5)

_file_handler.setLevel(logging.INFO)
# _file_handler.setLevel(logging.DEBUG)
_file_handler.setFormatter(logging.Formatter('%(asctime)s : %(levelname)s : %(filename)s: %(lineno)d行: %(message)s'))
logger.addHandler(_file_handler)

logger.setLevel(logging.INFO)



