import os
import logging

LOGGING_LEVEL = os.environ.get('LOGGING_LEVEL', 'DEBUG').upper()
logging.basicConfig(
    level=LOGGING_LEVEL,
    format='[%(asctime)s][%(levelname)8s][%(name)16.16s]@[%(lineno)5s] - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logging.getLogger("kafka").setLevel(level=logging.WARN)

def get_logger(name):
    return logging.getLogger(name)