import logging
import os

def setup_logging(logger_id, default_path='logging.json',
                  default_level=logging.INFO,
                  env_key='LOG_CFG'):
    """ Setup logging configuration
    """
    path = default_path
    value = os.getenv(env_key, None)
    if value:
        path = value
    if os.path.exists(path):
        with open(path, 'rt') as f:
            config = json.load(f)
        logging.config.dictConfig(config)
    else:
        logger = logging.getLogger(logger_id)
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)-8s %(message)s'))
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)
    return logger
