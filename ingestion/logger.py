import logging, logging.config
from datetime import datetime
from config import LOGGING

DEFAULTS = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'simple': {
            'format': '%(levelname)-7s | %(asctime)s | %(name)-8s | %(message)s',
            },
        'raw': {
            'format': '%(message)s',
            },
        },
    'handlers': {
        'file_handler': {
            'class': 'logging.FileHandler',
            'level': 'INFO',
            'formatter': 'simple',
            },
        'info_to_console': {
            'class': 'logging.StreamHandler',
            'level': 'INFO',
            'formatter': 'raw',
            'stream': 'ext://sys.stdout',
            },
        'errors_to_console': {
            'class': 'logging.StreamHandler',
            'level': 'ERROR',
            'formatter': 'raw',
            'stream': 'ext://sys.stdout',
            },
        },
    'loggers': {
        '': {
            'level': 'INFO',
            'handlers': ['file_handler', 'errors_to_console', ],
            'propagate': False,
            },
        },
    }

def setup_logging(log_file=None, verbose=False):
    logging_config = DEFAULTS
    log_file = log_file or datetime.today().strftime('ingestion_%Y_%m_%d.log')
    if verbose:
        logging_config['loggers']['']['handlers'].append('info_to_console')
    if log_file:
        logging_config['handlers']['file_handler']['filename'] = "/".join((
            LOGGING['ingestion'], log_file))
    logging.config.dictConfig(logging_config)
