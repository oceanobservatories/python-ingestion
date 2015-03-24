import logging, logging.config, mailinglogger
from datetime import datetime
from config import UFRAME, EMAIL, SERVER

DEFAULTS = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'simple': {
            'format': '%(levelname)-7s | %(asctime)s | %(name)-8s | %(message)s',
            },
        'email': {
            'format': '%(asctime)s - %(name)s: %(message)s'
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
        'errors_to_console': {
            'class': 'logging.StreamHandler',
            'level': 'ERROR',
            'formatter': 'raw',
            'stream': 'ext://sys.stdout',
            },
        'errors_to_email': {
            'class': 'mailinglogger.SummarisingLogger',
            'level': 'ERROR',
            'mailhost': (EMAIL['server'], EMAIL['port']),
            'fromaddr': EMAIL['sender'],
            'toaddrs': EMAIL['receivers'],
            'subject': '[OOI-RUIG] Auto-Notification: Ingestion Error(s) (%s)' % SERVER,
            'template': ('This is an automatically generated notification. '
                'The following errors occurred while running the ingestion script:\n\n%s'),
            'send_empty_entries': False,
            'formatter': 'email',
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

def setup_logging(log_file_name=None, send_mail=True):
    logging_config = DEFAULTS
    log_file_name = log_file_name or datetime.today().strftime('ingestion_%Y_%m_%d.log')
    if send_mail:
        logging_config['loggers']['']['handlers'] += ['errors_to_email']
    if log_file_name:
        logging_config['handlers']['file_handler']['filename'] = "/".join((
            UFRAME['log_path'], log_file_name))
    logging.config.dictConfig(logging_config)
