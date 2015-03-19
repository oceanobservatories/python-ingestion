import logging, logging.config, mailinglogger
from datetime import datetime
from config import UFRAME, EMAIL, SERVER

LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'simple': {
            'format': '%(levelname)-5s | %(asctime)s | %(name)-8s | %(message)s',
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
            'filename': UFRAME['log_path'] + datetime.today().strftime('/ingestion_%Y_%m_%d.log'),
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

logging.config.dictConfig(LOGGING_CONFIG)
