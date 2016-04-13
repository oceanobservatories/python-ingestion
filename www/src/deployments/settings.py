from django.conf import settings

from ingestion import config

def parse_date(date_string):
    if date_string:
        try:
            return datetime.strptime(date_string, "%Y-%m-%d")
        except ValueError:
            msg = "Date must be in YYYY-MM-DD format"
            raise ValueError(msg)
    return None

INGESTOR_OPTIONS = {
    'test_mode': False, 
    'force_mode': False,
    'no_edex': False,
    'sleep_timer': config.SLEEP_TIMER,
    'max_file_age': config.MAX_FILE_AGE,
    'start_date': config.START_DATE,
    'end_date': config.END_DATE,
    'cooldown': config.EDEX['cooldown'],
    'quick_look_quantity': config.QUICK_LOOK_QUANTITY,
    'edex_command': config.EDEX['command'],
    'health_check_enabled': config.EDEX['health_check_enabled'],
    'qpid_host': config.QPID['host'],
    'qpid_port': config.QPID['port'],
    'qpid_user': config.QPID['user'],
    'qpid_password': config.QPID['password'],
    }
INGESTOR_OPTIONS.update(settings.INGESTOR_OPTIONS)
INGESTOR_OPTIONS['start_date'] = parse_date(INGESTOR_OPTIONS['start_date'])
INGESTOR_OPTIONS['end_date'] = parse_date(INGESTOR_OPTIONS['end_date'])
