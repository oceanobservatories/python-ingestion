from django.conf import settings

from ingestion import config

def parse_date(date_string):
    if date_string:
        try:
            return datetime.strptime(date_string, "%Y-%m-%d")
        except ValueError:
            msg = "Date must be in YYYY-MM-DD format"
            self.logger.error(msg)
            raise ValueError(msg)
    return None

INGESTOR_OPTIONS = {
    'test_mode': getattr(
        settings, INGESTOR_TEST_MODE, False), 
    'force_mode': getattr(
        settings, INGESTOR_FORCE_MODE, False),
    'no_edex': getattr(
        settings, INGESTOR_NO_EDEX, False),
    'sleep_timer': getattr(
        settings, INGESTOR_SLEEP_TIMER, config.SLEEP_TIMER),
    'max_file_age': getattr(
        settings, INGESTOR_MAX_FILE_AGE, config.MAX_FILE_AGE),
    'start_date': parse_date(getattr(
        settings, INGESTOR_START_DATE, config.START_DATE)),
    'end_date': parse_date(getattr(
        settings, INGESTOR_END_DATE, config.END_DATE)),
    'cooldown': getattr(
        settings, INGESTOR_EDEX_COOLDOWN, config.EDEX['cooldown']),
    'quick_look_quantity': getattr(
        settings, INGESTOR_QUICK_LOOK_QUANTITY, config.QUICK_LOOK_QUANTITY),
    'edex_command': getattr(
        settings, INGESTOR_EDEX_COMMAND, config.EDEX['command']),
    'health_check_enabled': getattr(
        settings, INGESTOR_EDEX_HEALTH_CHECK_ENABLED, config.EDEX['health_check_enabled']),
    'qpid_host': getattr(
        settings, INGESTOR_QPID_HOST, config.QPID['host']),
    'qpid_port': getattr(
        settings, INGESTOR_QPID_PORT, config.QPID['port']),
    'qpid_user': getattr(
        settings, INGESTOR_QPID_USER, config.QPID['user']),
    'qpid_password': getattr(
        settings, INGESTOR_QPID_PASSWORD, config.QPID['password']),
    }
