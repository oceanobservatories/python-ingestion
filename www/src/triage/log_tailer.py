import os
import sys
import django
import signal
import tailer

sys.path.append(os.getcwd().replace('/triage', ''))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'diman.settings.base')
django.setup()

from triage import tasks

def exit_handler(sig, frame):
    sys.exit(0)

signal.signal(signal.SIGTERM, exit_handler)
signal.signal(signal.SIGINT, exit_handler)

if __name__ == '__main__':
    try:
        print 'Tailing process started for %s' % sys.argv[1]
        for line in tailer.follow(open(sys.argv[1]), 0.1):
            if '[Ingest.' in line and ('FileDecoder:' in line or 'ParticleFactory' in line):
                tasks.save_log.delay(line)
    except:
        pass
