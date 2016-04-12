import subprocess
import signal
import sys
import os
import datetime

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler, FileCreatedEvent

tailer_process = None

class EdexLogEventHandler(FileSystemEventHandler):
    def is_todays_log_file(self, src_path):
        filename = os.path.basename(src_path)
        if filename.startswith('edex-ooi-') and filename.endswith('.log'):
            file_parts = filename.split('-')
            if len(file_parts) == 3:
                today = datetime.date.today().strftime('%Y%m%d')
                if file_parts[2].replace('.log', '') == today:
                    return True
        return False
    
    def on_created(self, event):
        global tailer_process
        if type(event) == FileCreatedEvent:
            if self.is_todays_log_file(event.src_path):
                if tailer_process:
                    tailer_process.terminate()
                tailer_process = subprocess.Popen(['python', 'log_tailer.py', event.src_path])

observer = Observer()
observer.schedule(EdexLogEventHandler(), '.', recursive=False)
observer.start()

def exit_handler(sig, frame):
    if tailer_process:
        tailer_process.terminate()
    observer.stop()
    sys.exit(0)

signal.signal(signal.SIGTERM, exit_handler)
signal.signal(signal.SIGINT, exit_handler)

todays_file = 'edex-ooi-%s.log' % datetime.date.today().strftime('%Y%m%d')
if os.path.isfile(todays_file):
    tailer_process = subprocess.Popen(['python', 'log_tailer.py', todays_file])

while True:
    pass
