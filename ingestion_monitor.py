import sys, os
import time
import logging
import csv
import signal
from threading import Timer

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler, PatternMatchingEventHandler

from ingest import Ingestor, Task
import logger
import config

# ---------------------------------------
# Setup Logging

logger.setup_logging(
    log_file_name="ingestion_monitor.log",
    send_mail=config.EMAIL['enabled'],
    info_to_console=False)
main_logger = logging.getLogger("Main")

# ---------------------------------------
# Constants

CSV_FILES = []
for root, dirs, files in os.walk(config.MONITOR.get("ingestion_csv_path", "."), followlinks=True):
    CSV_FILES += ["/".join([root, f]) for f in files if f.endswith(".csv") and "#" not in f]

GLOBAL_INGESTOR = Ingestor(
    test_mode=config.MONITOR.get("test_mode", False), 
    force_mode=config.MONITOR.get("force_mode", True),
    )

QUEUE_INGESTION_ENABLED = config.MONITOR.get('queue_ingestion_enabled', False)
QUEUE_INGESTION_INTERVAL = config.MONITOR.get("queue_ingestion_interval", 30)

# ---------------------------------------
# Classes

class RepeatedTimer(object):
    ''' From: http://stackoverflow.com/questions/3393612/run-certain-code-every-n-seconds
        A threaded timer that repeatedly runs a function at a specified interval. 
        Used for periodic queue ingestion. '''
    def __init__(self, interval, auto_start, function, *args, **kwargs):
        self.logger = logging.getLogger('Timer')
        self._timer = None
        self.interval = interval
        self.function = function
        self.args = args
        self.kwargs = kwargs
        self.is_running = False
        if auto_start:
            self.start()

    def _run(self):
        self.is_running = False
        self.start()
        self.function(*self.args, **self.kwargs)

    def start(self):
        if not self.is_running:
            self._timer = Timer(self.interval, self._run)
            self._timer.start()
            self.is_running = True

    def stop(self):
        if self.is_running:
            self._timer.cancel()
            self.is_running = False
            self.logger.info("Queue ingestion timer stopped.")

class MaskRouteEventHandler(PatternMatchingEventHandler):
    ''' The event handler for ingestions, subclassed from PatternMatchingEventHandler.
        The file mask from the CSV is passed as the pattern, and the Handler class is extended to 
        also receive the "routes" for the file mask (i.e. the uframe_route, reference_designator, 
        and data_source). '''
    def __init__(self, *args, **kwargs):
        # Track the "routes" and set up logging.
        self.routes = kwargs.pop('routes', None)
        self.logger = logging.getLogger('Handler')
        super(MaskRouteEventHandler, self).__init__(*args, **kwargs)

    def on_created(self, event):
        ''' When a file matching the file mask is created and observed, add it to the ingestion 
            queue. If the ingestion queue is not enabled, get the deployment number and ingest the 
            file. '''
        ingest_file = event.src_path
        self.logger.info("%s has been found" % ingest_file)

        if QUEUE_INGESTION_ENABLED:
            # Add file to queue for periodic ingestion.
            GLOBAL_INGESTOR.load_queue(ingest_file, self.routes)
        else:
            # Immediately ingest the file.
            try:
                deployment_number = str(int([
                    n for n in ingest_file.split("/") 
                    if len(n)==6 and n[0] in ('D', 'R', 'X')
                    ][0][1:]))
            except:
                self.logger.error(
                    "Can't get deployment number from %s." % ingest_file)
                return False
            GLOBAL_INGESTOR.send([(ingest_file, self.routes)], deployment_number)

class IngestionMonitor:
    def __init__(self, csv_file):
        self.logger = logging.getLogger("Monitor")

        # Process CSV files and get the specific routes and file masks.
        self.csv_file = csv_file
        self.routes = self.process_csv()

        self.logger.info("Creating observer for %s" % csv_file)
        self.observer = Observer()
        for mask in self.routes:
            # Attach a watcher for each file mask in the CSV file to the Observer.
            mask_path = '/'.join(mask.split('/')[:-1])
            if os.path.isdir(mask_path):
                event_handler = MaskRouteEventHandler(patterns=[mask], routes=self.routes[mask])
                self.observer.schedule(event_handler, mask_path, recursive=True)
            else:
                self.logger.warning("Directory not found: %s" % mask_path)

        if self.watchers == 0:
             self.logger.warning("No watchers set for this observer: %s" % self.csv_file)

    @property
    def watchers(self):
        return len(self.observer._watches)

    def process_csv(self):
        try:
            reader = csv.DictReader(open(self.csv_file, "U"))
        except IOError:
            self.logger.error("%s not found." % self.csv_file)
            return False
        fieldnames = ['uframe_route', 'filename_mask', 'reference_designator', 'data_source']
        if not set(fieldnames).issubset(reader.fieldnames):
            self.logger.error((
                "%s does not have valid column headers. "
                "The following columns are required: %s") % (self.csv_file, ", ".join(fieldnames)))
            return False

        def commented(row):
            ''' Check to see if the row is commented out. Any field that starts with # indictes a 
                comment.'''
            return bool([v for v in row.itervalues() if v and v.startswith("#")])

        routes = {}

        # Load the queue with parameters from each row.
        for row in reader:
            if not commented(row):
                mask = row['filename_mask']
                parameters = {
                    f: row[f] for f in row 
                    if f in ('uframe_route', 'reference_designator', 'data_source')
                    }
                if mask in routes.keys():
                    routes[mask].append(parameters)
                else:
                    routes[mask] = [parameters]
       
        return {mask: routes[mask] for mask in routes}

    def start(self):
        self.logger.info("Starting %s watchers for %s" % (self.watchers, self.csv_file))
        self.observer.start()

    def stop(self):
        self.observer.stop()
        self.observer.join()

# ---------------------------------------
# Main

# Create IngestionMonitors for all csv files.
MONITORS = {}
TOTAL_WATCHERS = 0
TOTAL_RUNNING_WATCHERS = 0
for f in CSV_FILES:
    MONITORS[f] = IngestionMonitor(f)
    TOTAL_WATCHERS += MONITORS[f].watchers

main_logger.info("Total watchers for all monitors: %s" % TOTAL_WATCHERS)

# Remove any IngestionMonitors that have no watchers set.
MONITORS = {k: v for k, v in MONITORS.iteritems() if MONITORS[k].watchers > 0}

# Start all IngestionMonitors.
for m in MONITORS:
    try:
        MONITORS[m].start()
        TOTAL_RUNNING_WATCHERS += MONITORS[m].watchers
    except OSError:
        main_logger.exception(
            "inotify instance limit reached (created %s watcher(s)), increase OS's max_user_watches." % TOTAL_RUNNING_WATCHERS)
        sys.exit(1)

main_logger.info("All monitors ready. Running %s total watchers." % TOTAL_RUNNING_WATCHERS)

# Set up and start ingestor queue emptying thread.
def ingest_from_queue(ingestor):
    ''' Wrapper function for the ingestor's ingest_from_queue method. 
        Defined outside of any class for use in the Ingestion Queue Timer. '''
    if ingestor.queue:
        ingestor.ingest_from_queue()

QUEUE_INGESTION_TIMER = RepeatedTimer(
    QUEUE_INGESTION_INTERVAL, QUEUE_INGESTION_ENABLED, ingest_from_queue, GLOBAL_INGESTOR)
if QUEUE_INGESTION_ENABLED:
    QUEUE_INGESTION_TIMER.logger.info(
        "Queue ingestion timer started and will run every %s second(s)." % QUEUE_INGESTION_INTERVAL)

def exit_handler(sig, frame):
    # Stop all IngestionMonitors.
    main_logger.info("Got stop signal, stopping all observers and exiting script.")
    for m in MONITORS:
        MONITORS[m].stop()
    main_logger.info("All monitors stopped.")
    QUEUE_INGESTION_TIMER.stop()
    sys.exit(0)

signal.signal(signal.SIGTERM, exit_handler)
signal.signal(signal.SIGINT, exit_handler)

# Wait for SIGTERM or SIGINT to stop the script.
while True:
    pass
