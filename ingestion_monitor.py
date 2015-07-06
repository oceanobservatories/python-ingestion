import sys, os
import time
import logging
import csv
import signal

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler, PatternMatchingEventHandler

from ingest import Ingestor, Task
import logger
import config

# ---------------------------------------
# Constants

CSV_FILES = []
for root, dirs, files in os.walk(config.MONITOR["ingestion_csv_path"]):
    CSV_FILES += ["/".join([root, f]) for f in files if f.endswith(".csv")]

GLOBAL_INGESTOR = Ingestor(test_mode=config.MONITOR["test_mode"])

# ---------------------------------------
# Classes

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
        ''' When a file matching the file mask is created and observed, get the deployment number 
            and ingest the file. '''
        ingest_file = event.src_path
        self.logger.info("%s has been found" % ingest_file)

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
        self.schedules = 0
        for mask in self.routes:
            # Attach a schedule for each file mask in the CSV file to the Observer.
            mask_path = '/'.join(mask.split('/')[:-1])
            if os.path.isdir(mask_path):
                event_handler = MaskRouteEventHandler(patterns=[mask], routes=self.routes[mask])
                self.observer.schedule(event_handler, mask_path, recursive=True)
                self.schedules += 1 
            else:
                self.logger.warning("Directory not found: %s" % mask_path)

        if self.schedules == 0:
             self.logger.warning("No watchers set for this observer: %s" % self.csv_file)

    def process_csv(self):
        try:
            reader = csv.DictReader(open(self.csv_file))
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
        self.logger.info("Starting monitor for %s" % self.csv_file)
        self.observer.start()

    def stop(self):
        self.observer.stop()
        self.observer.join()

# ---------------------------------------
# Main

# Setup Logging
logger.setup_logging(
    log_file_name="ingestion_monitor.log",
    send_mail=config.EMAIL['enabled'],
    info_to_console=False)
main_logger = logging.getLogger("Main")

# Create IngestionMonitors for all csv files.
monitors = {}
for f in CSV_FILES:
    monitors[f] = IngestionMonitor(f)

# Remove any IngestionMonitors that have no schedules set.
monitors = {k: v for k, v in monitors.iteritems() if monitors[k].schedules > 0}

# Start all IngestionMonitors.
for f in monitors:
    monitors[f].start()

main_logger.info("All monitors ready.")

def exit_handler(sig, frame):
    # Stop all IngestionMonitors.
    main_logger.info("Got stop signal, stopping all observers and exiting script.")
    for f in monitors:
        monitors[f].stop()
    main_logger.info("All observers stopped.")
    sys.exit(0)

signal.signal(signal.SIGTERM, exit_handler)
signal.signal(signal.SIGINT, exit_handler)

# Wait for SIGTERM or SIGINT to stop the script.
while True:
    pass
