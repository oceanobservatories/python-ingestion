#!/usr/bin/env python
# CSV File Usage: python ingest.py from_csv <ingestion-parameters>.csv

import csv
import datetime
import glob
import logging
import subprocess
import sys
import time

SLEEP_TIMER = 0

# Set up UFrame and EDEX paths
INGESTION = {
    'sender': "/home/developer/uframes/ooi/bin/ingestsender",
    'log_path': "/home/wdk/race/log/",
    }
EDEX = {
    'server': "/home/developer/uframes/ooi/bin/edex-server",
    'log_path': "/home/developer/uframes/ooi/uframe-1.0/edex/logs/",
    }
EDEX_LOG_FILES = glob.glob("%s%s" % (EDEX['log_path'], "edex-ooi*.log"))

# Set up some basic logging.
logging.basicConfig(level=logging.INFO)
handler = logging.FileHandler('logfile.log')
handler.setLevel(logging.INFO)
handler.setFormatter(
    logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    )
logger = logging.getLogger(__name__)
logger.addHandler(handler)

# Start the EDEX server.
try:
    logger.info(
        "Starting the EDEX server.")
    run_edex = subprocess.check_output(['source', EDEX['server']])
except subprocess.CalledProcessError as e:
    logger.error(
        "There was a problem starting the EDEX server (Error code %s)" % e.returncode)
except Exception:
    logger.exception(
        "A system error occurred when starting the EDEX server.")
else:
    logger.info(
        "EDEX server started.")

class Ingest(object):
    ''' A helper class designed to handle the ingestion process.'''

    # A list of methods on the Ingest class that are valid ingestion tasks.
    valid_tasks = ('from_csv', ) 

    @classmethod
    def _ingest_sender(cls, filename_mask, uframe_route, reference_designator, data_source):
        ''' A helper method that finds the files that match the provided filename mask and calls 
            UFrame's ingest sender application with the appropriate command-line arguments. '''

        # Get a list of files that match the file mask and log the list size.
        data_files = glob.glob(filename_mask)
        logger.info("%s file(s) found for %s" % (len(data_files), filename_mask))

        # Ingest each file in the file list.
        for data_file in data_files:
            try:
                # Attempt to send the file to UFrame's ingest sender.
                subprocess.check_output([
                    INGESTION['sender'], uframe_route, data_file, reference_designator, data_source
                    ])
            except subprocess.CalledProcessError as e:
                # If UFrame's ingest sender fails and returns a non-zero exit code, log it.
                logger.error(
                    "There was a problem with UFrame when ingesting %s (Error code %s)." % (
                        data_file, e.returncode))
            except Exception:
                # If there is some other system issue, log it with traceback.
                logger.exception(
                    "There was an unexpected system error when ingesting %s" % data_file)
            else:
                # If the ingest sender returns without any error code, consider a success and log it.
                logger.info(
                    "%s submitted to UFrame for ingestion (%s, %s, %s)." % (
                        data_file, uframe_route, reference_designator, data_source))
        time.sleep(SLEEP_TIMER)

    def from_csv(self, args=None):
        ''' Reads the specified CSV file for mask, route, designator, and source parameters and 
            calls the ingest sender method with the appropriate parameters. '''
        if not args:
            logger.error("No mapping CSV specified.")
            return False
        try:
            reader = csv.DictReader(open(args[0]))
        except IOError:
            logger.error("Mapping CSV not found.")
            return False
        for row in reader:
            self.__class__._ingest_sender(
                row['filename_mask'],
                row['uframe_route'],
                row['reference_designator'],
                row['data_source'],
                )

if __name__ == '__main__':
    perform = Ingest()
    task, args = sys.argv[1], sys.argv[2:]
    if task in Ingest.valid_tasks:
        logger.info(
            "Running ingestion task '%s' with command-line arguments '%s'" % (
                task, " ".join(args)))
        getattr(perform, task)(args)
    else:
        logger.error("%s is not a valid ingestion task." % task)
    