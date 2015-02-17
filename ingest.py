#!/usr/bin/env python
'''
Usage: python ingest.py from_csv <ingestion-parameters>.csv
This script returns error codes at various points-of-failure:
    3 - There is a problem with the CSV file.
    4 - There is a problem with the EDEX server.
    5 - An integer value was not specified for the --sleep option.
'''

import csv
import datetime
import glob
import logging
import subprocess
import sys
import time
from config import SLEEP_TIMER, INGESTION, EDEX

EDEX_LOG_FILES = glob.glob("%s%s" % (EDEX['log_path'], "edex-ooi*.log"))

# Set up some basic logging.
logging.basicConfig(level=logging.INFO)
handler = logging.FileHandler(
    INGESTION['log_path'] + '/' + 'logfile.log'
    )
handler.setLevel(logging.INFO)
handler.setFormatter(
    logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    )
logger = logging.getLogger(__name__)
logger.addHandler(handler)

class Ingest(object):
    ''' A helper class designed to handle the ingestion process.'''

    # A list of methods on the Ingest class that are valid ingestion tasks.
    valid_tasks = ('from_csv', ) 

    @classmethod
    def _common_options(cls, args):
        test_mode = False
        sleep_timer = SLEEP_TIMER
        if "-t" in args:
            test_mode = True
        try:
            sleep_timer = int([a for a in args if a[:8]=="--sleep="][0].split("=")[1])
        except IndexError:
            pass
        except ValueError:
            logger.error("--sleep must be set to an integer")
            sys.exit(5)
        return test_mode, sleep_timer

    @classmethod
    def _start_edex(cls, fake=False):
        ''' Starts the EDEX server. The keyword argument 'fake' '''
        if fake:
            logger.info("Starting the EDEX server.")
            logger.info("EDEX server started.")
            return

        # Start the EDEX server.
        try:
            logger.info(
                "Starting the EDEX server.")
            run_edex = subprocess.check_output(['source', EDEX['command']])
        except subprocess.CalledProcessError as e:
            logger.error(
                "There was a problem starting the EDEX server (Error code %s)" % e.returncode)
            sys.exit(4)
        except Exception:
            logger.exception(
                "A system error occurred when starting the EDEX server.")
            sys.exit(4)
        else:
            logger.info(
                "EDEX server started.")
        return

    @classmethod
    def _ingest_sender(cls, 
                filename_mask, uframe_route, reference_designator, data_source,
                test_mode=False, sleep_timer=SLEEP_TIMER):
        ''' A helper method that finds the files that match the provided filename mask and calls 
            UFrame's ingest sender application with the appropriate command-line arguments. 
            This function will return a list of ingestions that failed. A blank list indicates 
            success.'''

        # Get a list of files that match the file mask and log the list size.
        data_files = glob.glob(filename_mask)
        logger.info("%s file(s) found for %s" % (len(data_files), filename_mask))

        # Keep a running list of failed ingestions.
        failed_ingestions = []
        def track_failure(file, route, designator, source):
            return {
                'filename_mask': file,
                'uframe_route': route,
                'reference_designator': designator,
                'data_source': source,
                }

        # Ingest each file in the file list.
        for data_file in data_files:
            ingestion_command = (
                INGESTION['command'], uframe_route, data_file, reference_designator, data_source
                )
            try:
                # Attempt to send the file to UFrame's ingest sender.
                if test_mode:
                    sys.stdout.write(" ".join(ingestion_command))
                else:
                    subprocess.check_output(ingestion_command)
            except subprocess.CalledProcessError as e:
                # If UFrame's ingest sender fails and returns a non-zero exit code, log it.
                logger.error(
                    "There was a problem with UFrame when ingesting %s (Error code %s)." % (
                        data_file, e.returncode))
                failed_ingestions.append(
                    track_failure(data_file, uframe_route, reference_designator, data_source))
            except Exception:
                # If there is some other system issue, log it with traceback.
                logger.exception(
                    "There was an unexpected system error when ingesting %s" % data_file)
                failed_ingestions.append(
                    track_failure(data_file, uframe_route, reference_designator, data_source))
            else:
                # If the ingest sender returns without any error code, consider a success and log it.
                if test_mode:
                    logger.info(
                        "Test: %s" % " ".join(ingestion_command))
                else:
                    logger.info(
                        "%s submitted to UFrame for ingestion (%s, %s, %s)." % (
                            data_file, uframe_route, reference_designator, data_source))
        time.sleep(sleep_timer)
        return failed_ingestions

    def from_csv(self, args=None):
        ''' Reads the specified CSV file for mask, route, designator, and source parameters and 
            calls the ingest sender method with the appropriate parameters. '''

        # Check for any command line options.
        test_mode, sleep_timer = self.__class__._common_options(args)

        # Check to see if a valid CSV has been specified.
        try:
            csv_file = [f for f in args if f[-4:].lower()==".csv"][0]
        except IndexError:
            logger.error("No mapping CSV specified.")
            sys.exit(3)
        try:
            reader = csv.DictReader(open(csv_file))
        except IOError:
            logger.error("Mapping CSV not found.")
            sys.exit(3)
        fieldnames = ['uframe_route', 'filename_mask', 'reference_designator', 'data_source']
        if reader.fieldnames != fieldnames:
            print reader.fieldnames == fieldnames
            logger.error("Mapping CSV does not have valid column headers.")
            sys.exit(3)

        # Start the EDEX server.
        if not test_mode:
            self.__class__._start_edex()

        # Run ingestions for each row in the CSV, keeping track of any failures.
        failed = []
        for row in reader:
            failed += self.__class__._ingest_sender(
                row['filename_mask'],
                row['uframe_route'], row['reference_designator'], row['data_source'],
                test_mode, sleep_timer,
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
    