#!/usr/bin/env python
INTERNAL_DOCUMENTATION = '''
Data Ingestion Script
Usage: python ingest.py [task] [options]

Tasks:
      from_csv  Ingest data from a CSV file. 
                Requires a filename argument with a .csv extension.
from_csv_batch  Ingest data from multiple CSV files defined in a batch file.  
                Requires a filename argument with a .csv.batch extension.
         dummy  A dummy task that creates an Ingestor but doesn't try to ingest any data. 
                Used for testing.

Options:
            -h  Display this help message.
            -t  Test Mode. 
                    The script will go through all of the motions of ingesting data, but will not 
                    call any ingest sender commands.
            -c  Commands-only Mode. 
                    The script will output the ingest sender commands for all files in the queue, 
                    but will not go through the ingestion process.
            -f  Force Mode. 
                    The script will disregard the EDEX log file checks for already ingested data 
                    and ingest all matching files.
     --sleep=n  Override the sleep timer with a value of n seconds.
 --startdate=d  Only ingest files newer than the specified start date d (in the YYYY-MM-DD format).
   --enddate=d  Only ingest files older than the specified end date d (in the YYYY-MM-DD format).
       --age=n  Override the maximum age of the files to be ingested in n seconds.
  --cooldown=n  Override the EDEX service startup cooldown timer with a value of n seconds.
     --quick=n  Override the number of files per filemask to ingest. Used for quick look 
                ingestions.

Error Codes:
             4  There is a problem with the EDEX server.
             5  An integer value was not specified for any of the override options.

'''

import csv
import datetime
import logging
import os
import subprocess
import sys

from time import sleep
from config import (
    SLEEP_TIMER, MAX_FILE_AGE, START_DATE, END_DATE, QUICK_LOOK_QUANTITY, 
    UFRAME, EDEX)
from whelk import shell, pipe
from glob import glob

import email_notifications

# Set up some basic logging.
logging.basicConfig(level=logging.INFO)
file_handler = logging.FileHandler(
    UFRAME['log_path'] + '/ingestion_' + datetime.datetime.today().strftime('%Y_%m_%d') + '.log'
    )
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger = logging.getLogger(__name__)
logger.addHandler(file_handler)
logger.propagate = False

def set_options(object, attrs, options):
    defaults = {
        'test_mode': False,
        'force_mode': False,
        'sleep_timer': SLEEP_TIMER,
        'start_date': None,
        'end_date': None,
        'max_file_age': MAX_FILE_AGE,
        'cooldown': EDEX['cooldown'],
        'quick_look_quantity': None,
        'edex_command': EDEX['command'],
        }
    for attr in attrs:
        setattr(object, attr, options.get(attr, defaults[attr]))

class Task(object):
    ''' A helper class designed to manage the different types of ingestion tasks.'''

    # A list of methods on the Task class that are valid ingestion tasks.
    valid_tasks = (
        'from_csv',         # Ingest from a single CSV file.
        'from_csv_batch',   # Ingest from multiple CSV files defined in a batch.
        'dummy',            # A dummy task. Doesn't do anything.
        ) 

    def __init__(self, args):
        ''' Parse and interpret common command-line options.'''

        def number_switch(args, switch):
            switch = "--%s=" % switch
            try:
                return int([a for a in args if a[:len(switch)]==switch][0].split("=")[1])
            except IndexError:
                return None
            except ValueError:
                logger.error("%s must be set to an integer" % switch)
                sys.exit(5)

        def get_date(date_string):
            if date_string:
                return datetime.datetime.strptime(date_string, "%Y-%m-%d")
            return None

        def date_switch(args, switch):
            switch = "--%s=" % switch
            try:
                return get_date([a for a in args if a[:len(switch)]==switch][0].split("=")[1])
            except IndexError:
                return None
            except ValueError:
                logger.error("%s must be in YYYY-MM-DD format" % switch)
                sys.exit(5)

        self.options = {
            'test_mode': "-t" in args, 
            'force_mode': "-f" in args,
            'commands_only': '-c' in args, 
            'sleep_timer': number_switch(args, "sleep") or SLEEP_TIMER,
            'max_file_age': number_switch(args, "age") or MAX_FILE_AGE,
            'start_date': date_switch(args, "startdate") or get_date(START_DATE),
            'end_date': date_switch(args, "enddate") or get_date(END_DATE),
            'cooldown': number_switch(args, "cooldown") or EDEX['cooldown'],
            'quick_look_quantity': number_switch(args, "quick") or QUICK_LOOK_QUANTITY,
            'edex_command': EDEX['command'],
            }
        self.args = args

    def dummy(self):
        ''' A dummy task that doesn't do anything except create an Ingestor. '''
        ingest = Ingestor(**self.options)
        email_notifications.send('Dummy Task', self.verbose_options())
        logger.info("Dummy task was run with options.")
        logger.info(self.options)

    def from_csv(self):
        ''' Ingest data mapped out by a single CSV file. '''

        # Check to see if a valid CSV has been specified.
        try:
            csv_file = [f for f in self.args if f[-4:].lower()==".csv"][0]
        except IndexError:
            logger.error("No mapping CSV specified.")
            return False

        # Create an instance of the Ingestor class with common options set.
        ingestor = Ingestor(**self.options)

        # Ingest from the CSV file.
        ingestor.load_queue_from_csv(csv_file)
        ingestor.write_queue_to_file()
        if "-c" not in self.args:
            ingestor.ingest_from_queue()

        # Write out any failed ingestions to a new CSV file.
        if ingestor.failed_ingestions:
            ingestor.write_failures_to_csv(
                csv_file.split("/")[-1].split(".")[0])

        logger.info("Ingestion completed.")
        email_notifications.ingestion_completed(csv_file, self.options)
        return True

    def from_csv_batch(self):
        ''' Ingest data mapped out by multiple CSV files, defined in a single .csv.batch file.'''

        # Create an instance of the Ingestor class with common options set.
        ingestor = Ingestor(**self.options)

        # Open the batch list file and parse the csv paths into a list
        try:
            csv_batch = [f for f in self.args if f[-10:].lower()==".csv.batch"][0]
        except IndexError:
            logger.error("No CSV batch file specified.")
            return False
        with open(csv_batch, 'r') as f:
            csv_files = [x.strip() for x in f.readlines() if x.strip()]

        # Ingest from each CSV file.
        for csv_file in csv_files:
            ingestor.load_queue_from_csv(csv_file)
        ingestor.write_queue_to_file()
        if "-c" not in self.args:
            ingestor.ingest_from_queue()

        # Write out any failed ingestions from the entire batch to a new CSV file.
        if ingest.failed_ingestions:
            ingest.write_failures_to_csv(
                csv_batch.split("/")[-1].split(".")[0] + "_batch")

        logger.info("Ingestion completed.")
        email_notifications.ingestion_completed(csv_batch, self.options)
        return True

class ServiceManager(object):
    ''' A helper class that manages the services that the ingestion depends on.'''

    def __init__(self, **options):
        set_options(self, ('test_mode', 'edex_command', 'cooldown'), options)

        self.edex_log_files = self.process_all_logs()

        # Source the EDEX server environment.
        if self.test_mode or EDEX['test_mode']:
            logger.info("TEST MODE: Sourcing the EDEX server environment.")
            logger.info("TEST MODE: EDEX server environment sourced.")
            return

        # Source the EDEX environment.
        try:
            logger.info("Sourcing the EDEX server environment.")
            # Adapted from http://pythonwise.blogspot.fr/2010/04/sourcing-shell-script.html
            proc = subprocess.Popen(
                ". %s; env -0" % self.edex_command, stdout=subprocess.PIPE, shell=True)
            output = proc.communicate()[0]
            env = dict((line.split("=", 1) for line in output.split('\x00') if line))
            os.environ.update(env)
        except Exception:
            logger.exception(
                "An error occurred when sourcing the EDEX server environment.")
            sys.exit(4)
        else:
            logger.info("EDEX server environment sourced.")

    def action(self, action):
        ''' Starts or stops all services. '''
        verbose_action = {'start': 'start', 'stop': 'stopp'}[action]
        
        # Check if the action is valid.
        if action not in ("start", "stop"):
            logger.error("% is not a valid action" % action.title())
            sys.exit(4)

        logger.info("%sing all services." % verbose_action.title())
        command = [self.edex_command, "all", action]
        command_string = " ".join(command)
        try:
            if self.test_mode:
                logger.info("TEST MODE: " + command_string)
            else:
                logger.info(command_string)
                subprocess.check_output(command)
        except Exception:
            logger.exception(
                "An error occurred when %sing services." % verbose_action)
            sys.exit(4)
        else:
            ''' When EDEX is started, it takes some time for the service to be ready. A cooldown 
                setting from the config file specifies how long to wait before continuing the 
                script.'''
            if action == "start":
                logger.info("Waiting specified cooldown time (%s seconds)" % self.cooldown)
                sleep(self.cooldown)

            # Check to see if all processes were started or stopped, and exit if there's an issue.
            logger.info("Checking service statuses and refreshing process IDs.")
            if self.refresh_status() == {'start': True, 'stop': False}[action]:
                logger.info("All services %sed." % verbose_action)
            else:
                logger.error("There was an issue %sing the services." % verbose_action)
                logger.error(self.process_ids)
                sys.exit(4)

    def restart(self):
        ''' Restart all services.'''
        self.action("stop")
        self.action("start")

    def refresh_status(self):
        ''' Run the edex-server script's status command to get and store process IDs for all 
            services, as well as determine the actual PID for the EDEX application.
            Returns True if all services have PIDs, and False if any one service doesn't. '''
        self.process_ids = {}
        try:
            if self.test_mode:
                status = "edex_ooi:   632\npostgres:   732\nqpidd:   845\npypies: 948 7803 \n"
            else:
                status = subprocess.check_output([self.edex_command, "all", "status"])
        except Exception:
            logger.exception(
                "An error occurred when checking the service statuses.")
            sys.exit(4)
        else:
            # Parse and process the output of 'edex-server all status' into a dict.
            status = [s.strip() for s in status.split('\n') if s.strip()]
            for s in status:
                name, value = s.split(":")
                value = value.strip().split(" ")
                if len(value) == 1:
                    value = value[0]
                self.process_ids[name] = value

            ''' Determine the child processes for edex_ooi to get the actual PID of the EDEX 
                application. '''
            if self.test_mode:
                self.process_ids['edex_wrapper'], self.process_ids['edex_server'] = "test", "test"
            else:
                self.process_ids['edex_wrapper'] = \
                    shell.pgrep("-P", self.process_ids["edex_ooi"])[1].split('\n')[0]
                if self.process_ids['edex_wrapper']:
                    self.process_ids['edex_server'] = \
                        shell.pgrep("-P", self.process_ids["edex_wrapper"])[1].split('\n')[0]
                else:
                    self.process_ids['edex_server'] = None
        return all(self.process_ids.itervalues())

    def wait_until_ready(self, previous_data_file):
        ''' Sits in a loop until all services are up and running. '''
        crashed = False
        while True:
            if self.refresh_status():
                if crashed:
                    email_notifications.send(
                        "Service Crash During Ingestion",
                        ("One or more EDEX services crashed after ingesting the previous data file "
                        "(%s). The services were restarted successfully and ingestion will continue."
                        ) % previous_data_file, 
                        )
                return
            logger.warn((
                "One or more EDEX services crashed after ingesting the previous data file "
                "(%s). Attempting to restart services." % previous_data_file
                ))
            crashed = True
            self.restart()

    def process_log(self, log_file):
        ''' Processes an EDEX log and creates a new log file with only the relevant, 
            searchable data. '''

        new_log_file = "/".join((EDEX['processed_log_path'], log_file.split("/")[-1] + ".p"))

        # Check to see if the processed log file already exists.
        if os.path.isfile(new_log_file):
            log_file_timestamp = datetime.datetime.fromtimestamp(
                os.path.getmtime(log_file))
            new_log_file_timestamp = datetime.datetime.fromtimestamp(
                os.path.getmtime(new_log_file))
            ''' Check to see if the original log file has been modified since being previously 
                processed. '''
            if log_file_timestamp < new_log_file_timestamp:
                logger.info(
                    "%s has already been processed." % log_file)
                return
            else:
                logger.info((
                    "%s has already been processed, "
                    "but has been modified and will be re-processed."
                    ) % log_file)

        result = shell.zgrep("Latency", log_file)[1]
        with open(new_log_file, "w") as outfile:
            for row in result:
                outfile.write(row)
        logger.info(
            "%s has been processed and written to %s." % (log_file, new_log_file))

    def process_all_logs(self):
        ''' Processes all EDEX logs in preparation for duplicate ingestion prevention. '''

        # Build a list of all valid EDEX logs
        edex_logs  = glob("/".join((EDEX['log_path'], "edex-ooi*.log")))
        edex_logs += glob("/".join((EDEX['log_path'], "edex-ooi*.log.[0-9]*")))
        edex_logs += glob("/".join((EDEX['log_path'], "*.zip")))
        edex_logs = sorted([l for l in edex_logs if ".lck" not in l])

        logger.info("Pre-processing log files for duplicate searching.")
        for log_file in edex_logs:
            self.process_log(log_file)
        return glob("/".join((EDEX['processed_log_path'], "*.p")))

class Ingestor(object):
    ''' A helper class designed to handle the ingestion process.'''

    def __init__(self, **options):
        set_options(self, (
                'test_mode', 'force_mode', 'sleep_timer', 
                'start_date', 'end_date', 'max_file_age', 
                'quick_look_quantity'), 
            options)
        self.queue = []
        self.failed_ingestions = []

        ''' Instantiate a ServiceManager for this Ingestor object and start the services if any are
            not running. '''
        self.service_manager = options.get('service_manager', ServiceManager(**options))
        if not self.service_manager.refresh_status():
            self.service_manager.action("start")

    def load_queue(self, parameters):
        ''' Finds the files that match the filename_mask parameter and loads them into the 
            Ingestor object's queue. '''

        def in_edex_log(uframe_route, datafile):
            ''' Check EDEX logs to see if the file has been ingested by EDEX.'''
            search_string = "%s.*%s" % (uframe_route, datafile)
            return bool(pipe(
                pipe.zgrep(
                    "-m1", search_string, *self.service_manager.edex_log_files) | pipe.head("-1")
                )[1])
        
        # Get a list of files that match the file mask and log the list size.
        data_files = sorted(glob(parameters['filename_mask']))

        # If a start date is set, only ingest files modified after that start date.
        if self.start_date:
            logger.info("Start date set to %s, filtering file list." % (
                self.start_date))
            data_files = [
                f for f in data_files
                if datetime.datetime.fromtimestamp(os.path.getmtime(f)) > self.start_date]

        # If a end date is set, only ingest files modified before that end date.
        if self.end_date:
            logger.info("end date set to %s, filtering file list." % (
                self.end_date))
            data_files = [
                f for f in data_files
                if datetime.datetime.fromtimestamp(os.path.getmtime(f)) < self.end_date]

        # If a maximum file age is set, only ingest files that fall within that age.
        if self.max_file_age:
            logger.info("Maximum file age set to %s seconds, filtering file list." % (
                self.max_file_age))
            current_time = datetime.datetime.now()
            age = datetime.timedelta(seconds=self.max_file_age)
            data_files = [
                f for f in data_files
                if current_time - datetime.datetime.fromtimestamp(os.path.getmtime(f)) < age]

        logger.info(
            "%s file(s) found for %s before filtering." % (
                len(data_files), parameters['filename_mask']))

        # Check if the data_file has previously been ingested. If it has, then skip it, unless 
        # force mode (-f) is active.
        filtered_data_files = []
        logger.info(
            "Determining if any files have already been ingested to %s." % parameters['uframe_route'])
        for data_file in data_files:
            file_and_queue = "%s (%s)" % (data_file, parameters['uframe_route'])
            if in_edex_log(parameters['uframe_route'], data_file):
                if self.force_mode:
                    logger.warning((
                        "EDEX logs indicate that %s has already been ingested, "
                        "but force mode (-f) is active. The file will be reingested."
                        ) % file_and_queue)
                else:
                    logger.warning((
                        "EDEX logs indicate that %s has already been ingested. "
                        "The file will not be reingested."
                        ) % file_and_queue)
                    continue
            filtered_data_files.append(data_file)

        # If no files are found, consider the entire filename mask a failure and track it.
        if len(filtered_data_files) == 0:
            self.failed_ingestions.append(parameters)
            return False

        ''' If a quick look quantity is set (either through the config.yml or the command-line 
            argument), truncate the size of the list down to the specified quantity. '''
        if self.quick_look_quantity and self.quick_look_quantity < len(filtered_data_files):
            before_quick_look = len(filtered_data_files)
            filtered_data_files = filtered_data_files[:self.quick_look_quantity]
            logger.info(
                "%s of %s file(s) from %s set for quick look ingestion." % (
                    len(filtered_data_files), before_quick_look, parameters['filename_mask']))
        else:
            logger.info(
                "%s file(s) from %s set for ingestion." % (
                    len(filtered_data_files), parameters['filename_mask']))

        parameters['data_files'] = filtered_data_files
        self.queue.append(parameters)

    def send(self, data_files, uframe_route, reference_designator, data_source):
        ''' Calls UFrame's ingest sender application with the appropriate command-line arguments 
            for all files specified in the data_files list. '''

        # Define some helper methods.
        def annotate_parameters(file, route, designator, source):
            ''' Turn the ingestion parameters into a dictionary with descriptive keys.'''
            return {
                'filename_mask': file, 
                'uframe_route': route, 
                'reference_designator': designator, 
                'data_source': source,
                }

        # Ingest each file in the file list.
        previous_data_file = ""
        for data_file in data_files:
            # Check if the EDEX services are still running. If not, attempt to restart them.
            self.service_manager.wait_until_ready(previous_data_file)

            ingestion_command = (
                UFRAME['command'], uframe_route, data_file, reference_designator, data_source)
            try:
                # Attempt to send the file to UFrame's ingest sender.
                ingestion_command_string = " ".join(ingestion_command)
                if self.test_mode:
                    ingestion_command_string = "TEST MODE: " + ingestion_command_string
                else:
                    subprocess.check_output(ingestion_command)
            except subprocess.CalledProcessError as e:
                # If UFrame's ingest sender fails and returns a non-zero exit code, log it.
                logger.error(
                    "There was a problem with UFrame when ingesting %s (Error code %s)." % (
                        data_file, e.returncode))
                self.failed_ingestions.append(
                    annotate_parameters(data_file, uframe_route, reference_designator, data_source))
            except Exception:
                # If there is some other system issue, log it with traceback.
                logger.exception(
                    "There was an unexpected system error when ingesting %s" % data_file)
                self.failed_ingestions.append(
                    annotate_parameters(data_file, uframe_route, reference_designator, data_source))
            else:
                # If there are no errors, consider the ingest send a success and log it.
                logger.info(ingestion_command_string)
            previous_data_file = data_file
            sleep(self.sleep_timer)
        return True

    def load_queue_from_csv(self, csv_file):
        ''' Reads the specified CSV file for mask, route, designator, and source parameters and 
            loads the Ingestor object's queue with a batch with those matching parameters.'''

        try:
            reader = csv.DictReader(open(csv_file))
        except IOError:
            logger.error("%s not found." % csv_file)
            return False
        fieldnames = ['uframe_route', 'filename_mask', 'reference_designator', 'data_source']
        if reader.fieldnames != fieldnames:
            logger.error("%s does not have valid column headers." % csv_file)
            return False

        # Load the queue with parameters from each row.
        for row in reader:
            self.load_queue(row)

    def ingest_from_queue(self):
        ''' Call the ingestion command for each batch of files in the Ingestor object's queue. '''
        for batch in self.queue:
            filename_mask = batch.pop('filename_mask')
            logger.info(
                "Ingesting %s files for %s from the queue." % (
                    len(batch['data_files']), filename_mask))
            self.send(**batch)

    def write_queue_to_file(self, command_file=None):
        ''' Write the ingestion command for each file to be ingested to a log file. '''
        today_string = datetime.datetime.today().strftime('%Y_%m_%d_%H_%M')
        commands_file = command_file or \
            UFRAME['log_path'] + '/commands_' + today_string + '.log'
        with open(commands_file, 'w') as outfile:
            for batch in self.queue:
                for data_file in batch['data_files']:
                    ingestion_command = " ".join((
                        UFRAME['command'], 
                        batch['uframe_route'], 
                        data_file, 
                        batch['reference_designator'], 
                        batch['data_source'])) + "\n"
                    outfile.write(ingestion_command)
        logger.info('Wrote queue to %s.' % commands_file)

    def write_failures_to_csv(self, label):
        ''' Write any failed ingestions out into a CSV file that can be re-ingested later. '''

        date_string = datetime.datetime.today().strftime('%Y_%m_%d')
        fieldnames = ['uframe_route', 'filename_mask', 'reference_designator', 'data_source']
        outfile = "%s/failed_ingestions_%s_%s.csv" % (
            UFRAME["failed_ingestion_path"], label, date_string)

        writer = csv.DictWriter(
            open(outfile, 'wb'), delimiter=',', fieldnames=fieldnames)

        logger.info(
            "Writing %s failed ingestion(s) out to %s" % (len(self.failed_ingestions), outfile))
        writer.writerow(dict((fn,fn) for fn in fieldnames))
        for f in self.failed_ingestions:
            writer.writerow(f)

if __name__ == '__main__':
    # If the -h argument is passed at the command line, display the internal documentation and exit.
    if "-h" in sys.argv:
        sys.stdout.write(INTERNAL_DOCUMENTATION)
        sys.exit(0)
    
    # Separate the task and arguments and run the task with the arguments.
    task, args = sys.argv[1], sys.argv[2:]
    perform = Task(args)
    if task in Task.valid_tasks:
        logger.info("-")
        logger.info(
            "Running ingestion task '%s' with command-line arguments '%s'" % (
                task, " ".join(args)))
        try:
            getattr(perform, task)()
        except Exception:
            logger.exception("There was an unexpected error.")
    else:
        logger.error("%s is not a valid ingestion task." % task)
    