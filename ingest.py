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
                        The script will go through all of the motions of ingesting data, but will 
                        not call any ingest sender commands.
                -c  Commands-only Mode. 
                        The script will write the ingest sender commands to a file for all files in 
                        the queue, but will not go through the ingestion process.
                -f  Force Mode. 
                        The script will disregard the EDEX log file checks for already ingested 
                        data and ingest all matching files.
         -no-email  Don't send email notifications.
         --sleep=n  Override the sleep timer with a value of n seconds.
     --startdate=d  Only ingest files newer than the specified start date d (in the YYYY-MM-DD 
                    format).
       --enddate=d  Only ingest files older than the specified end date d (in the YYYY-MM-DD 
                    format).
           --age=n  Override the maximum age of the files to be ingested in n seconds.
      --cooldown=n  Override the EDEX service startup cooldown timer with a value of n seconds.
         --quick=n  Override the number of files per filemask to ingest. Used for quick look 
                    ingestions.

Error Codes:
                 4  There is a problem with the EDEX server.
                 5  An integer value was not specified for any of the override options.

'''

import sys, os, subprocess
import logging, logging.config, mailinglogger
import csv
from datetime import datetime, timedelta
from time import sleep
from glob import glob
from whelk import shell, pipe

from config import (
    SERVER, SLEEP_TIMER, 
    MAX_FILE_AGE, START_DATE, END_DATE, QUICK_LOOK_QUANTITY, 
    UFRAME, EDEX, EMAIL)

import logger
import email_notifications

import qpid.messaging as qm

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
        'no_email': False,
        }
    for attr in attrs:
        setattr(object, attr, options.get(attr, defaults[attr]))

def log_and_exit(error_code):
    exit_logger = logging.getLogger('Exit')
    if error_code != 0:
        exit_logger.error("Script exited with error code %s." % error_code)
    exit_logger.info("-")
    sys.exit(error_code)

class QpidSender:
    ''' A helper class for sending ingest messages to ooi uframe with qpid.'''
    def __init__(self, address, host="localhost", port=5672, user="guest",
            password="guest"):
        self.host=host
        self.port=port
        self.user=user
        self.password=password
        self.address=address

    def connect(self):
        self.connection = qm.Connection(host=self.host, port=self.port,
                username=self.user, password=self.password)
        self.connection.open()
        self.session = self.connection.session()
        self.sender = self.session.sender(self.address)

    def send(self, message, content_type, sensor, delivery_type, deployment_number):
        self.sender.send(qm.Message(content=message,
                                    content_type=content_type,
                                    user_id=self.user,
                                    properties={"sensor":sensor,
                                                "deliveryType":delivery_type,
                                                "deploymentNumber": deployment_number}))

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

        self.logger = logging.getLogger('Task')

        def get_date(date_string):
            if date_string:
                return datetime.strptime(date_string, "%Y-%m-%d")
            return None

        def switch_value(switch, converter):
            value_error_messages = {
                int: "%s must be set to an integer" % switch,
                get_date: "%s must be in YYYY-MM-DD format" % switch,
                }
            switch = "--%s=" % switch
            try:
                return converter([a for a in args if a[:len(switch)]==switch][0].split("=")[1])
            except IndexError:
                return None
            except ValueError:
                self.logger.error(value_error_messages[converter])
                log_and_exit(5)

        self.options = {
            'test_mode': "-t" in args, 
            'force_mode': "-f" in args,
            'commands_only': '-c' in args, 
            'no_email': '-no-email' in args, 
            'sleep_timer': switch_value("sleep", int) or SLEEP_TIMER,
            'max_file_age': switch_value("age", int) or MAX_FILE_AGE,
            'start_date': switch_value("startdate", get_date) or get_date(START_DATE),
            'end_date': switch_value("enddate", get_date) or get_date(END_DATE),
            'cooldown': switch_value("cooldown", int) or EDEX['cooldown'],
            'quick_look_quantity': switch_value("quick", int) or QUICK_LOOK_QUANTITY,
            'edex_command': EDEX['command'],
            }
        self.args = args
    
        # Create a Mailer for non-logging based email notifications.
        self.mailer = email_notifications.Mailer(self.options)

    def dummy(self):
        ''' The dummy task is used for testing basic initialization functions. It creates an 
            Ingestor (which in turn creates a ServiceManager) and outputs all of the script's 
            options to the log and sends an email notification with the same information. '''
        ingestor = Ingestor(**self.options)
        self.logger.info("Dummy task was run with the following options:")
        for option in sorted(["%s: %s" % (o, self.options[o]) for o in self.options]):
            self.logger.info(option)
        self.mailer.options_summary()

    def from_csv(self):
        ''' Ingest data mapped out by a single CSV file. '''

        # Check to see if a valid CSV has been specified.
        try:
            csv_file = [f for f in self.args if f[-4:].lower()==".csv"][0]
        except IndexError:
            self.logger.error("No mapping CSV specified.")
            return False

        # Create an instance of the Ingestor class with common options set.
        ingestor = Ingestor(**self.options)

        # Ingest from the CSV file.
        ingestor.load_queue_from_csv(csv_file)
        ingestor.write_queue_to_file(
            csv_file.split("/")[-1].split(".")[0])
        if not self.options['commands_only']:
            ingestor.ingest_from_queue()

        # Write out any failed ingestions to a new CSV file.
        if ingestor.failed_ingestions:
            ingestor.write_failures_to_csv(
                csv_file.split("/")[-1].split(".")[0])

        self.logger.info('')
        self.logger.info("Ingestion completed.")
        self.mailer.ingestion_completed(csv_file)
        return True

    def from_csv_batch(self):
        ''' Ingest data mapped out by multiple CSV files, defined in a single .csv.batch file.'''

        # Create an instance of the Ingestor class with common options set.
        ingestor = Ingestor(**self.options)

        # Open the batch list file and parse the csv paths into a list
        try:
            csv_batch = [f for f in self.args if f[-10:].lower()==".csv.batch"][0]
        except IndexError:
            self.logger.error("No CSV batch file specified.")
            return False
        with open(csv_batch, 'r') as f:
            csv_files = [x.strip() for x in f.readlines() if x.strip()]

        # Ingest from each CSV file.
        for csv_file in csv_files:
            ingestor.load_queue_from_csv(csv_file)
        ingestor.write_queue_to_file(
            csv_batch.split("/")[-1].split(".")[0] + "_batch")
        if not self.options['commands_only']:
            ingestor.ingest_from_queue()

        # Write out any failed ingestions from the entire batch to a new CSV file.
        if ingestor.failed_ingestions:
            ingestor.write_failures_to_csv(
                csv_batch.split("/")[-1].split(".")[0] + "_batch")

        self.logger.info('')
        self.logger.info("Ingestion completed.")
        self.mailer.ingestion_completed(csv_batch)
        return True

class ServiceManager(object):
    ''' A helper class that manages the services that the ingestion depends on.'''

    def __init__(self, **options):
        set_options(self, ('test_mode', 'edex_command', 'cooldown'), options)

        self.logger = logging.getLogger('Services')

        # Process all logs.
        self.edex_log_files = self.process_all_logs()

        # Source the EDEX server environment.
        if self.test_mode or EDEX['fake_source']:
            self.logger.info("TEST MODE: Sourcing the EDEX server environment.")
            self.logger.info("TEST MODE: EDEX server environment sourced.")
            return

        # Source the EDEX environment.
        try:
            self.logger.info("Sourcing the EDEX server environment.")
            # Adapted from http://pythonwise.blogspot.fr/2010/04/sourcing-shell-script.html
            proc = subprocess.Popen(
                ". %s; env -0" % self.edex_command, stdout=subprocess.PIPE, shell=True)
            output = proc.communicate()[0]
            env = dict((line.split("=", 1) for line in output.split('\x00') if line))
            os.environ.update(env)
        except Exception:
            self.logger.exception(
                "An error occurred when sourcing the EDEX server environment.")
            log_and_exit(4)
        else:
            self.logger.info("EDEX server environment sourced.")

    def action(self, action):
        ''' Starts or stops all services. '''
        
        # Check if the action is valid.
        if action not in ("start", "stop"):
            self.logger.error("% is not a valid action" % action.title())
            log_and_exit(4)
        verbose_action = {'start': 'start', 'stop': 'stopp'}[action]

        self.logger.info("%sing all services." % verbose_action.title())
        command = [self.edex_command, "all", action]
        command_string = " ".join(command)
        try:
            if self.test_mode:
                self.logger.info("TEST MODE: " + command_string)
            else:
                self.logger.info(command_string)
                subprocess.check_output(command)
        except Exception:
            self.logger.exception(
                "An error occurred when %sing services." % verbose_action)
            log_and_exit(4)
        else:
            ''' When EDEX is started, it takes some time for the service to be ready. A cooldown 
                setting from the config file specifies how long to wait before continuing the 
                script.'''
            if action == "start":
                self.logger.info("Waiting specified cooldown time (%s seconds)" % self.cooldown)
                sleep(self.cooldown)

            # Check to see if all processes were started or stopped, and exit if there's an issue.
            self.logger.info("Checking service statuses and refreshing process IDs.")
            if self.refresh_status() == {'start': True, 'stop': False}[action]:
                self.logger.info("All services %sed." % verbose_action)
            else:
                self.logger.error("There was an issue %sing the services." % verbose_action)
                self.logger.error(self.process_ids)
                log_and_exit(4)

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
                status = "edex_ooi: test\npostgres: test\nqpidd: test\npypies: test test \n"
            else:
                status = subprocess.check_output([self.edex_command, "all", "status"])
        except Exception:
            self.logger.exception(
                "An error occurred when checking the service statuses.")
            log_and_exit(4)
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
            def child_process(parent_name):
                return shell.pgrep("-P", self.process_ids[parent_name])[1].split('\n')[0]
            if self.test_mode:
                self.process_ids['edex_wrapper'], self.process_ids['edex_server'] = "test", "test"
            else:
                self.process_ids['edex_wrapper'] = child_process("edex_ooi")
                if self.process_ids['edex_wrapper']:
                    self.process_ids['edex_server'] = child_process("edex_wrapper")
                else:
                    self.process_ids['edex_server'] = None
        return all(self.process_ids.itervalues())

    def wait_until_ready(self, previous_data_file):
        ''' Sits in a loop until all services are up and running. '''
        crashed = False
        while True:
            if self.refresh_status():
                if crashed:
                    self.logger.error((
                        "One or more EDEX services crashed after ingesting the previous data file "
                        "(%s). The services were restarted successfully and ingestion will continue."
                        ) % previous_data_file)
                return
            self.logger.warn((
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
            log_file_timestamp = datetime.fromtimestamp(
                os.path.getmtime(log_file))
            new_log_file_timestamp = datetime.fromtimestamp(
                os.path.getmtime(new_log_file))
            ''' Check to see if the original log file has been modified since being previously 
                processed. '''
            if log_file_timestamp < new_log_file_timestamp:
                self.logger.info(
                    "%s has already been processed." % log_file)
                return
            else:
                self.logger.info((
                    "%s has already been processed, "
                    "but has been modified and will be re-processed."
                    ) % log_file)

        result = shell.zgrep("Finished Processing file", log_file)[1]
        with open(new_log_file, "w") as outfile:
            for row in result:
                outfile.write(row)
        self.logger.info(
            "%s has been processed and written to %s." % (log_file, new_log_file))

    def process_all_logs(self):
        ''' Processes all EDEX logs in preparation for duplicate ingestion prevention. '''

        # Build a list of all valid EDEX logs
        edex_logs  = glob("/".join((EDEX['log_path'], "edex-ooi*.log")))
        edex_logs += glob("/".join((EDEX['log_path'], "edex-ooi*.log.[0-9]*")))
        edex_logs += glob("/".join((EDEX['log_path'], "*.zip")))
        edex_logs = sorted([l for l in edex_logs if ".lck" not in l])

        self.logger.info("Pre-processing log files for duplicate searching.")
        for log_file in edex_logs:
            self.process_log(log_file)
        return glob("/".join((EDEX['processed_log_path'], "*.p")))

class Ingestor(object):
    ''' A helper class designed to handle the ingestion process.'''

    def __init__(self, **options):
        self.logger = logging.getLogger('Ingestor')

        set_options(self, (
                'test_mode', 'force_mode', 'sleep_timer', 
                'start_date', 'end_date', 'max_file_age', 
                'quick_look_quantity'), 
            options)
        self.queue = []
        self.failed_ingestions = []

        ''' Instantiate a ServiceManager for this Ingestor and start the services if any are not 
            running. '''
        self.service_manager = options.get('service_manager', ServiceManager(**options))
        if not self.service_manager.refresh_status():
            self.service_manager.action("start")

    def load_queue(self, parameters):
        ''' Finds the files that match the filename_mask parameter and loads them into the Ingestor
            object's queue. '''

        # Check EDEX logs to see if any file matching the mask has been ingested.
        mask_search_string = "%s.*%s" % (
            parameters['uframe_route'], 
            parameters['filename_mask'].replace("*", ".*")
            ),

        mask_in_logs = shell.zgrep(
            mask_search_string, 
            *self.service_manager.edex_log_files
            )[1]

        def in_edex_log(uframe_route, filemask):
            ''' Check EDEX logs to see if the file has been ingested by EDEX.'''
            if not mask_in_logs:
                return False
            search_string = "%s.*%s" % (uframe_route, filemask)
            return bool(pipe(
                    pipe.grep(mask_search_string, *self.service_manager.edex_log_files) | pipe.grep("-m1", search_string ) | pipe.head("-1")
                )[1])
        
        # Get a list of files that match the file mask and log the list size.
        data_files = sorted(glob(parameters['filename_mask']))

        # Grab the deployment number.
        # The filename mask structure might change pending decision from the MIOs.
        try:
            parameters['deployment_number'] = str(int([
                n for n 
                in parameters['filename_mask'].split("/") 
                if len(n)==6 and n[0] in ('D', 'R', 'X')
                ][0][1:]))
        except:
            self.logger.error(
                "Can't get deployment number from %s." % parameters['filename_mask'])
            self.failed_ingestions.append(parameters)
            return False

        self.logger.info('')
        self.logger.info(
            "%s file(s) found for %s before filtering." % (
                len(data_files), parameters['filename_mask']))

        # If a start date is set, only ingest files modified after that start date.
        if self.start_date:
            self.logger.info("Start date set to %s, filtering file list." % (
                self.start_date))
            data_files = [
                f for f in data_files
                if datetime.fromtimestamp(os.path.getmtime(f)) > self.start_date]

        # If a end date is set, only ingest files modified before that end date.
        if self.end_date:
            self.logger.info("end date set to %s, filtering file list." % (
                self.end_date))
            data_files = [
                f for f in data_files
                if datetime.fromtimestamp(os.path.getmtime(f)) < self.end_date]

        # If a maximum file age is set, only ingest files that fall within that age.
        if self.max_file_age:
            self.logger.info("Maximum file age set to %s seconds, filtering file list." % (
                self.max_file_age))
            current_time = datetime.now()
            age = timedelta(seconds=self.max_file_age)
            data_files = [
                f for f in data_files
                if current_time - datetime.fromtimestamp(os.path.getmtime(f)) < age]

        # Check if the data_file has previously been ingested. If it has, then skip it, unless 
        # force mode (-f) is active.
        filtered_data_files = []
        self.logger.info(
            "Determining if any files have already been ingested to %s." % parameters['uframe_route'])
        for data_file in data_files:
            file_and_queue = "%s (%s)" % (data_file, parameters['uframe_route'])
            if self.force_mode:
                pass
            else:
                if bool(mask_in_logs):
                    if in_edex_log(parameters['uframe_route'], data_file):
                        self.logger.warning((
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
            self.logger.info(
                "%s of %s file(s) from %s set for quick look ingestion." % (
                    len(filtered_data_files), before_quick_look, parameters['filename_mask']))
        else:
            self.logger.info(
                "%s file(s) from %s set for ingestion." % (
                    len(filtered_data_files), parameters['filename_mask']))

        parameters['data_files'] = filtered_data_files
        self.queue.append(parameters)

    def load_queue_from_csv(self, csv_file):
        ''' Reads the specified CSV file for mask, route, designator, and source parameters and 
            loads the Ingestor object's queue with a batch with those matching parameters.'''

        try:
            reader = csv.DictReader(open(csv_file))
        except IOError:
            self.logger.error("%s not found." % csv_file)
            return False
        fieldnames = ['uframe_route', 'filename_mask', 'reference_designator', 'data_source']
        if not set(fieldnames).issubset(reader.fieldnames):
            self.logger.error((
                "%s does not have valid column headers. "
                "The following columns are required: %s") % (csv_file, ", ".join(fieldnames)))
            return False

        def commented(row):
            ''' Check to see if the row is commented out. Any field that starts with # indictes 
                a comment.'''
            return bool([v for v in row.itervalues() if v.startswith("#")])

        # Load the queue with parameters from each row.
        for row in reader:
            if not commented(row):
                self.load_queue({f: row[f] for f in row if f in fieldnames})

    def send(self, data_files, uframe_route, reference_designator, data_source, deployment_number):
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

            ingestion_command = ( "ingestsender", uframe_route, data_file, reference_designator, data_source, deployment_number)
            try:
                # Attempt to send the data file over qpid to uframe.
                ingestion_command_string = " ".join(ingestion_command)
                if self.test_mode:
                    ingestion_command_string = "TEST MODE: " + ingestion_command_string
                else:
                    qpid_sender = QpidSender(address=uframe_route)
                    qpid_sender.connect()
                    qpid_sender.send(data_file, "text/plain", reference_designator, data_source, deployment_number)
            except qm.exceptions.MessagingError as e:
                # Log any qpid errors
                self.logger.error(
                    "There was a problem with qpid when ingesting %s (Exception %s)." % (
                        data_file, e))
                self.failed_ingestions.append(
                    annotate_parameters(data_file, uframe_route, reference_designator, data_source))
            except Exception:
                # If there is some other system issue, log it with traceback.
                self.logger.exception(
                    "There was an unexpected system error when ingesting %s" % data_file)
                self.failed_ingestions.append(
                    annotate_parameters(data_file, uframe_route, reference_designator, data_source))
            else:
                # If there are no errors, consider the ingest send a success and log it.
                self.logger.info(ingestion_command_string)
            previous_data_file = data_file
            sleep(self.sleep_timer)
        return True

    def ingest_from_queue(self):
        ''' Call the ingestion command for each batch of files in the Ingestor object's queue. '''
        for batch in self.queue:
            filename_mask = batch.pop('filename_mask')
            self.logger.info('')
            self.logger.info(
                "Ingesting %s files for %s from the queue." % (
                    len(batch['data_files']), filename_mask))
            self.send(**batch)

    def write_queue_to_file(self, command_file=None):
        ''' Write the ingestion command for each file to be ingested to a log file. '''
        today_string = datetime.today().strftime('%Y_%m_%d_%H_%M_%S')
        if command_file:
            commands_file = "_".join(("commands", command_file, today_string)) + '.log'
        else:
            commands_file = 'commands_' + today_string + '.log'
        commands_file = "/".join((UFRAME['log_path'], commands_file))
        with open(commands_file, 'w') as outfile:
            for batch in self.queue:
                for data_file in batch['data_files']:
                    ingestion_command = " ".join((
                        UFRAME['command'], 
                        batch['uframe_route'], 
                        data_file, 
                        batch['reference_designator'], 
                        batch['data_source'],
                        batch['deployment_number'],
                        )) + "\n"
                    outfile.write(ingestion_command)
        self.logger.info('')
        self.logger.info('Wrote ingestion commands for files in queue to %s.' % commands_file)

    def write_failures_to_csv(self, label):
        ''' Write any failed ingestions out into a CSV file that can be re-ingested later. '''

        date_string = datetime.today().strftime('%Y_%m_%d')
        fieldnames = ['uframe_route', 'filename_mask', 'reference_designator', 'data_source', 'deployment_number']
        outfile = "%s/failed_ingestions_%s_%s.csv" % (
            UFRAME["failed_ingestion_path"], label, date_string)

        writer = csv.DictWriter(
            open(outfile, 'wb'), delimiter=',', fieldnames=fieldnames)

        self.logger.info(
            "Writing %s failed ingestion(s) out to %s" % (len(self.failed_ingestions), outfile))
        writer.writerow(dict((fn,fn) for fn in fieldnames))
        for f in self.failed_ingestions:
            writer.writerow(f)

if __name__ == '__main__':
    # Separate the task and arguments.
    task, args = sys.argv[1], sys.argv[2:]

    # Setup Logging
    log_file_name = "_".join((
        "ingestion", task, datetime.today().strftime('%Y_%m_%d_%H_%M_%S'),
        )) + ".log"
    logger.setup_logging(
        log_file_name=log_file_name,
        send_mail="-no-email" not in args and EMAIL['enabled'])
    main_logger = logging.getLogger('Main')

    # If the -h argument is passed at the command line, display the internal documentation and exit.
    if "-h" in sys.argv:
        sys.stdout.write(INTERNAL_DOCUMENTATION)
        log_and_exit(0)

    # Run the task with the arguments.
    perform = Task(args)
    if task in Task.valid_tasks:
        main_logger.info("-")
        main_logger.info(
            "Running ingestion task '%s' with command-line arguments '%s'" % (
                task, " ".join(args)))
        try:
            getattr(perform, task)()
        except Exception:
            main_logger.exception("There was an unexpected error.")
    else:
        main_logger.error("%s is not a valid ingestion task." % task)
