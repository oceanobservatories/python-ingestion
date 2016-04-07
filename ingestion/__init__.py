import sys, os, subprocess, multiprocessing
import logging, logging.config
import csv
import yaml
import requests
import re

from collections import deque
from datetime import datetime, timedelta
from time import sleep
from glob import glob
from whelk import shell, pipe
from qpid import messaging as qm

from config import LOGGING, EDEX

import logger

def log_and_exit(error_code):
    exit_logger = logging.getLogger('Exit')
    if error_code != 0:
        exit_logger.error("Script exited with error code %s." % error_code)
    exit_logger.info("-")
    sys.exit(error_code)

def set_options(object, attrs, options):
    for attr in attrs:
        setattr(object, attr, options.get(attr))

class QpidSender:
    ''' A helper class for sending ingest messages to ooi uframe with qpid.'''
    def __init__(self, address, host="localhost", port=5672, user="guest", password="guest"):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.address = address

    def connect(self):
        self.connection = qm.Connection(
            host=self.host,
            port=self.port,
            username=self.user,
            password=self.password
            )
        self.connection.open()
        self.session = self.connection.session()
        self.sender = self.session.sender(self.address)

    def send(self, message, content_type, sensor, delivery_type, deployment_number):
        self.sender.send(
            qm.Message(content=message, content_type=content_type, user_id=self.user, 
                properties={
                    "sensor": sensor,
                    "deliveryType": delivery_type,
                    "deploymentNumber": deployment_number,
                    }))

    def disconnect(self):
        self.connection.close()

class ServiceManager(object):
    ''' A helper class that manages the services that the ingestion depends on.'''

    def __init__(self, test_mode=False, force_mode=False, cooldown=60, health_check_enabled=False, edex_command=EDEX['command'], **kwargs):

        options = locals().copy()
        options.update(kwargs)
        options.pop('self')

        set_options(self, ('test_mode', 'edex_command', 'cooldown', 'health_check_enabled', ), options)

        self.logger = logging.getLogger('Services')

        if not options['force_mode']:
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
            self.logger.exception("An error occurred when sourcing the EDEX server environment.")
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
                a = subprocess.check_output(command_string)
                self.logger.info(a)
        except Exception:
            self.logger.exception("An error occurred when %sing services." % verbose_action)
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
                status = shell[self.edex_command]("all", "status")[1]
        except Exception as e:
            self.logger.exception("An error occurred when checking the service statuses.")
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
                        "One or more EDEX services crashed after ingesting the previous data file (%s)."
                        "The services were restarted successfully and ingestion will continue."
                        ) % previous_data_file)
                break
            self.logger.warn(
                ("One or more EDEX services crashed after ingesting the previous data file (%s)."
                    ) % previous_data_file)
            crashed = True
            if EDEX['auto_restart']:
                self.logger.warn("Attempting to restart the services.")
                self.restart()
            else:
                self.logger.warn("Waiting for external processes to restart the services.")
        while self.health_check_enabled:
            if requests.get(EDEX['health_check_url']).status_code == 200:
                break
            self.logger.warn("uFrame Health Check failed, pausing ingestion.")
            while True:
                if requests.get(EDEX['health_check_url']).status_code == 200:
                    break
        return True

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
                self.logger.info(
                    ("%s has already been processed, but has been modified and will be re-processed."
                        ) % log_file)

        result = shell.zgrep("Finished Processing file", log_file)[1]
        if not os.path.exists(EDEX['processed_log_path']):
            os.mkdir(EDEX['processed_log_path'])
        with open(new_log_file, "w") as outfile:
            for row in result:
                outfile.write(row)
        self.logger.info(
            "%s has been processed and written to %s." % (log_file, new_log_file))

    def process_all_logs(self):
        ''' Processes all EDEX logs in preparation for duplicate ingestion prevention. '''

        # Build a list of all valid EDEX logs
        for log_path in EDEX['log_paths']:
            edex_logs  = glob("/".join((log_path, "edex-ooi*.log")))
            edex_logs += glob("/".join((log_path, "edex-ooi*.log.[0-9]*")))
            edex_logs += glob("/".join((log_path, "*.zip")))
            edex_logs = sorted([l for l in edex_logs if ".lck" not in l])

        self.logger.info("Pre-processing log files for duplicate searching.")
        for log_file in edex_logs:
            self.process_log(log_file)
        return glob("/".join((EDEX['processed_log_path'], "*.p")))

class Ingestor(object):
    ''' A helper class designed to handle the ingestion process.'''
    logger = logging.getLogger('Ingestor')

    def __init__(self, 
            test_mode=False, force_mode=False, sleep=0, 
            start_date=None, end_date=None, max_file_age=None, 
            quick_look_quantity=None, no_edex=False, 
            qpid_host=None, qpid_port=None, qpid_user=None, qpid_password=None,
            service_manager=None, **kwargs):

        self.logger = logging.getLogger('Ingestor')

        options = locals().copy()
        options.update(kwargs)
        options.pop('self')

        set_options(self, (
                'test_mode', 'force_mode', 'sleep', 
                'start_date', 'end_date', 'max_file_age', 
                'quick_look_quantity', 'no_edex', 
                'qpid_host', 'qpid_port', 'qpid_user', 'qpid_password', 
                ),
            options)
        self.queue = deque()
        self.failed_ingestions = []
        self.qpid_senders = {}

        ''' Instantiate a ServiceManager for this Ingestor and start the services if any are not 
            running. '''
        self.service_manager = service_manager or ServiceManager(**options)
        if not self.service_manager.refresh_status():
            self.service_manager.action("start")

    @staticmethod
    def update_max_jobs(max_jobs, previous_timestamp):
        try:
            jobs_config_file = "jobs.yml"
            if os.path.isfile(jobs_config_file):
                last_updated = datetime.fromtimestamp(os.path.getmtime(jobs_config_file))
                if last_updated == previous_timestamp:
                    return max_jobs, last_updated
                else:
                    return yaml.load(open(jobs_config_file))['MAX_CONCURRENT_JOBS'], last_updated
        except:
            pass
        return 1, previous_timestamp

    def get_qpid_sender(self, route):
        ''' Connect or retrieve an already connected QPID sender for a specific route.'''
        qpid_sender = self.qpid_senders.get(route, None)
        if not qpid_sender:
            qpid_sender = QpidSender(
                address=route, 
                host=self.qpid_host, port=self.qpid_port, 
                user=self.qpid_user, password=self.qpid_password)
            qpid_sender.connect()
            self.qpid_senders[route] = qpid_sender
        return qpid_sender

    def close_qpid_connections(self):
        ''' Close all connected QPID senders. '''
        for route in self.qpid_senders:
            self.qpid_senders[route].disconnect()

    @classmethod
    def process_csv(cls, csv_file):
        ''' Reads the specified CSV file for mask, route, designator, and source parameters and 
            loads the Ingestor object's queue with a batch with those matching parameters.'''

        # Parse the deployment number from the file name.
        try:
            deployment_number = str(int([
                n for n in csv_file.split("_") 
                if len(n)==6 and n[0] in ('D', 'R', 'X')
                ][0][1:]))
        except:
            cls.logger.info('')
            cls.logger.error(
                "Can't get deployment number from %s. Will attempt to get deployment numbers from file masks." % csv_file)
            deployment_number = None

        try:
            reader = csv.DictReader(open(csv_file, "U"))
        except IOError:
            cls.logger.error("%s not found." % csv_file)
            return False
        fieldnames = ['uframe_route', 'filename_mask', 'reference_designator', 'data_source']
        if not set(fieldnames).issubset(reader.fieldnames):
            cls.logger.error((
                "%s does not have valid column headers. "
                "The following columns are required: %s") % (csv_file, ", ".join(fieldnames)))
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

        return [(mask, routes[mask], deployment_number) for mask in routes]

    def in_edex_log(self, mask, data_file, uframe_route):
        ''' Check EDEX logs to see if the file has been ingested by EDEX.'''
        return bool(pipe(
                pipe.grep(
                    "%s.*%s" % (uframe_route, mask.replace("*", ".*")), 
                    *self.service_manager.edex_log_files
                    ) | 
                pipe.grep(
                    "-m1", "%s.*%s" % (uframe_route, data_file)
                    ) | 
                pipe.head("-1")
            )[1])

    def load_queue(self, mask, routes, deployment_number):
        ''' Finds the files that match the filename_mask parameter and loads them into the 
            Ingestor object's queue. '''

        # Get a list of files that match the file mask and log the list size.
        data_files = sorted(glob(mask))

        if not deployment_number:
            # Grab the deployment number from the file name mask if no deployment number is specified.
            # The filename mask structure might change pending decision from the MIOs.
            try:
                deployment_number = str(int([
                    n for n in mask.split("/") 
                    if len(n)==6 and n[0] in ('D', 'R', 'X')
                    ][0][1:]))
            except:
                self.logger.info('')
                self.logger.error(
                    "Can't get deployment number from %s." % mask)
                for p in routes:
                    self.failed_ingestions.append(dict(filename_mask=mask, **p))
                return False

        self.logger.info('')
        self.logger.info(
            "%s file(s) found for %s before filtering." % (
                len(data_files), mask))

        # If a start date is set, only ingest files modified after that start date.
        if self.start_date:
            self.logger.info("Start date set to %s, filtering file list." % (
                self.start_date))
            data_files = [
                f for f in data_files
                if datetime.fromtimestamp(os.path.getmtime(f)) > self.start_date]

        # If a end date is set, only ingest files modified before that end date.
        if self.end_date:
            self.logger.info("End date set to %s, filtering file list." % (
                self.end_date))
            data_files = [
                f for f in data_files
                if datetime.fromtimestamp(os.path.getmtime(f)) < self.end_date]

        # If a maximum file age is set, only ingest files that fall within that age.
        if self.max_file_age:
            self.logger.info(
                "Maximum file age set to %s seconds, filtering file list." % (self.max_file_age))
            current_time = datetime.now()
            age = timedelta(seconds=self.max_file_age)
            data_files = [
                f for f in data_files
                if current_time - datetime.fromtimestamp(os.path.getmtime(f)) < age]

        ''' Check if the data_file has previously been ingested. If it has, then skip it, unless 
            force mode (-f) is active. '''
        filtered_data_files = []
        if self.force_mode:
            # If force mode is active, add all data files to the queue with the respective routes.
            for data_file in data_files:
                if self.quick_look_quantity and self.quick_look_quantity == len(filtered_data_files):
                    self.logger.info(
                        "%s of %s file(s) from %s set for quick look ingestion." % (
                            len(filtered_data_files), len(data_files), mask))
                    break
                filtered_data_files.append((data_file, routes))
        else:
            # Otherwise, check EDEX logs to see if any file matching the mask has been ingested.
            route_in_logs = {}
            for p in routes:
                route_in_logs[p['uframe_route']] = bool(pipe(
                        pipe.grep(
                            "%s.*%s" % (p['uframe_route'], mask.replace("*", ".*")), 
                            *self.service_manager.edex_log_files
                            ) |
                        pipe.head("-1")
                        )[1])

            self.logger.info(
                "Determining if any files matching %s have already been ingested." % mask)
            for data_file in data_files:
                valid_routes = []
                for p in routes:
                    uframe_route = p['uframe_route']
                    if route_in_logs[uframe_route]:
                        if self.in_edex_log(mask, data_file, uframe_route):
                            self.logger.warning((
                                "EDEX logs indicate that %s (%s) has already been ingested. "
                                "The file will not be reingested.") % (data_file, uframe_route))
                            continue
                    valid_routes.append(p)
                if len(valid_routes) > 0:
                    filtered_data_files.append((data_file, valid_routes))

                ''' If a quick look quantity is set (either through the config.yml or the 
                    command-line argument), exit the loop once the quick look quantity is met. '''
                if self.quick_look_quantity and self.quick_look_quantity == len(filtered_data_files):
                    self.logger.info(
                        "%s of %s file(s) from %s set for quick look ingestion." % (
                            len(filtered_data_files), len(data_files), mask))
                    break
            else:
                self.logger.info(
                    "%s file(s) from %s set for ingestion." % (len(filtered_data_files), mask))

        # If no files are found, consider the entire filename mask a failure and track it.
        if len(filtered_data_files) == 0:
            for p in routes:
                self.failed_ingestions.append(dict(filename_mask=mask, **p))
            return False

        self.queue.append({
            "mask": mask, 
            "files": filtered_data_files, 
            'deployment_number': deployment_number
            })

    def ingest_from_queue(self, use_billiard=False):
        ''' Call the ingestion command for each batch of files in the Ingestor object's queue, 
            using multiple processes to concurrently send batches. '''
        max_jobs, max_jobs_last_updated = self.update_max_jobs(1, datetime.now())

        self.logger.info('')
        pool = []
        while self.queue:
            batch = self.queue.popleft()
            # Wait for any job slots to become available
            while len(pool) == max_jobs:
                max_jobs, max_jobs_last_updated = self.update_max_jobs(
                    max_jobs, max_jobs_last_updated)
                pool = [j for j in pool if j.is_alive()]

            # Create, track, and start the job.
            if use_billiard:
                import billiard
                job = billiard.process.Process(
                    target=self.send, args=(batch['files'], batch['deployment_number']))
            else:
                job = multiprocessing.Process(
                    target=self.send, args=(batch['files'], batch['deployment_number']))
            pool.append(job)
            job.start()
            self.logger.info(
                "Ingesting %s files for %s from the queue in PID %s." % (
                    len(batch['files']), batch['mask'], job.pid))

        # Wait for all jobs to end completely.
        while any([job for job in pool if job.is_alive()]):
            pass

        self.logger.info("All batches completed.")

    def send(self, files, deployment_number):
        ''' Calls UFrame's ingest sender application with the appropriate command-line arguments 
            for all files specified in the files list. '''

        # Define some helper methods.
        def annotate_parameters(filename, route, designator, source):
            ''' Turn the ingestion parameters into a dictionary with descriptive keys.'''
            return {
                'filename_mask': filename, 
                'uframe_route': route, 
                'reference_designator': designator, 
                'data_source': source,
                }

        sender_process = multiprocessing.current_process()

        deployment_number = str(deployment_number)

        # Ingest each file in the file list.
        previous_data_file = ""
        for data_file, routes in files:
            for r in routes:
                uframe_route = r['uframe_route']
                reference_designator = r['reference_designator']
                data_source = r['data_source']

                # Check if the EDEX services are still running. If not, attempt to restart them.
                if not self.no_edex:
                    self.service_manager.wait_until_ready(previous_data_file)
                ingestion_command = ("ingestsender",
                    uframe_route, data_file, reference_designator, data_source, deployment_number)
                try:
                    # Attempt to send the data file over QPID to uFrame.
                    ingestion_command_string = " ".join(ingestion_command)
                    if self.test_mode:
                        ingestion_command_string = "TEST MODE: " + ingestion_command_string
                    else:
                        self.get_qpid_sender(uframe_route).send(
                            data_file, "text/plain", 
                            reference_designator, data_source, deployment_number)
                except qm.exceptions.MessagingError as e:
                    # Log any qpid errors
                    self.logger.error(
                        "There was a problem with qpid when ingesting %s (Exception %s)." % (
                            data_file, e))
                    self.failed_ingestions.append(
                        annotate_parameters(
                            data_file, uframe_route, reference_designator, data_source))
                else:
                    # If there are no errors, consider the ingest send a success and log it.
                    self.logger.info(
                        "PID: %s | %s" % (str(sender_process.pid), ingestion_command_string))
                previous_data_file = data_file
            sleep(self.sleep)
        return True

    def write_failures_to_csv(self, label):
        ''' Write any failed ingestions out into a CSV file that can be re-ingested later. '''

        date_string = datetime.today().strftime('%Y_%m_%d')
        fieldnames = [
            'uframe_route', 
            'filename_mask', 
            'reference_designator', 
            'data_source', 
            'deployment_number',
            ]
        outfile = "%s/failed_ingestions_%s.csv" % (
            LOGGING["failed"], label)

        writer = csv.DictWriter(
            open(outfile, 'wb'), delimiter=',', fieldnames=fieldnames)

        self.logger.info(
            "Writing %s failed ingestion(s) out to %s" % (len(self.failed_ingestions), outfile))
        writer.writerow(dict((fn,fn) for fn in fieldnames))
        for f in self.failed_ingestions:
            writer.writerow(f)

