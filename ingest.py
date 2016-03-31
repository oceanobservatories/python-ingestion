#!/usr/bin/env python

import os, argparse
from glob import glob
from datetime import datetime

import logging

from ingestion import Ingestor, log_and_exit

import ingestion.config as config
import ingestion.logger as logger

parser = argparse.ArgumentParser(description="Ingest data to UFrame.")
subparsers = parser.add_subparsers(dest="task")

# From CSV (from_csv)
parser_from_csv = subparsers.add_parser('from_csv', 
    help="Ingest using parameters in a CSV file.")
parser_from_csv.add_argument('files', nargs='*', 
    help="Path to CSV file.")

# From File (from_file)
parser_single_file = subparsers.add_parser('from_file', 
    help="Ingest a single file.")
parser_single_file.add_argument('files', nargs='*',
    help="Path to data file.")
parser_single_file.add_argument('uframe_route', 
    help="UFrame route.")
parser_single_file.add_argument('reference_designator', 
    help="Reference Designator.")
parser_single_file.add_argument('data_source', 
    help="Data source (i.e. telemetered, recovered, etc.).")
parser_single_file.add_argument('deployment_number', 
    help="Deployment number.")

# Dummy (dummy)
parser_dummy = subparsers.add_parser('dummy', 
    help="Create ingestor but don't ingest any data.")

# Optional Arguments
parser.add_argument('-v', '--verbose', action='store_true',
    help="Verbose mode. Logging messages will output to console.")
parser.add_argument('-t', '--test', action='store_true',
    help="Test mode. No ingestions will be sent to UFrame.")
parser.add_argument('-f', '--force', action='store_true',
    help="Force mode. EDEX logs will not be checked for previous ingestions of the specified data.")
parser.add_argument('-no-edex', action='store_true',
    help="Don't check to see if EDEX is alive after every send.")
parser.add_argument('--sleep_timer', type=int, default=config.SLEEP_TIMER, metavar="N",
    help="Override the sleep timer with a value of N seconds.")
parser.add_argument('--start', default=config.START_DATE, metavar="YYYY-MM-DD",
    help="Only ingest files newer than the specified date in the YYYY-MM-DD format.")
parser.add_argument('--end', default=config.END_DATE, metavar="YYYY-MM-DD",
    help="Only ingest files older than the specified date in the YYYY-MM-DD format.")
parser.add_argument('--age', type=int, default=config.MAX_FILE_AGE, metavar="N",
    help="Only ingest files that are N seconds old or less.")
parser.add_argument('--cooldown', type=int, default=config.EDEX['cooldown'], metavar="N",
    help="Wait N seconds after EDEX services are started before ingesting.")
parser.add_argument('--quick', type=int, default=config.QUICK_LOOK_QUANTITY, metavar="N",
    help="Ingest a maximum of N files per CSV.")
parser.add_argument('--qpid_host', type=str, default=config.QPID['host'], metavar="host",
    help="The QPID server hostname.")
parser.add_argument('--qpid_port', type=str, default=config.QPID['port'], metavar="port",
    help="The QPID server port.")
parser.add_argument('--qpid_user', type=str, default=config.QPID['user'], metavar="username",
    help="The QPID server username.")
parser.add_argument('--qpid_password', type=str, default=config.QPID['password'], metavar="password",
    help="The QPID server password.")

class Task(object):
    ''' A helper class designed to manage the different types of ingestion tasks.'''

    def __init__(self, args):
        self.logger = logging.getLogger('Task')

        def parse_date(date_string):
            if date_string:
                try:
                    return datetime.strptime(date_string, "%Y-%m-%d")
                except ValueError:
                    self.logger.error("Date must be in YYYY-MM-DD format")
                    log_and_exit(5)
            return None

        self.args = args

        self.options = {
            'test_mode': self.args.test, 
            'force_mode': self.args.force,
            'no_edex': self.args.no_edex,
            'sleep_timer': self.args.sleep_timer,
            'max_file_age': self.args.age,
            'start_date': parse_date(self.args.start),
            'end_date': parse_date(self.args.end),
            'cooldown': self.args.cooldown,
            'quick_look_quantity': self.args.quick,
            'edex_command': config.EDEX['command'],
            'health_check_enabled': config.EDEX['health_check_enabled'],
            'qpid_host': self.args.qpid_host,
            'qpid_port': self.args.qpid_port,
            'qpid_user': self.args.qpid_user,
            'qpid_password': self.args.qpid_password,
            }

    def execute(self):
        getattr(self, self.args.task)()

    def dummy(self):
        ''' The dummy task is used for testing basic initialization functions. It creates an 
            Ingestor (which in turn creates a ServiceManager) and outputs all of the script's 
            options to the log. '''
        ingestor = Ingestor(**self.options)
        self.logger.info("Dummy task was run with the following options:")
        for option in sorted(["%s: %s" % (o, self.options[o]) for o in self.options]):
            self.logger.info(option)

    def from_csv(self):
        ''' Ingest from specified CSV files.'''
        timestamp_logname = "from_csv_" + datetime.today().strftime('%Y_%m_%d_%H_%M_%S')
        csv_files = [f for f in self.args.files if f.endswith('.csv')]
        if not csv_files:
            self.logger.error("No CSV files found.")
            return False

        # Create an instance of the Ingestor class with common options set.
        ingestor = Ingestor(**self.options)

        # Ingest from each CSV file.
        for csv_file in csv_files:
            data_groups = Ingestor.process_csv(csv_file)
            for mask, routes, deployment_number in data_groups:
                ingestor.load_queue(mask, routes, deployment_number)
        ingestor.ingest_from_queue()

        # Write out any failed ingestions from the entire batch to a new CSV file.
        if ingestor.failed_ingestions:
            ingestor.write_failures_to_csv(timestamp_logname)

        self.logger.info('')
        self.logger.info("Ingestion completed.")
        return True

    def from_file(self):
        timestamp_logname = "from_file_" + datetime.today().strftime('%Y_%m_%d_%H_%M_%S')

        ingestor = Ingestor(**self.options)

        for f in self.args.files:
            ingestor.load_queue(
                mask=f,
                routes={
                    'uframe_route': self.uframe_route,
                    'reference_designator': self.reference_designator,
                    'data_source': self.data_source, },
                deployment_number=self.deployment_number)
        ingestor.ingest_from_queue()

        self.logger.info('')
        self.logger.info("Ingestion completed.")
        return True

args = parser.parse_args()

task = Task(args)

if __name__ == '__main__':
    # Setup Logging
    log_file = "_".join(
        ("ingestion", args.task, datetime.today().strftime('%Y_%m_%d_%H_%M_%S'))) + ".log"
    logger.setup_logging(log_file=log_file, verbose=args.verbose)
    main_logger = logging.getLogger('Main')
    logging.getLogger("requests").setLevel(logging.WARNING)

    # Run the task with the arguments.
    task_start_time = datetime.now()
    args_string = ", ".join(["%s: %s" % (a, vars(args)[a]) for a in vars(args)])
    main_logger.info(
        "Running ingestion task '%s' with the following options: '%s'" % (args.task, args_string))
    main_logger.info('')
    try:
        task.execute()
    except Exception:
        main_logger.exception("There was an unexpected error.")

    time_elapsed = datetime.now() - task_start_time
    main_logger.info("Task completed in %s." % str(time_elapsed).split('.')[0])
