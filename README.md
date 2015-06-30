# python-ingestion
Python scripts for ingestion of UFrame data.

## Setup 
Requires Python 2.7.x

A standard practice for deploying a Python application is to create a virutalenv to house the libraries the application will use in a sandboxed environment. If it isn't already installed, install virtualenv (https://virtualenv.pypa.io/en/latest/).

Source the EDEX environment on the server to ensure that the version of Python being used is 2.7.x, otherwise the virutalenv will be created with the wrong version of Python. Check the Python version to make sure.

    python --version

Create and activate a new virtualenv by following the instructions in the virutalenv User Guide (https://virtualenv.pypa.io/en/latest/userguide.html).

Ensure the virutalenv is activated and install the libraries using pip:

    pip install -r requirements.txt

Copy the config.yml.template to config.yml and edit the new config.yml file to specify the correct paths for the various configuration options. Use the comments in the config.yml file as a guide and create any directories as necessary. Whenever possible, use absolute paths.

Command line options override options set in the config.yml file.

## Usage

Remember to activate the virtualenv before using the script.

    Usage: python ingest.py [task] [options]

    Tasks:
           from_csv  Ingest data from a CSV file. 
                     Requires a filename argument with a .csv extension.
     from_csv_batch  Ingest data from multiple CSV files defined in a batch file.  
                     Requires a filename argument with a .csv.batch extension.
           from_dir  Ingest data from CSVs contained in the specified directory.
                     Requires a path argument.
         single_file Only ingest a single file. This option will ingest a signle data file.  Depending on the rest
                     of the command line options this option will search for the correct values to send to the ingest queue. 
                     If only a file is given it will search the ingest_csvs directory for the file containing all of the ingest
                     parameters needed.  if --param `location` is passed it will read the location to see if it is the csv file that contains
                     the correct parameters and load them.  If it is not the correct csv file it will begin a recursive search from that location
                     to find the file.  
                     Finally the parameters can all be passed to the command line to ingest a file if known: uframe_route file reference_designator data_source deployment
              dummy  A dummy task that creates an Ingestor but doesn't try to ingest any data. 
                     Used for testing.

    Options:
                 -h  Display this help message.
                 -v  Verbose mode. Outputs the script's INFO and ERROR messages to the console while the script runs.
                 -t  Test Mode. 
                         The script will go through all of the motions of ingesting data, but will not 
                         call any ingest sender commands.
                 -c  Commands-only Mode. 
                         The script will write the ingest sender commands to a file for all files in 
                         the queue, but will not go through the ingestion process.
                 -f  Force Mode. 
                        The script will disregard the EDEX log file checks for already ingested data 
                        and ingest all matching files.
          -no-email  Don't send email notifications.
    --no-check-edex  Don't check to see if edex is alive after every input.
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
