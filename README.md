# python-ingestion
Python scripts for ingestion of UFrame data.

Requires Python 2.7.x

A standard practice for deploying a Python application is to create a virutalenv to house the libraries the application will use in a sandboxed environment. If it isn't already installed, install virtualenv, (https://virtualenv.pypa.io/en/latest/), create a new virtualenv, and then activate it.

Install the libraries using pip:

    pip install -r requirements.txt

Copy the config.yml.template to config.yml and edit the new config.yml file to specify the correct paths for the various configuration options. Use the comments in the config.yml file as a guide and create any directories as necessary. Whenever possible, use absolute paths.

Command line options override options set in the config.yml file.

Remember to activate the virtualenv before using the script.

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
                        The script will write the ingest sender commands to a file for all files in 
                        the queue, but will not go through the ingestion process.
                -f  Force Mode. 
                        The script will disregard the EDEX log file checks for already ingested data 
                        and ingest all matching files.
         -no-email  Don't send email notifications.
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
