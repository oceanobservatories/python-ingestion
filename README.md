# python-ingestion
Python scripts for ingestion of UFrame data.

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
