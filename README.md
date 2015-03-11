# python-ingestion
Python scripts for ingestion of UFrame data.

**Usage**

    python ingest.py [task] [options]

**Tasks**

| Task           | Description |
| -------------- | ----------- |
| from_csv       | Ingest data based on parameters stored in a CSV file. Takes a *file* argument. Contains the filename masks, uframe routes, reference designators, and data sources. This file must have the .csv extension. |
| from_csv_batch | Ingest data from multiple CSV files. Takes a *file* argument. Contains the filenames of CSV files that will be ingested. This file must have a .csv.batch extension.|
| dummy          | A dummy task that only instantiates an Ingestor object. |

**Options**

| Switch          | Description |
| --------------- | ----------- |
| -t              | puts the script in test mode. No data will actually be ingested.|
| -f              | forces the script to disregard the EDEX log file checks for already ingested data. |
| --sleep=*n*     | overrides the sleep timer with a value of *n* seconds. |
| --age=*n*       | overrides the maximum age of the files to be ingested in *n* seconds. |
| --cooldown=*n*  | overrides the EDEX service startup cooldown timer with a value of *n* seconds. |
| --quick=*n*     | overrides the number of files per filemask to ingest. Used for quick look ingestions. |
