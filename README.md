# python-ingestion
Python scripts for ingestion of UFrame data.

Usage:

    python ingest.py [task] [options]

**Valid Tasks**

| Task           | Description |
| -------------- | ----------- |
| from_csv       | Ingest data based on parameters stored in a CSV file. Takes a *file* argument. Contains the filename masks, uframe routes, reference designators, and data sources. This file must have the .csv extension. |
| from_csv_batch | Ingest data from multiple CSV files. Takes a *file argument. Contains the filenames of CSV files that will be ingested. This file must have a .csv.batch extension.|
| dummy          | A dummy task that only instantiates an Ingestor object. |

**Common Options**

| Switch    | Description |
| --------- | ----------- |
| -t        | an optional switch that puts the script in test mode. No data will actually be ingested.|
| -f        | an optional switch that will force the script to disregard the EDEX log file checks for already ingested data. |
| --sleep=*n* | an optional switch that overrides the sleep timer with a value of *n* seconds. |

