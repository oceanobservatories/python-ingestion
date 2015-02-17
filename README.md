# python-ingestion
Python scripts for ingestion of UFrame data.

## From CSV
Ingest data based on parameters stored in a CSV file:

    python ingest.py from_csv <parameters>.csv [-t] [--sleep=x]
*<parameters>.csv* is the file containing the filename masks, uframe routes, reference designators, and data sources.

*-t* is an optional switch that puts the script in test mode. No data will actually be ingested.

*--sleep=x* is an optional switch that overrides the sleep timer with a value of x seconds.
