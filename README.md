# python-ingestion
Python scripts for ingestion of UFrame data.

## Setup 
Requires Python 2.7.x

### The Virtual Environment
The standard practice for deploying a Python application is to create a virutalenv to house the libraries the application will use in a sand-boxed environment. This way, installed libraries don't interfere with system libraries (and vice versa), and the application will have no issues with incorrect dependencies.

If it isn't already installed, install [virtualenv](https://virtualenv.pypa.io/en/latest/).

Source the EDEX environment on the server to ensure that the version of Python being used is 2.7.x, otherwise the virutalenv will be created with the wrong version of Python. Check the Python version to make sure.

    python --version

Create and activate a new virtualenv by following the instructions in the [virutalenv User Guide](https://virtualenv.pypa.io/en/latest/userguide.html).

Ensure the virutalenv is activated and install the required libraries using pip:

    pip install -r requirements.txt

Creating a virutal environment for the application to run inside is of **paramount importance**! Don't just source the the EDEX environment and run the ```ingest.py```. The script has specific libraries tied to specific versions that it needs in order to run without issue.


### config.py

In the ```ingestion``` folder, copy ```config.yml.template``` to ```config.yml``` and edit it to specify the correct paths for the various configuration options. Use the comments in ```config.yml``` as a guide and create any directories as necessary. Whenever possible, use absolute paths.

Command line options override options set in the config.yml file.

## Usage

**Always activate the virutal environment before using the script.**

List any optional arguments before specifying the task. For example:

```ingest.py -t -v --sleep_timer 3 --quick 6 from_csv live_ingestions/*.csv```

The ```--help``` output:

    usage: ingest.py [-h] [-v] [-t] [-f] [-no-edex] [--sleep_timer N]
                     [--start YYYY-MM-DD] [--end YYYY-MM-DD] [--age N]
                     [--cooldown N] [--quick N] [--qpid_host host]
                     [--qpid_port port] [--qpid_user username]
                     [--qpid_password password]
                     {from_csv,from_file,dummy} ...

    tasks
      from_csv            Ingest using parameters in a CSV file.
      from_file           Ingest a single file.
      dummy               Create ingestor but don't ingest any data.
    
    optional arguments:
      -h, --help                show this help message and exit
      -v, --verbose             Verbose mode. Logging messages will output to console.
      -t, --test                Test mode. No ingestions will be sent to UFrame.
      -f, --force               Force mode. EDEX logs will not be checked for previous
                                ingestions of the specified data.
      -no-edex                  Don't check to see if EDEX is alive after every send.
      --sleep_timer N           Override the sleep timer with a value of N seconds.
      --start YYYY-MM-DD        Only ingest files newer than the specified date in the
                                YYYY-MM-DD format.
      --end YYYY-MM-DD          Only ingest files older than the specified date in the
                                YYYY-MM-DD format.
      --age N                   Only ingest files that are N seconds old or less.
      --cooldown N              Wait N seconds after EDEX services are started before
                                ingesting.
      --quick N                 Ingest a maximum of N files per CSV.
      --qpid_host host          The QPID server hostname.
      --qpid_port port          The QPID server port.
      --qpid_user username      The QPID server username.
      --qpid_password password  The QPID server password.
    


## Error Codes
The script will return specific error codes if it encounters certain issues duing the ingestion process.

    4  There is a problem with the EDEX server.
    5  An integer value was not specified for any of the override options.
