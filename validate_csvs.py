import logging, logging.config
import csv
from StringIO import StringIO
from github import Github
from glob import glob

from config import GITHUB_TOKEN

logging.config.dictConfig({
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'simple': {
            'format': '%(levelname)-7s | %(asctime)s | %(name)-8s | %(message)s',
            },
        'raw': {
            'format': '%(message)s',
            },
        },
    'handlers': {
        'file_handler': {
            'class': 'logging.FileHandler',
            'level': 'INFO',
            'formatter': 'simple',
            'filename': "validate_csv.log",
            'mode': 'w',
            },
        'stream_handler': {
            'class': 'logging.StreamHandler',
            'level': 'ERROR',
            'formatter': 'raw',
            'stream': 'ext://sys.stdout',
            },
        },
    'loggers': {
        '': {
            'level': 'INFO',
            'handlers': ['file_handler', 'stream_handler', ],
            'propagate': False,
            },
        },
    })

g = Github(GITHUB_TOKEN)
r = [
    o for o 
    in g.get_user().get_orgs() 
    if o.name=='OOI Integration'
    ][0].get_repo('ingestion-csvs')

CSV_FILES = {}

log = logging.getLogger('Main')

def find_csvs(repo, filepath):
    for item in repo.get_dir_contents(filepath):
        if item.type == "dir":
            find_csvs(repo, item.path)
        elif item.type == "file":
            CSV_FILES[item.path] = StringIO(item.decoded_content)
            log.info(item.path)

log.info("Verifying CSVs stored at %s" % r.html_url)
find_csvs(r, ".")

for f in CSV_FILES:
    try:
        reader = csv.DictReader(CSV_FILES[f])
        log.info("")
        log.info("Checking file paths in %s" % f) 
        for row in reader:
            files = glob(row["filename_mask"])
            action = {True: "info", False: "warning"}[bool(len(files))]
            getattr(log, action)(
                "%s file(s) found for %s in %s" % (len(files), row["filename_mask"], f))
    except Exception:
        log.exception(f)
