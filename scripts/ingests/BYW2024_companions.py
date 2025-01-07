from astrodb_utils import load_astrodb
from astrodb_utils import ingest_publication
import sys
sys.path.append(".")
from simple.schema import *
from simple.schema import REFERENCE_TABLES
from astropy.io import ascii
from simple.utils.companions import ingest_companion_relationships
import logging

logger = logging.getLogger("AstroDB")
logger.setLevel(logging.INFO)
SAVE_DB = True  # save the data files in addition to modifying the .db file
RECREATE_DB = True  # recreates the .db file from the data files
# LOAD THE DATABASE
db = load_astrodb("SIMPLE.sqlite", recreatedb=RECREATE_DB, reference_tables=REFERENCE_TABLES)
#ingest_publication(db, doi='10.3847/1538-3881/ad324e' )
# Load Google sheet

link = 'scripts/ingests/Companion_relations.csv'
#Link to google sheet used for CSV: https://docs.google.com/spreadsheets/d/1JFa8F4Ngzp3qAW8NOBurkz4bMKo9zXYeF6N1vMtqDZs/edit?usp=sharing

# read the csv data into an astropy table
# ascii.read attempts to read data from local files rather from URLs so using a library like requests helps get data and create object that can be passed to ascii.read
byw_table = ascii.read(
    link, #change this to the path for the csv file
    format="csv",
    data_start=1,
    header_start=0,
    guess=False,
    fast_reader=False,
    delimiter=",",
)

for row in byw_table:  # skip the header row - [1:10]runs only first 10 rows
    if row['Source'] == 'CWISE J210640.16+250729.0':
        continue
    
    ingest_companion_relationships(
        db,
        source=row["Source"],
        companion_name=row["Host"],
        relationship=row["Relationship"],
        projected_separation_arcsec=row["Separation"],
        ref=row["ref"]
        )
    

    # WRITE THE JSON FILES
if SAVE_DB:
    db.save_database(directory="data/")