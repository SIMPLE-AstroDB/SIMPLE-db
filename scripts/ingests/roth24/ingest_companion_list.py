from astrodb_utils import load_astrodb
from astrodbkit.schema_example import *
from astrodb_utils.publications import (
    logger,
)
from astrodb_utils.sources import coords_from_simbad
import sys
sys.path.append(".")
from simple import REFERENCE_TABLES
from astropy.io import ascii
import logging

logger = logging.getLogger("AstroDB")
logger.setLevel(logging.INFO)
SAVE_DB = True  # save the data files in addition to modifying the .db file
RECREATE_DB = True  # recreates the .db file from the data files
SCHEMA_PATH = "simple/schema.yaml" 
# LOAD THE DATABASE
# Was not being properly loaded before because the load_astrodb didn't previously include the pointer to the schema 
# which is what the felis_schema is
db = load_astrodb("SIMPLE.sqlite", recreatedb=RECREATE_DB, reference_tables=REFERENCE_TABLES, felis_schema=SCHEMA_PATH)

link = (
    "scripts/ingests/roth24/bywCompanionList.csv"
)

# read the csv data into an astropy table
# ascii.read attempts to read data from local files rather from URLs so using a library like requests helps get data and create object that can be passed to ascii.read
byw_table = ascii.read(
    link, #change this to the path for the csv file
    format="csv",
    data_start=1, #starts reading data from the second line
    header_start=0, #specifies that column names are in the first line
    guess=False,
    fast_reader=False,
    delimiter=",", #specifies the character that separates datafields
)

ingested = 0



for row in byw_table:
    
    companion = row["Host"]
    


    with db.engine.connect() as conn:
        conn.execute(
            db.CompanionList.insert().values(
                {
                    "companion": companion,
                }
            )
        )
        conn.commit()
        print(coords_from_simbad(companion))
        ingested+=1

print(ingested)
if SAVE_DB:
    db.save_database(directory="data/")