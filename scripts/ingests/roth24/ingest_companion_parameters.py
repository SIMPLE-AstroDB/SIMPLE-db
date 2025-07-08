from astrodb_utils import load_astrodb
from astrodbkit.schema_example import *
from astrodb_utils.publications import (
    logger,
    ingest_publication,
    find_publication
)
from astrodb_utils.sources import (
    find_source_in_db,
    AstroDBError,
    ingest_source,
    find_publication,
)

import sys
sys.path.append(".")
from simple import REFERENCE_TABLES
from astropy.io import ascii
import logging
import sqlalchemy.exc
from sqlalchemy import create_engine
from sqlalchemy import MetaData, Table, Column, Integer, String, Float

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
    "scripts/ingests/roth24/bywCompanionParameters.csv"
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


def extractADS(link):
    start = link.find("abs/") + 4
    end = link.find("/abstract")
    ads = link[start:end]
    ads = ads.replace("%26", "&")
    logger.debug(f"ads: {ads}")
    return ads


for row in byw_table:
    source_name = find_source_in_db(
       db,
       row["Source"],
       ra=row["RA"],
       dec=row["DEC"],
       ra_col_name="ra",
       dec_col_name="dec",
    )[0]
    companion = row["Host"]
    parameter = row["Parameter"].lower()
    value = row["Value"]
    upper_error = row["upper_error"]
    lower_error = row["lower_error"]
    unit = row["Unit"]
    print("printing source:" + source_name)
    comments = str(row["Comments"])
    if(comments == "--"):
        comments = None
    #the ones in the sheet that are provided a link for have not been ingested so do that here
    if "https://" in row["Ref"]:
        ads = extractADS(row["Ref"])
        publication_exists = find_publication(db=db, bibcode=ads)
        if publication_exists[0] == False:
            ingest_publication(db = db, bibcode=ads)
            
        source_reference = find_publication(db, bibcode=ads)
        reference = source_reference[1].replace("\u017d", "Z")
    else:
        reference = row["Ref"]

    with db.engine.connect() as conn:
            conn.execute(
                db.CompanionParameters.insert().values(
                    {
                        "source": source_name,
                        "companion": companion,
                        "parameter": parameter,
                        "value": value,
                        "upper_error": upper_error,
                        "lower_error": lower_error,
                        "unit": unit,
                        "comments": comments,
                        "reference": reference
                    }
                )
            )
            conn.commit()

if SAVE_DB:
    db.save_database(directory="data/")