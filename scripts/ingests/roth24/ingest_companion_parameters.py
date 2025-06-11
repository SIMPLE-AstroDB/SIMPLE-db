from astrodb_utils import load_astrodb
from astrodb_utils.publications import (
    logger,
    ingest_publication,
    find_publication
)
import sys
sys.path.append(".")
from simple.schema import *
from simple.schema import REFERENCE_TABLES
from astropy.io import ascii
import logging
import sqlalchemy.exc
from sqlalchemy import create_engine
from sqlalchemy import MetaData, Table, Column, Integer, String, Float

logger = logging.getLogger("AstroDB")
logger.setLevel(logging.INFO)
SAVE_DB = False  # save the data files in addition to modifying the .db file
RECREATE_DB = True  # recreates the .db file from the data files
# LOAD THE DATABASE
db = load_astrodb("SIMPLE.sqlite", recreatedb=RECREATE_DB, reference_tables=REFERENCE_TABLES)

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

# Create a database engine
#engine = create_engine("sqlite:////Users/kasey/Documents/GitHub/SIMPLE-db/SIMPLE.sqlite", echo=True)

metadata = MetaData() #metadata stores the table schema and database structure

CompanionParameters = Table(
    "CompanionParameters", metadata,
    Column("source", String),
    Column("parameter", String),
    Column("value", String),
    Column("value_error", String),
    Column("upper_error", String),
    Column("lower_error", String),
    Column("unit", String),
    Column("comments", String),
    Column("ref", String)
)

metadata.create_all(db.engine) #ensures table is created if it doens't already exist

def extractADS(link):
    start = link.find("abs/") + 4
    end = link.find("/abstract")
    ads = link[start:end]
    ads = ads.replace("%26", "&")
    logger.debug(f"ads: {ads}")
    return ads


for row in byw_table:
    source_name = row["Source"]
    parameter = row["Parameter"]
    value = row["Value"]
    value_error = row["Value_error"]
    upper_error = row["upper_error"]
    lower_error = row["lower_error"]
    unit = row["Unit"]
    comments = row["Comments"]
    #Austin said that the ones in the sheet that he provided a link for have not been ingested so I am doing that here
    if "https://" in row["Ref"]:
        ads = extractADS(row["Ref"])
        ingest_publication(db = db, bibcode=ads)
        source_reference = find_publication(db, bibcode=ads)
        ref = source_reference[1]
    else:
        ref = row["Ref"]
    with db.engine.connect() as conn:
            conn.execute(
                CompanionParameters.insert().values(
                    {
                        "source": source_name,
                        "parameter": parameter,
                        "value": value,
                        "value_error": value_error,
                        "upper_error": upper_error,
                        "lower_error": lower_error,
                        "unit": unit,
                        "comments": comments,
                        "ref": ref
                    }
                )
            )
            conn.commit()

if SAVE_DB:
    db.save_database(directory="data/")