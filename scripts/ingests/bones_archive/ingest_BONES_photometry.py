from astrodb_utils import load_astrodb
from simple.schema import *
from simple.schema import REFERENCE_TABLES
from astrodb_utils import (
    load_astrodb,
    find_source_in_db,
    AstroDBError,
    ingest_names,
    ingest_publication,
)
from astrodb_utils.photometry import ingest_photometry
import sys

sys.path.append(".")
import logging
from astropy.io import ascii
from simple.schema import Photometry
from simple.schema import REFERENCE_TABLES
from math import isnan
import sqlalchemy.exc
from simple.utils.astrometry import ingest_parallax

logger = logging.getLogger(__name__)
names_ingested = 0
photometry_ingested = 0
skipped = 0
total =0
duplicate_measurement = 0
multiple_sources = 0
no_sources = 0

# Logger setup
# This will stream all logger messages to the standard output and
# apply formatting for that
logger.propagate = False  # prevents duplicated logging messages
LOGFORMAT = logging.Formatter(
    "%(asctime)s %(levelname)s: %(message)s", datefmt="%m/%d/%Y %I:%M:%S%p"
)
ch = logging.StreamHandler(stream=sys.stdout)
ch.setFormatter(LOGFORMAT)
# To prevent duplicate handlers, only add if they haven't been set previously
if len(logger.handlers) == 0:
    logger.addHandler(ch)
logger.setLevel(logging.INFO)

DB_SAVE = False
RECREATE_DB = True
db = load_astrodb("SIMPLE.sqlite", recreatedb=RECREATE_DB, reference_tables=REFERENCE_TABLES)

link = (
    "scripts/ingests/bones_archive/bones_archive_photometry.csv"
)

# read the csv data into an astropy table
bones_sheet_table = ascii.read(
    link,
    format="csv",
    data_start=1,
    header_start=0,
    guess=False,
    fast_reader=False,
    delimiter=",",
)

for source in bones_sheet_table:
    bones_name = source["NAME"]
    match = None
    if isnan(source["GAIA_G"]):
        skipped+=1
        continue
    ##tries to find match of name in database
    ##if found, ingest
    if len(bones_name) > 0 and bones_name != "null":
        match = find_source_in_db(
            db,
            source["NAME"],
            ra=source["RA"],
            dec=source["DEC"],
            ra_col_name="ra",
            dec_col_name="dec",
        )
        if len(match) == 1:
            try:
                ingest_names(
                    db, match[0], bones_name
                )  # ingest new names while were at it
                names_ingested += 1
            except AstroDBError as e:
                None  # only error is if there is a preexisting name anyways.

        if(match == None):
            match = find_source_in_db(
                db,
                source("NAME"),
                ra=source["RA"],
                dec=source["DEC"],
                ra_col_name="ra",
                dec_col_name="dec",
            )
        if len(match) == 1:
            simple_source = match[0]
            try: 
                band_filter = "GAIA2.G"
                measurement = source["GAIA_G"]
                telescope = "Gaia"
                error = source["GAIA_G_E"]
                reference = "GaiaDR2"
                ingest_photometry(
                    db,
                    source = simple_source,
                    band = band_filter,
                    magnitude = float(measurement),
                    magnitude_error = float(error),
                    telescope = telescope,
                    reference = reference,
                    raise_error = True,
                    regime = "optical",
                )
                photometry_ingested+=1
            except AstroDBError as e:
                msg = "ingest failed with error: " + str(e)
                logger.warning(msg)
                skipped +=1
                if "The measurement may be a duplicate." in str(e) :
                    duplicate_measurement+=1
                else:
                    raise AstroDBError(msg) from e
        elif len(match) == 0:
            skipped+=1
            no_sources+=1
        else:
            skipped+=1
            multiple_sources+=1

total = len(bones_sheet_table)
logger.info(f"skipped:{skipped}") #192 skipped
logger.info(f"photometry_ingested:{photometry_ingested}") #17 ingested
logger.info(f"no sources:{no_sources}") #69 no sources
logger.info(f"multiple_sources:{multiple_sources}") #0 multiple sources
logger.info(f"total:{total}") #209 total
if DB_SAVE:
    db.save_database(directory="data/")
