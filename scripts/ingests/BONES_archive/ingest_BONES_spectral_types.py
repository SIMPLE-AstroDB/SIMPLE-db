from astrodb_utils import load_astrodb
from astrodb_utils.sources import (
    find_source_in_db,
    AstroDBError,
    logger
)

import sys

sys.path.append(".")
from astropy.io import ascii


from simple.utils.spectral_types import(
    ingest_spectral_type
)
from simple import REFERENCE_TABLES

DB_SAVE = False
RECREATE_DB = True
SCHEMA_PATH = "simple/schema.yaml"
db = load_astrodb(
    "SIMPLE.sqlite",
    recreatedb=RECREATE_DB,
    reference_tables=REFERENCE_TABLES,
    felis_schema=SCHEMA_PATH,
)

link = (
    "scripts/ingests/bones_archive/bones_archive_spectra.csv"
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

ingested = 0
skipped = 0

def find_regime(source):
    regime = "unknown"
    optical = source["OPTICAL_SPECTRUM"]
    nir = source["NIR_SPECTRUM"]
    if optical is not None:
        regime = "optical"
    if nir is not None:
        regime = "nir"
    return regime


for source in bones_sheet_table:
    bones_name = source["NAME"].replace("\u2212", "-")
    bones_name = bones_name.replace("\u2212", "-")
    bones_name = bones_name.replace("\u2013", "-")
    bones_name = bones_name.replace("\2014", "-")
    bones_spectra = source["LIT_SPT"]
    match = None

    ##tries to find match of name in database
    ##if found, ingest
    match = find_source_in_db(
        db,
        source["NAME"],
        ra=source["RA"],
        dec=source["DEC"],
        ra_col_name="ra",
        dec_col_name="dec",
    )
    if len(match) == 1:
        #if source exists in the database, ingest the spectral type
        try: 
            sp_regime = find_regime(source)
            ingest_spectral_type(
                db,
                source = match[0], 
                spectral_type_string = bones_spectra,
                regime = sp_regime
            )
            ingested+=1
        except AstroDBError as e:
            msg = "ingest failed with error: " + str(e)
            logger.warning(msg)
            skipped +=1
    else:
        #more than one source, or source doesn't exist in the database
        skipped+=1

total = len(bones_sheet_table)
logger.info(f"skipped:{skipped}") #194 skipped
logger.info(f"spectra_ingested:{ingested}") #15 ingested
logger.info(f"total:{total}") #209 total
if DB_SAVE:
    db.save_database(directory="data/")
