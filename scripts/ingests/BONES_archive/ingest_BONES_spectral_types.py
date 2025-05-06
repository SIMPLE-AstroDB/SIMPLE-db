from astrodb_utils import load_astrodb
from astrodb_utils.sources import (
    AstroDBError,
)

import sys
import logging

sys.path.append(".")
from astropy.io import ascii


from simple.utils.spectral_types import(
    ingest_spectral_type
)
from simple import REFERENCE_TABLES, logger


logger.setLevel(logging.INFO)
# logger.setLevel(logging.DEBUG)  # uncomment to see debug messages

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

    try:
        sp_regime = find_regime(source)
        ingest_spectral_type(
            db,
            source=source["NAME"],
            spectral_type_string=bones_spectra,
            regime=sp_regime,
            comment="From the BONES archive",
        )
        ingested += 1
    except AstroDBError as e:
        msg = "ingest failed with error: " + str(e)
        logger.warning(msg)
        skipped += 1

total = len(bones_sheet_table)
logger.info(f"skipped: {skipped}")  # 0 skipped
logger.info(f"spectra_ingested: {ingested}")  # 209 ingested
logger.info(f"total: {total}")  # 209 total
if DB_SAVE:
    db.save_database(directory="data/")
