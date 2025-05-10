from astrodb_utils import load_astrodb
from astrodb_utils.sources import (
    AstroDBError,
    find_publication,
)
from astrodb_utils.publications import(
    ingest_publication
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
logger.setLevel(logging.DEBUG)  # uncomment to see debug messages

DB_SAVE = True
RECREATE_DB = True

SCHEMA_PATH = "simple/schema.yaml"
db = load_astrodb(
    "SIMPLE.sqlite",
    recreatedb=RECREATE_DB,
    reference_tables=REFERENCE_TABLES,
    felis_schema=SCHEMA_PATH,
)

link = (
    "scripts/ingests/bones_archive/bones_archive_spectra_fixed.csv"
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
already_exists = 0
pub_ingested=0

def find_regime(source):
    regime = "unknown"
    optical = source["OPTICAL_SPECTRUM"]
    nir = source["NIR_SPECTRUM"]
    if optical is not None:
        regime = "optical"
    if nir is not None:
        regime = "nir"
    return regime

#helper method for extracting ads key from link
def extractADS(link):
    start = link.find('abs/')+4
    end = link.find('/abstract')
    ads = link[start:end]
    ads = ads.replace("%26", "&")
    return ads

#ingest ULAS J074431.30+283915.6 separately
ingest_spectral_type(
    db,
    source="ULAS J074431.30+283915.6",
    spectral_type_string="sdM7",
    regime="unknown",
    comments="From the BONES archive",
    reference = "Ahn_12"
)
ingested+=1

for source in bones_sheet_table:
    bones_name = source["NAME"].replace("\u2212", "-")
    bones_name = bones_name.replace("\u2212", "-")
    bones_name = bones_name.replace("\u2013", "-")
    bones_name = bones_name.replace("\2014", "-")
    bones_spectra = source["LIT_SPT"]

    try:
        sp_regime = find_regime(source)
        ads = extractADS(source["ADS_LINK"])
        ref = find_publication(db=db, bibcode = ads)
        #if publication not in db, ingest
        try:
            if ref[0] == False:
                ingest_publication(db = db, bibcode = ads)
                pub_ingested +=1
        except:
            logger.warning("Find and ingest pattern didn't work" + ads)
        ref = find_publication(db=db, bibcode=ads)
        #ingest the spectral types
        ingest_spectral_type(
            db,
            source=source["NAME"],
            spectral_type_string=bones_spectra,
            regime=sp_regime,
            comments="From the BONES archive",
            reference = ref[1]
        )
        ingested += 1
    except AstroDBError as e:
        msg = "ingest failed with error: " + str(e)
        logger.warning(msg)
        if "Spectral type already in the database" in str(e):
            already_exists+=1
        skipped+=1

total = len(bones_sheet_table)
logger.info(f"skipped: {skipped}")  # 32 skipped
logger.info(f"spectra_ingested: {ingested}")  # 178 ingested
logger.info(f"Already exists: {already_exists}") # 31 already exists in database
logger.info(f"Publications ingested: {pub_ingested}") #7 new publications ingested
logger.info(f"total: {total}")  # 209 total
if DB_SAVE:
    db.save_database(directory="data/")
