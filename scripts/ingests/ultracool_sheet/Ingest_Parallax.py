from astrodb_utils import (
    load_astrodb,
    find_source_in_db,
    AstroDBError,
)
import sys

sys.path.append(".")
import logging
from astropy.io import ascii
from simple.schema import Photometry
from simple.schema import REFERENCE_TABLES
from math import isnan
import sqlalchemy.exc
from simple.utils.astrometry import ingest_parallax
from scripts.ingests.ultracool_sheet.references import uc_ref_to_simple_ref

logger = logging.getLogger("AstroDB")
logger.setLevel(logging.INFO)

DB_SAVE = False
RECREATE_DB = True
db = load_astrodb(
    "SIMPLE.sqlite", recreatedb=RECREATE_DB, reference_tables=REFERENCE_TABLES
)


# Load Ultracool sheet
doc_id = "1i98ft8g5mzPp2DNno0kcz4B9nzMxdpyz5UquAVhz-U8"
sheet_id = "361525788"
link = (
    f"https://docs.google.com/spreadsheets/d/{doc_id}/export?format=csv&gid={sheet_id}"
)

# read the csv data into an astropy table
uc_sheet_table = ascii.read(
    link,
    format="csv",
    data_start=1,
    header_start=0,
    guess=False,
    fast_reader=False,
    delimiter=",",
)

no_sources = 0
multiple_sources = 0
ingested = 0
already_exists = 0

# Ingest loop
for source in uc_sheet_table:
    if isnan(source["plx_lit"]):  # skip if no data
        continue
    uc_sheet_name = source["name"]
    match = find_source_in_db(
        db,
        uc_sheet_name,
        ra=source["ra_j2000_formula"],
        dec=source["dec_j2000_formula"],
    )

    if len(match) == 1:
        # 1 Match found. INGEST!
        simple_source = match[0]
        logger.info(f"Match found for {uc_sheet_name}: {simple_source}")

        try:
            references = source["ref_plx_lit"].split(";")
            if references[0] == "Harr15":  # weird reference in UC sheet.
                reference = "Harr15"
            else:
                reference = uc_ref_to_simple_ref(db, references[0])

            comment = None
            if len(references) > 1:
                comment = f"other references: {uc_ref_to_simple_ref(db, references[1])}"
            ingest_parallax(
                db,
                simple_source,
                source["plx_lit"],
                source["plxerr_lit"],
                reference,
                comment,
            )
            ingested += 1
        except AstroDBError as e:
            msg = "ingest failed with error: " + str(e)
            if "Duplicate measurement exists" not in str(e):
                logger.warning(msg)
                raise AstroDBError(msg) from e
            already_exists += 1
    elif len(match) == 0:
        no_sources += 1
    else:
        multiple_sources += 1


# 1108 data points in UC sheet in total
print(f"ingested:{ingested}")  # 1013 ingested
print(f"already exists:{already_exists}")  # skipped 6 due to preexisting data
print(f"no sources:{no_sources}")  # skipped 86 due to 0 matches
print(f"multiple sources:{multiple_sources}")  # skipped 2 due to multiple matches
print(f"total:{ingested+already_exists+no_sources+multiple_sources}")  # 1108
if DB_SAVE:
    db.save_database(directory="data/")
