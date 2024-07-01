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
from scripts.ingests.ultracool_sheet.references import uc_ref_to_simple_ref

logger = logging.getLogger("AstroDB")
logger.setLevel(logging.INFO)

DB_SAVE = True
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
    if isnan(source["ch1"]) and isnan(source["ch2"]):  # skip if no data
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
        for val in [["IRAC.I1", "ch1", "ch1err"], ["IRAC.I2", "ch2", "ch2err"]]:
            band, magnitude, mag_error = val
            if isnan(source[magnitude]):
                continue

            comment = None
            references = source["ref_Spitzer"].split(";")  # may have 2 references
            if len(references) > 1:
                comment = "Second reference: " + uc_ref_to_simple_ref(db, references[1])
            table_data = {
                "source": simple_source,
                "band": band,
                "magnitude": source[magnitude],
                "magnitude_error": source[mag_error],
                "telescope": "Spitzer",
                "comments": comment,
                "reference": uc_ref_to_simple_ref(db, references[0]),
            }
            try:
                pho_obj = Photometry(**table_data)
                with db.session as session:
                    session.add(pho_obj)
                    session.commit()
                logger.info(f" Photometry added to database: {table_data}\n")
                ingested += 1
            except sqlalchemy.exc.IntegrityError as e:
                msg = f"Could not add {table_data} to database. Error: {e}"
                logger.warning(msg)
                if "UNIQUE constraint failed" not in str(e):
                    raise AstroDBError(msg) from e
                already_exists += 1
    elif len(match) == 0:
        no_sources += 1
        if not (isnan(source["ch1"]) or isnan(source["ch2"])):
            no_sources += 1
    else:
        multiple_sources += 1
        if not (isnan(source["ch1"]) or isnan(source["ch2"])):
            multiple_sources += 1

# 1491 data points in UC sheet in total, 8 with 2 references
logger.info(f"ingested:{ingested}")  # 899 ingested
logger.info(f"already exists:{already_exists}")  # skipped 463 due to preexisting data
logger.info(f"no sources:{no_sources}")  # skipped 129 due to 0 matches
logger.info(f"multiple sources:{multiple_sources}")  # skipped 0 due to multiple matches
logger.info(f"total:{ingested+already_exists+no_sources+multiple_sources}")  # 1491
if DB_SAVE:
    db.save_database(directory="data/")
