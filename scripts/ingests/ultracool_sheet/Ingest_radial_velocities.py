from astrodb_utils import (
    load_astrodb,
    find_source_in_db,
    AstroDBError,
)
import logging
from astropy.io import ascii
from simple.schema import RadialVelocities
from simple.schema import *  # import all database tables
from simple.schema import REFERENCE_TABLES
from math import isnan
import sqlalchemy.exc
import sys

sys.path.append(".")
from scripts.ingests.ultracool_sheet.references import uc_ref_to_simple_ref

logger = logging.getLogger("AstroDB")
logger.setLevel(logging.INFO)

DB_SAVE = False
RECREATE_DB = False
db = load_astrodb(
    "SIMPLE.sqlite", recreatedb=RECREATE_DB, reference_tables=REFERENCE_TABLES
)

# Load Ultracool sheet
doc_id = "1i98ft8g5mzPp2DNno0kcz4B9nzMxdpyz5UquAVhz-U8"  # Last update: 2024-02-04 23:29:26 (UTC)
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


def ingest_radial_velocity(
    source: str = None,
    rv: float = None,
    rv_err: float = None,
    reference: str = None,
    raise_error: bool = True,
):
    rv_data = {
        "source": source,
        "radial_velocity_km_s": rv,
        "radial_velocity_error_km_s": rv_err,
        "reference": reference,
    }

    try:
        rv_obj = RadialVelocities(**rv_data)
        with db.session as session:
            session.add(rv_obj)
            session.commit()
        logger.info(f" Radial Velocity added to database: {rv_data}\n")
    except sqlalchemy.exc.IntegrityError as e:
        if (
            "UNIQUE constraint failed: RadialVelocities.source, RadialVelocities.reference"
            in str(e)
        ):
            msg = f"Radial Velocity with same reference already exists in database: {rv_data}"
            if raise_error:
                raise AstroDBError(msg) from e
            else:
                msg2 = f"SKIPPING: {source}. Radial Velocity with same reference already exists in database."
                logger.warning(msg2)
        else:
            msg = f"Could not add {rv_data} to database. Error: {e}"
            logger.warning(msg)
            raise AstroDBError(msg) from e


bad_sources = [
    "2MASS J02192210-3925225B",
    "beta Pic b",
]  # due to coordinate overlap or similar
n_no_match = 0
n_multiple_sources = 0
n_added = 0
missing = []
n_no_rv = 0
no_rv = []
n_no_rv_err = 0

# uc_sheet_table = uc_sheet_table[0:50]

# Ingest loop
for source in uc_sheet_table:
    uc_sheet_name = source["name"]

    # Skip if no radial velocity in the ultracool sheet
    if isnan(source["rv_lit"]):
        n_no_rv += 1
        no_rv.append(uc_sheet_name)
        continue

    match = find_source_in_db(
        db,
        uc_sheet_name,
        ra=source["ra_j2000_formula"],
        dec=source["dec_j2000_formula"],
    )

    if len(match) == 1:
        # 1 Match found. INGEST radial velocity!
        simple_source = match[0]
        if uc_sheet_name in bad_sources:
            continue
        logger.info(f"Match found for {uc_sheet_name}: {simple_source}")

        try:
            ingest_radial_velocity(
                source=simple_source,
                rv=source["rv_lit"],
                rv_err=source["rverr_lit"],
                reference=uc_ref_to_simple_ref(db, source["ref_rv_lit"]),
                raise_error=False,
            )
            n_added += 1
            if isnan(source["rverr_lit"]):
                n_no_rv_err += 1
        except AstroDBError as e:
            logger.error(e)

    elif len(match) == 0:
        n_no_match += 1
        missing.append(uc_sheet_name)
        msg = f"SKIPPING: {uc_sheet_name}. No match in SIMPLE."
        logger.warning(msg)

    else:
        n_multiple_sources += 1

logger.info(f"no rv in ultracool sheet: {n_no_rv}")  # skipped 0 due to no rv

logger.info(f"RVs added to SIMPLE: {n_added}")  # added 1015 rvs

# Write no source match list to file
file_name = "scripts/ingests/ultracool_sheet/missing_rv_sources.txt"
with open(file_name, "w") as file:
    for source in missing:
        file.write(source + "\n")
logger.info(
    f"No source match in SIMPLE: {n_no_match}. Wrote list to {file_name}"
)  # skipped 244 due to 0 matches


logger.info(
    f"Multiple source matches: {n_multiple_sources}"
)  # skipped 0 due to multiple matches

logger.info(f"No RV error: {n_no_rv_err}")

n_rows = n_no_rv + n_added + n_no_match + n_multiple_sources + len(bad_sources)
if n_rows != len(uc_sheet_table):
    logger.warning(
        f"Number of rows in Ultracool sheet: {len(uc_sheet_table)}. Number of rows processed: {n_rows}"
    )

if DB_SAVE:
    db.save_database(directory="data/")
