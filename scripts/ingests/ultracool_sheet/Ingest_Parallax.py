from astrodb_utils import load_astrodb, find_source_in_db, AstroDBError
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


logger = logging.getLogger(__name__)

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
no_data = 0

# Ingest loop
for source in uc_sheet_table:
    if isnan(source["plx_lit"]):  # skip if no data
        no_data += 1
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
        logger.debug(f"Match found for {uc_sheet_name}: {simple_source}")

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
            if "Duplicate measurement exists" in str(e):
                already_exists += 1
            else:
                logger.warning(msg)
                raise AstroDBError(msg) from e

    elif len(match) == 0:
        no_sources += 1
    elif len(match) > 1:
        multiple_sources += 1
    else:
        msg = "Unexpected situation occured"
        logger.error(msg)
        raise AstroDBError(msg)


# 1108 data points in UC sheet in total
logger.info(f"ingested:{ingested}")  # 1013 ingested
logger.info(f"already exists:{already_exists}")  # skipped 6 due to preexisting data
logger.info(f"no sources:{no_sources}")  # skipped 86 due to 0 matches
logger.info(f"multiple sources:{multiple_sources}")  # skipped 2 due to multiple matches
logger.info(f"no data: {no_data}")
logger.info(
    f"data points tracked:{ingested+already_exists+no_sources+multiple_sources}"
)  # 1108
total = ingested + already_exists + no_sources + multiple_sources + no_data
logger.info(f"total: {total}")

if total != len(uc_sheet_table):
    msg = "data points tracked inconsistent with UC sheet"
    logger.error(msg)
    raise AstroDBError(msg)
elif DB_SAVE:
    db.save_database(directory="data/")
