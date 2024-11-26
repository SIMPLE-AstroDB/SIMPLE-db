from astrodb_utils import (
    load_astrodb,
    find_source_in_db,
    AstroDBError,
)
from astrodb_utils.photometry import ingest_photometry
import sys

sys.path.append(".")
import logging
from astropy.io import ascii
from simple.schema import REFERENCE_TABLES
from math import isnan
from scripts.ingests.ultracool_sheet.references import uc_ref_to_simple_ref

"""Everything in the Ks_2MASS columns with
a reference of McMa13 or McMa21 is from VISTA, which has a very similar Ks
filter to 2MASS."""

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
bad_reference = 0
no_data = 0

# Ingest loop
for source in uc_sheet_table:
    reference = source["ref_Ks_2MASS"]

    if reference[0:4] != "McMa":
        bad_reference += 1
        continue

    measurement = source["Ks_2MASS"]
    error = source["Kserr_2MASS"]
    band_filter = "VISTA.Ks"

    if isnan(measurement):  # skip if no data
        no_data += 1
        continue

    uc_name = source["name"]
    match = find_source_in_db(
        db,
        uc_name,
        ra=source["ra_j2000_formula"],
        dec=source["dec_j2000_formula"],
        ra_col_name="ra",
        dec_col_name="dec",
    )

    if len(match) == 1:
        # 1 Match found. INGEST!
        simple_source = match[0]
        logger.debug(f"Match found for {uc_name}: {simple_source}")

        try:
            ingest_photometry(
                db,
                source=simple_source,
                band=band_filter,
                magnitude=measurement,
                magnitude_error=error,
                telescope="VISTA",
                reference=uc_ref_to_simple_ref(db, reference),
                raise_error=True,
            )
            ingested += 1
        except AstroDBError as e:
            msg = "ingest failed with error: " + str(e)
            if "The measurement may be a duplicate" in str(e):
                already_exists += 1
            elif "Reference match failed due to bad publication" in str(e):
                bad_reference += 1
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

logger.info(f"ingested:{ingested}")  # 84
logger.info(f"already exists:{already_exists}")  # 0
logger.info(f"no sources:{no_sources}")  # 114
logger.info(f"multiple sources:{multiple_sources}")  # 0
logger.info(f"bad references:{bad_reference}")  # 3692
logger.info(f"no data: {no_data}")  # 0
logger.info(
    f"data points tracked:{ingested+already_exists+no_sources+multiple_sources+bad_reference}"
)  # 3890
total = (
    ingested + already_exists + no_sources + multiple_sources + no_data + bad_reference
)
logger.info(f"total: {total}")  # 3890

if total != len(uc_sheet_table):
    msg = "data points tracked inconsistent with UC sheet"
    logger.error(msg)
    raise AstroDBError(msg)
elif DB_SAVE:
    db.save_database(directory="data/")
