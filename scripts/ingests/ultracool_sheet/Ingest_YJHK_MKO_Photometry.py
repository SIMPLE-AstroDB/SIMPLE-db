from astrodb_utils import load_astrodb, find_source_in_db, AstroDBError, ingest_names
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


def ingest_Photometry_Evan(
    source: str = None,
    band: str = None,
    magnitude: float = None,
    mag_error: float = None,
    telescope: str = None,
    # instrument: str = None,
    comment: str = None,
    reference: str = None,
    raise_error: bool = True,
):
    table_data = {
        "source": source,
        "band": band,
        "magnitude": magnitude,
        "magnitude_error": mag_error,
        "telescope": telescope,
        # "instrument": instrument,
        "comments": comment,
        "reference": reference,
    }

    try:
        pho_obj = Photometry(**table_data)
        with db.session as session:
            session.add(pho_obj)
            session.commit()
        logger.info(f" Photometry added to database: {table_data}\n")
    except sqlalchemy.exc.IntegrityError as e:
        if "UNIQUE constraint failed:" in str(e):
            msg = f" Photometry with same reference already exists in database: {table_data}"
            if raise_error:
                raise AstroDBError(msg) from e
            else:
                msg2 = f"SKIPPING: {source}. Photometry with same reference already exists in database."
                logger.warning(msg2)
        else:
            msg = f"Could not add {table_data} to database. Error: {e}"
            logger.warning(msg)
            raise AstroDBError(msg) from e


no_sources = 0
multiple_sources = 0
ingested = 0
already_exists = 0
bad_reference = 0
no_data = 0
names_ingested = 0

# Ingest loop
for source in uc_sheet_table:
    comment_filter = ""
    MKO_name = source["designation_MKO"]
    if len(MKO_name) == 0:
        # instrument = "WFCAM"
        telescope = "UKIRT"
        filter_name = "WFCAM"
        comment_filter = "WFCAM filter is a guess. Check reference for actual filter and telescope used. "
    elif MKO_name[0] == "U":
        # instrument = "WFCAM"
        telescope = "UKIRT"
        filter_name = "WFCAM"
    elif MKO_name[0] == "V":
        # instrument = "VIRCAM"
        telescope = "VISTA"
        filter_name = "VISTA"
    else:
        # instrument = "WFCAM"
        telescope = "UKIRT"
        filter_name = "WFCAM"
        comment_filter = "WFCAM filter is a guess. Check reference for actual filter and telescope used. "

    match = None
    if len(MKO_name) > 0 and MKO_name != "null":
        match = find_source_in_db(
            db,
            source["name"],
            ra=source["ra_j2000_formula"],
            dec=source["dec_j2000_formula"],
        )
        if len(match) == 1:
            try:
                ingest_names(
                    db, match[0], MKO_name
                )  # ingest new names while were at it
                names_ingested += 1
            except AstroDBError as e:
                None  # only error is if there is a preexisting name anyways.

    for band in ["Y", "J", "H", "K"]:
        measurement = source[f"{band}_MKO"]
        error = source[f"{band}err_MKO"]
        band_filter = f"{filter_name}.{band}"
        if band_filter == "VISTA.K":
            band_filter = "VISTA.Ks"

        if isnan(measurement):  # skip if no data
            no_data += 1
            continue
        uc_sheet_name = source["name"]
        if match is None:
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
                references = source[f"ref_{band}_MKO"].split(";")
                reference = uc_ref_to_simple_ref(db, references[0])

                comment_reference = ""
                if len(references) > 1:
                    comment_reference = (
                        f"other references: {uc_ref_to_simple_ref(db, references[1])}"
                    )
                if len(comment_filter) == 0 and len(comment_reference) == 0:
                    comment = None
                else:
                    comment = comment_filter + comment_reference
                ingest_photometry(
                    db,
                    source=simple_source,
                    band=band_filter,
                    magnitude=measurement,
                    magnitude_error=error,
                    telescope=telescope,
                    # instrument=instrument,
                    comments=comment,
                    reference=reference,
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

logger.info(f"ingested:{ingested}")  # 6692
logger.info(f"already exists:{already_exists}")  # 801
logger.info(f"no sources:{no_sources}")  # 1403
logger.info(f"multiple sources:{multiple_sources}")  # 8
logger.info(f"bad references:{bad_reference}")  # 1244
logger.info(f"no data: {no_data}")  # 5412
logger.info(
    f"data points tracked:{ingested+already_exists+no_sources+multiple_sources+bad_reference}"
)  # 10148
total = (
    ingested + already_exists + no_sources + multiple_sources + no_data + bad_reference
)
logger.info(f"total: {total}")  # 15560
logger.info(f"names ingested: {names_ingested}")  # 155

if total != len(uc_sheet_table) * 4:
    msg = "data points tracked inconsistent with UC sheet"
    logger.error(msg)
    raise AstroDBError(msg)
elif DB_SAVE:
    db.save_database(directory="data/")
