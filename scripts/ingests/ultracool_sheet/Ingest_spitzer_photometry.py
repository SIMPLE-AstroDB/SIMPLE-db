from astrodb_utils import (
    load_astrodb,
    find_source_in_db,
    AstroDBError,
    ingest_publication,
)
import sys

sys.path.append(".")
import logging
from astropy.io import ascii
from astropy.table import Table
from simple.schema import *
from simple.schema import REFERENCE_TABLES
from math import isnan
import sqlalchemy.exc


logger = logging.getLogger("AstroDB")
logger.setLevel(logging.INFO)


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

# Load Ultracool sheet refrences
sheet_id = "453417780"
link = (
    f"https://docs.google.com/spreadsheets/d/{doc_id}/export?format=csv&gid={sheet_id}"
)

# read the csv data into an astropy table
uc_reference_table = ascii.read(
    link,
    format="csv",
    data_start=1,
    header_start=0,
    guess=False,
    fast_reader=False,
    delimiter=",",
)

uc_ref_to_ADS = {}
for ref in uc_reference_table:
    uc_ref_to_ADS[ref["code_ref"]] = ref["ADSkey_ref"]


# used to translate between uc references and SIMPLE references
def uc_ref_to_simple_ref(ref):
    t = (
        db.query(db.Publications)
        .filter(db.Publications.c.bibcode == uc_ref_to_ADS[ref])
        .astropy()
    )
    if len(t) == 0:
        ingest_publication(db, bibcode=uc_ref_to_ADS[ref], reference=ref)
        return ref
    else:
        return t["reference"][0]


bad_sources = []  # due to coordinate overlap or similar
bad_references = ["Kirk11;Legg13", "Luhm10;Mart22", "Dupu13b;Legg13"]

no_sources = 0
multiple_sources = 0
ingested = 0
already_exists = 0
bad_reference = 0

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
        print(f"Match found for {uc_sheet_name}: {simple_source}")
        for val in [["IRAC.I1", "ch1", "ch1err"], ["IRAC.I2", "ch2", "ch2err"]]:
            band, magnitude, mag_error = val
            if isnan(source[magnitude]):
                continue
            # if source["ref_Spitzer"] in bad_references:
            #    bad_reference += 1
            #    continue

            # ingest for each reference provided
            for reference in source["ref_Spitzer"].split(";"):
                table_data = {
                    "source": simple_source,
                    "band": band,
                    "magnitude": source[magnitude],
                    "magnitude_error": source[mag_error],
                    "telescope": "Spitzer",
                    "reference": uc_ref_to_simple_ref(reference),
                }
                try:
                    rv_obj = Photometry(**table_data)
                    with db.session as session:
                        session.add(rv_obj)
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
print(f"ingested:{ingested}")  # 905 ingested
print(f"bad ref:{bad_reference}")  # 0 with bad ref
print(f"already exists:{already_exists}")  # skipped 465 due to preexisting data
print(f"no sources:{no_sources}")  # skipped 129 due to 0 matches
print(f"multiple sources:{multiple_sources}")  # skipped 0 due to multiple matches
print(
    f"total:{ingested+bad_reference+already_exists+no_sources+multiple_sources}"
)  # 1499
# db.save_database(directory="data/")
