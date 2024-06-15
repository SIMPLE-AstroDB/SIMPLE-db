from astrodb_utils import (
    load_astrodb,
    find_source_in_db,
    AstroDBError,
    ingest_publication,
)
import logging
from astropy.io import ascii
from astropy.table import Table
from simple.schema import *
from simple.schema import REFERENCE_TABLES
from math import isnan
import sqlalchemy.exc

import sys

sys.path.append(".")


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


bad_sources = [
    "2MASS J02192210-3925225B",
    "beta Pic b",
]  # due to coordinate overlap or similar
no_sources = 0
multiple_sources = 0

# Ingest loop
for source in uc_sheet_table:
    if isnan(source["rv_lit"]):
        # skip if no rv
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
        if uc_sheet_name in bad_sources:
            continue
        logger.info(f"Match found for {uc_sheet_name}: {simple_source}")
        print(f"Match found for {uc_sheet_name}: {simple_source}")
        rv_data = {
            "source": simple_source,
            "radial_velocity_km_s": source["rv_lit"],
            "radial_velocity_error_km_s": source["rverr_lit"],
            "reference": uc_ref_to_simple_ref(source["ref_rv_lit"]),
        }
        try:
            rv_obj = RadialVelocities(**rv_data)
            with db.session as session:
                session.add(rv_obj)
                session.commit()
            logger.info(f" Radial Velocity added to database: {rv_data}\n")
        except sqlalchemy.exc.IntegrityError as e:
            msg = f"Could not add {rv_data} to database. Error: {e}"
            logger.warning(msg)
            raise AstroDBError(msg) from e
    elif len(match) == 0:
        no_sources += 1
    else:
        multiple_sources += 1

print(f"no sources:{no_sources}")  # skipped 244 due to 0 matches
print(f"multiple sources:{multiple_sources}")  # skipped 0 due to multiple matches
# db.save_database(directory="data/")
