from astrodb_utils import load_astrodb, find_source_in_db, AstroDBError
import logging
from astropy.io import ascii
from urllib.parse import quote
import requests
from astropy.table import Table
import sys
sys.path.append( '.' )
from simple.schema import *
from simple.schema import REFERENCE_TABLES
import sqlalchemy.exc



logger = logging.getLogger("AstroDB")
logger.setLevel(logging.INFO)


RECREATE_DB = False
db = load_astrodb("SIMPLE.sqlite", recreatedb=RECREATE_DB, reference_tables=REFERENCE_TABLES)

# Load Ultracool sheet
#sheet_id = "1i98ft8g5mzPp2DNno0kcz4B9nzMxdpyz5UquAVhz-U8"
#link = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv"
# link = "scripts/ultracool_sheet/UltracoolSheet - Main_010824.csv"
link = "scripts/ingests/ultracool_sheet/Radial_velocities_with_references.csv"

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

# Match sources in Ultracool sheet to sources in SIMPLE
for source in uc_sheet_table:
    uc_sheet_name = source["name"]
    match = find_source_in_db(
        db,
        uc_sheet_name,
        ra=source["ra_j2000_formula"],
        dec=source["dec_j2000_formula"],
    )

    if len(match) == 1:
        simple_source = match[0]
        logger.info(f"Match found for {uc_sheet_name}: {simple_source}")
        print(f"Match found for {uc_sheet_name}: {simple_source}")
        rv_data = [{"source": simple_source, "radial_velocity_km_s": source["rv_lit"], "reference": source["ref_rv_lit"]}]
        try:
            with db.engine.connect() as conn:
                conn.execute(db.RadialVelocities.insert().values(rv_data))
                conn.commit()
            logger.info(f" Radial Velocity added to database: {rv_data}\n")
        except sqlalchemy.exc.IntegrityError as e:
            msg = f"Could not add {rv_data} to database."
            logger.warning(msg)
            #raise AstroDBError(msg) from e

db.save_database(directory="data/")
        