from astrodb_utils import load_astrodb, AstroDBError
from simple import REFERENCE_TABLES
from astrodb_utils.sources import find_source_in_db
from simple.utils.astrometry import ingest_proper_motions
import pandas as pd
import logging

# Set up astrodb_utils logger
astrodb_utils_logger = logging.getLogger("astrodb_utils")
astrodb_utils_logger.setLevel(logging.INFO)

# set up proper motion logger
logger = logging.getLogger(
    "astrodb_utils.proper_motion"
)
logger.setLevel(logging.INFO)

# set up logger for SIMPLE
simple_logger = logging.getLogger("SIMPLE")
simple_logger.setLevel(logging.INFO)

# Load Database
recreate_db = True
save_db = False

SCHEMA_PATH = "simple/schema.yaml"
db = load_astrodb(
    "SIMPLE.sqlite",
    recreatedb=recreate_db,
    reference_tables=REFERENCE_TABLES, 
    felis_schema=SCHEMA_PATH
)

# Read Excel file
excel_path = "scripts/ingests/Pan-STARRS/Pan-STARRS Proper Motion.xlsx"

data = pd.read_excel(excel_path)

# utilize the ingest_proper_motions from SIMPLE
# Process data in chunks
def ingest_PanSTARRS_proper_motion(data, start_idx=0, chunk_size=0):
    motion_added = 0
    skipped = 0
    inaccessible = 0

    end_idx = min(start_idx + chunk_size, len(data))
    chunk = data.iloc[start_idx:end_idx]

    print(f"\nProcessing rows {start_idx} to {end_idx - 1}\n")

    for _, row in chunk.iterrows():
        try:
            ingest_proper_motions(
                db,
                sources=[row["source"]],
                pm_ras=[row["pmRA"]],
                pm_ra_errs=[row["e_pmRA"]],
                pm_decs=[row["pmDEC"]],
                pm_dec_errs=[row["e_pmDEC"]],
                pm_references="Best18"
            )
            motion_added += 1
            logger.info(f"Proper motion added for {row['source']}")
        except Exception as e:
            if "No unique source match" in str(e):
                skipped += 1
                logger.warning(f"Skipping {row['source']}: No unique match in database.")
            else:
                inaccessible += 1
                logger.error(f"Unexpected error for {row['source']}: {e}")

    logger.info(f"Processing rows ({start_idx} -{end_idx - 1}):")
    logger.info(f" Added: {motion_added}")
    logger.info(f" Skipped: {skipped}")
    logger.info(f" Errors: {inaccessible}")

# Call ingest function
ingest_PanSTARRS_proper_motion(data,0,100)

# Save updated SQLite database
if save_db:
    db.save_database(directory="data/")
    logger.info("Proper Motion Database saved successfully.")