from astrodb_utils import load_astrodb, AstroDBError
from simple import REFERENCE_TABLES
from simple.utils.astrometry import ingest_proper_motions
import pandas as pd
import sqlalchemy
import logging
import csv

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

csv_output1 = "scripts/ingests/Pan-STARRS/valid_proper_motion.csv"
csv_output2 = "scripts/ingests/Pan-STARRS/invalid_proper_motion.csv"

# utilize the ingest_proper_motions from SIMPLE
# Process data in chunks
def ingest_PanSTARRS_proper_motion(data, start_idx=0, chunk_size=0):
    motion_added = 0
    skipped = 0
    inaccessible = 0
    successful_pm = []
    failed_pm = []

    end_idx = min(start_idx + chunk_size, len(data))
    chunk = data.iloc[start_idx:end_idx]


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
            successful_pm.append(row["source"])
            logger.info(f"Proper motion added for {row['source']}")
        except Exception as e:
            if "No unique source match" in str(e):
                skipped += 1
                failed_pm.append((row["source"], f"No unique match in database for {row['source']}"))
                logger.warning(f"Skipping {row['source']}: No unique match in database.")
            else:
                inaccessible += 1
                failed_pm.append((row["source"], str(e)))
                logger.error(f"Unexpected error for {row['source']}: {e}")


    # Valid proper motion data
    with open(csv_output1, "w", newline="") as f:
        fieldnames = [
            "source", "pmRA", "e_pmRA", "pmDEC", "e_pmDEC", "reference"
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows([
            {
                "source": row["source"],
                "pmRA": row["pmRA"],
                "e_pmRA": row["e_pmRA"],
                "pmDEC": row["pmDEC"],
                "e_pmDEC": row["e_pmDEC"],
                "reference": "Best18"
            } for _, row in chunk.iterrows()
        ])

    # Invalid proper motion data
    with open(csv_output2, "w", newline="") as f:
        fieldnames = ["source", "reason"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for source, reason in failed_pm:
            writer.writerow({"source": source, "reason": reason})

    logger.info(f" Total Proper Motion Added: {motion_added}")
    logger.info(f" Skipped count: {skipped}")
    logger.info(f" Inaccessible data: {inaccessible}")


    """ Log output:
        INFO     - astrodb_utils.proper_motion -  Total Proper Motion Added: 1966
        INFO     - astrodb_utils.proper_motion -  Skipped count: 497
        INFO     - astrodb_utils.proper_motion -  Inaccessible data: 7425
    """

# # after review function: ingest into database from valid_proper_motion.csv to reduce time
def ingest_into_database(csv_path):
    with open(csv_path, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                ingest_proper_motions(
                    db,
                    sources=[row["source"]],
                    pm_ras=[row["pmRA"]],
                    pm_ra_errs=[row["e_pmRA"]],
                    pm_decs=[row["pmDEC"]],
                    pm_dec_errs=[row["e_pmDEC"]],
                    pm_references=row["reference"]
                )
            except AstroDBError as e:
                logger.error(f"Error ingesting proper motion for {row['source']}: {e}")


# Call ingest function
ingest_PanSTARRS_proper_motion(data,0,10000)

#ingest_into_database("scripts/ingests/Pan-STARRS/valid_proper_motion.csv") 



# Save updated SQLite database
if save_db:
    db.save_database(directory="data/")
    logger.info("Proper Motion Database saved successfully.")