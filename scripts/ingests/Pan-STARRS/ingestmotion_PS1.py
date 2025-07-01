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

# read Excel file
excel_path = "scripts/ingests/Pan-STARRS/Pan-STARRS Proper Motion.xlsx"
data = pd.read_excel(excel_path)


# CSV matching file
matched_csv = "scripts/ingests/Pan-STARRS/matched_sources.csv"
matched_df = pd.read_csv(matched_csv)

# CSV output paths
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

    # map the matched sources in CSV to proper motion source name
    matched_map = dict(zip(matched_df["original_source"], matched_df["matched_source"]))


    for _, row in chunk.iterrows():
        try:
            original_source = row["source"]

            # Check if the source is in the matched map, map the source name to the matched source name
            if original_source in matched_map:
                matched_source = matched_map[original_source]
            else:
                logger.warning(f"Source {original_source} not found in matched sources.")

            ingest_proper_motions(
                db,
                sources=[matched_source],
                pm_ras=[row["pmRA"]],
                pm_ra_errs=[row["e_pmRA"]],
                pm_decs=[row["pmDEC"]],
                pm_dec_errs=[row["e_pmDEC"]],
                pm_references="Best18"
            )
            motion_added += 1
            successful_pm.append(matched_source)
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



# Call ingest function
ingest_PanSTARRS_proper_motion(data,0,10)



# Save updated SQLite database
if save_db:
    db.save_database(directory="data/")
    logger.info("Proper Motion Database saved successfully.")