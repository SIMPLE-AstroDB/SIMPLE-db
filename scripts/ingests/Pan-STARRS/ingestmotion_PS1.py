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
recreate_db = False
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

def ingest_PanSTARRS_proper_motion(data):
    motion_added = 0
    skipped = 0
    inaccessible = 0

    # only get the valid sources that already pass through find_source_in_db
    # map the matched sources in CSV to proper motion source name
    matched_map = dict(zip(matched_df["original_source"], matched_df["matched_source"]))

    with open(csv_output1, "w", newline='') as valid_f, \
            open(csv_output2, "w", newline='') as invalid_f:
        
        valid_writer = csv.writer(valid_f)
        valid_writer.writerow(["source", "pmRA", "e_pmRA", "pmDEC", "e_pmDEC"])

        invalid_writer = csv.writer(invalid_f)
        invalid_writer.writerow(["source", "reason"])

        for row in data.itertuples():
            try:
                original_source = row.source

                # Check if the source is in the matched map, map the source name to the matched source name
                if original_source in matched_map:
                    matched_source = matched_map[original_source]
                else:
                    logger.warning(f"Source {original_source} not found in matched sources.")
                    skipped += 1
                    continue

                # skip if ra/deg is null
                if pd.isna(row.pmRA) or pd.isna(row.pmDEC):
                    logger.warning(f"Skipping {row.source}: No proper motion data.")
                    invalid_writer.writerow([row.source, "No proper motion data"])
                    skipped += 1
                    continue

                ingest_proper_motions(
                        db,
                        sources=[matched_source],
                        pm_ras=row.pmRA,
                        pm_ra_errs=row.e_pmRA,
                        pm_decs=row.pmDEC,
                        pm_dec_errs=row.e_pmDEC,
                        pm_references="Best18"
                )
                logger.info(f"Proper motion measurement added: {matched_source} ")
                valid_writer.writerow([matched_source, row.pmRA, row.e_pmRA, row.pmDEC, row.e_pmDEC])
                
                motion_added += 1

            except AstroDBError as e:
                inaccessible += 1
                logger.error(f"Unexpected error for {row.source}: {e}")
                invalid_writer.writerow([row.source, str(e)])
                
        
    logger.info(f"Total proper motion measurements added: {motion_added}")
    logger.info(f"Total skipped sources: {skipped}")
    logger.info(f"Total inaccessible sources: {inaccessible}")

    """ log output:
    INFO     - astrodb_utils.proper_motion - Total proper motion measurements added: 1966
    INFO     - astrodb_utils.proper_motion - Total skipped sources: 7922
    """

# Call ingest function
ingest_PanSTARRS_proper_motion(data)


# Save updated SQLite database
if save_db:
    db.save_database(directory="data/")
    logger.info("Proper Motion Database saved successfully.")

