import logging
from astrodb_utils import load_astrodb, AstroDBError
from astrodb_utils.sources import ingest_name
from simple import REFERENCE_TABLES
import pandas as pd
import csv

# Set the logging level of the astrodb_utils logger
astrodb_utils_logger = logging.getLogger("astrodb_utils")
astrodb_utils_logger.setLevel(logging.INFO)

# Set up the logging for pan_starrs.
panlogger = logging.getLogger("astrodb_utils.pan_starrs")
panlogger.setLevel(logging.INFO)

# Load Database
recreate_db = True
save_db = True

SCHEMA_PATH = "simple/schema.yaml"
db = load_astrodb(
    "SIMPLE.sqlite",
    recreatedb=recreate_db,
    reference_tables=REFERENCE_TABLES,
    felis_schema=SCHEMA_PATH
)

"""
This script is to ingest the source name from Pan-STARRS dataset that is matched with the source name in current SIMPLE db.
"""

# CSV matching file
matched_csv = "scripts/ingests/Pan-STARRS/matched_sources.csv"
matched_df = pd.read_csv(matched_csv)

def ingest_source_name():
    name_added = 0
    inaccessible = 0

    with open(matched_csv, "r") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            original_source = row["original_source"]
            matched_source = row["matched_source"]

            try:
                # Ingest the source name from matched source name in SIMPLE, skip if the source is already in SIMPLE
                if (original_source == matched_source):
                    panlogger.info(f"The source name is already in current database, skipping.")
                    continue
                ingest_name(
                    db,
                    source=matched_source,
                    other_name=original_source
                )
                name_added += 1

            except AstroDBError as e:
                panlogger.error(f"Error ingesting name for {original_source}: {e}")
                inaccessible += 1

    panlogger.info(f"Total names added: {name_added}")
    panlogger.info(f"Total inaccessible names: {inaccessible}")


# call ingestion function
ingest_source_name()

# Save updated SQLite database
if save_db:
    db.save_database(directory="data/")
    panlogger.info("Proper Motion Database saved successfully.")

