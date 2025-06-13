import logging
from astrodb_utils import load_astrodb, AstroDBError
from astrodb_utils.sources import find_source_in_db
from astrodb_utils.photometry import ingest_photometry
from simple import REFERENCE_TABLES
import pandas as pd

# Set the logging level of the astrodb_utils logger
astrodb_utils_logger = logging.getLogger("astrodb_utils")
astrodb_utils_logger.setLevel(logging.INFO)

# Set up the logging for pan_starrs.
logger = logging.getLogger("astrodb_utils.pan_starrs")
logger.setLevel(logging.INFO) 

# load database
recreate_db = True
save_db = False

SCHEMA_PATH = "simple/schema.yaml"
db = load_astrodb(
    "SIMPLE.sqlite",
    recreatedb = recreate_db,
    reference_tables = REFERENCE_TABLES,
    felis_schema = SCHEMA_PATH
)

excel_path = "scripts/ingests/Pan-STARRS/Optical Pan-STARRS Photometry.xlsx"
data = pd.read_excel(excel_path)

# Ingest Photometry: Add different bands depends on the ugriz magnitude available in the datasets
def ingest_PanSTARRS_photometry(data):
    photometry_added = 0
    skipped = 0
    inaccessible = 0
     # easy for adding band counts in test_photometry.py
    band_counts = {band: 0 for band in ['g', 'r', 'i', 'z', 'y']}

    for _, row in data.iterrows():
        try:
            for band in ['g', 'r', 'i', 'z', 'y']:
                mag = f"{band}mag"
                err = f"e_{band}mag"
                if not pd.isna(row[f"{band}mag"]):
                    ingest_photometry(
                        db,
                        source=row["source"],
                        band=f"PS1.{band}",
                        magnitude=row[mag],
                        magnitude_error=row.get(err),
                        reference="Best18",
                        telescope="Pan-STARRS",
                        epoch=None,
                        comments=None,
                        raise_error=True
                    )
                    photometry_added += 1
                    band_counts[band] += 1
            logger.info(f"Photometry added for {row['source']}")
        except AstroDBError as e:
            if "No unique source match" in str(e):
                skipped += 1
                logger.warning(f"Skipping {row['source']}:no match in database.")
            elif "does not appear to be accessible" in str(e):
                inaccessible += 1
                logger.error(f"Inaccessible data for {row['source']}: {e}")
            else:
                logger.error(f"Unexpected error ingesting photometry for {row['source']}: {e}")

    logger.info(f"Total photometry added: {photometry_added}") 
    logger.info(f"Total photometry skipped: {skipped}") 
    logger.info(f"Total inaccessible data: {inaccessible}")
    for band, count in band_counts.items():
        logger.info(f"Total entries ingested for PS1.{band} band: {count}")

# Run ingestion
ingest_PanSTARRS_photometry(data=data)

# Save updated SQLite database
if save_db:
    db.save_database(directory="data/")
    logger.info("Pan-STARRS Photometry Database saved as SIMPLE.sqlite")
