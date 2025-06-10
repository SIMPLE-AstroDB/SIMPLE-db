import logging

# SIMPLE & Astrodb Packages
from astrodb_utils import load_astrodb, AstroDBError
from simple import REFERENCE_TABLES
from astrodb_utils.photometry import ingest_photometry, ingest_photometry_filter
import pandas as pd

# Set the loggging level of the astrodb_utils logger
astrodb_utils_logger = logging.getLogger("astrodb_utils")
astrodb_utils_logger.setLevel(logging.INFO) 

# Set up the logging for this ingest script.
logger = logging.getLogger(
    "astrodb_utils.beiler24"
) 

logger.setLevel(logging.INFO)

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

# Read the Excel file
excel_path = "scripts/ingests/beiler24/BeilerSIMPLE.Photometry.Ingest.1.xlsx"
data = pd.read_excel(excel_path)

# Ingest Photometry
def ingest_photo(data):
    photometry_added = 0
    skipped = 0
    inaccessible = 0

    for _, row in data.iterrows():
        print(row["source"])
        try:
            ingest_photometry(
                db,
                source=row["source"],
                band=row["band"],
                magnitude=row["magnitude"],
                magnitude_error=row["magnitude_error"],
                reference="Beil24",
                telescope=row["telescope"],
                epoch=row["epoch"],
                comments = None,
                raise_error=True
            )
            photometry_added += 1
            logger.info(f"Photometry added: {photometry_added}" )

        except AstroDBError as e:
            if "already exists" in str(e):
                skipped += 1
                logger.info(f"Photometry skipped: {skipped}")
                print(f"Skipping {row['source']}: Photometry already exists.")
            else:
                inaccessible += 1
                logger.info(f"Inaccessible data: {inaccessible}")
                print(f"Inaccessible data for {row['source']}: {e}")
    
    logger.info(f"Total photometry added: {photometry_added}")
    logger.info(f"Total photometry skipped: {skipped}")
    logger.info(f"Total inaccessible data: {inaccessible}")
    logger.info(f"Total Photometry: {photometry_added + skipped + inaccessible}/ {len(data)}")

# Call function
ingest_photo(data=data)

if save_db:
    db.save_database(directory="data/")
    logger.info("Database saved as SIMPLE.sqlite")




