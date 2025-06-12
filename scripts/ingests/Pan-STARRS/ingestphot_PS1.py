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
save_db = True

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
    # easy for adding band counts in test_photometry.py
    ps1g_mag = ps1r_mag = ps1i_mag = ps1z_mag = ps1y_mag = 0
    skipped = 0
    inaccessbible = 0

    for _, row in data.iterrows():
        try:
            if(not pd.isna(row["gmag"])):
                ingest_photometry(
                    db,
                    source = row["source"],
                    band = "PS1.g",
                    magnitude = row["gmag"],
                    magnitude_error = row["e_gmag"],
                    reference = "Best18",
                    telescope = "Pan-STARRS",
                    epoch = None,
                    comments = None,
                    raise_error = True
                )
                photometry_added += 1
                ps1g_mag += 1

            if( not pd.isna(row["rmag"])):
                ingest_photometry(
                    db,
                    source = row["source"],
                    band = "PS1.r",
                    magnitude = row["rmag"],
                    magnitude_error = row["e_rmag"],
                    reference = "Best18",
                    telescope = "Pan-STARRS",
                    epoch = None,
                    comments = None,
                    raise_error = True
                )
                photometry_added += 1
                ps1r_mag += 1

            if (not pd.isna(row["imag"]) ):
                ingest_photometry(
                    db,
                    source = row["source"],
                    band = "PS1.i",
                    magnitude = row["imag"],
                    magnitude_error = row["e_imag"],
                    reference = "Best18",
                    telescope = "Pan-STARRS",
                    epoch = None,
                    comments = None,
                    raise_error = True
                )
                photometry_added += 1
                ps1i_mag += 1

            if (not pd.isna(row["zmag"])):
                ingest_photometry(
                    db,
                    source = row["source"],
                    band = "PS1.z",
                    magnitude = row["zmag"],
                    magnitude_error = row["e_zmag"],
                    reference = "Best18",
                    telescope = "Pan-STARRS",
                    epoch = None,
                    comments = None,
                    raise_error = True
                )
                photometry_added += 1
                ps1z_mag += 1

            if (not pd.isna(row["ymag"])):
                ingest_photometry(
                    db,
                    source = row["source"],
                    band = "PS1.y",
                    magnitude = row["ymag"],
                    magnitude_error = row["e_ymag"],
                    reference = "Best18",
                    telescope = "Pan-STARRS",
                    epoch = None,
                    comments = None,
                    raise_error = True
                )
                photometry_added += 1
                ps1y_mag += 1
                
            logger.info(f"Photometry added for {row['source']}: {photometry_added}")
        except AstroDBError as e:
            if "No unique source match" in str(e):
                skipped += 1
                logger.warning(f"Skipping {row['source']}: No unique match in database.")
            elif "does not appear to be accessible" in str(e):
                inaccessbible += 1
                logger.error(f"Inaccessible data for {row['source']}: {e}")
            else:
                logger.error(f"Unexpected error ingesting photometry for {row['source']}: {e}")
                raise
        # else:
        #     skipped += 1
        #     logger.warning(f"Skipping {row['source']} not found in current database.")

    logger.info(f"Total photometry added: {photometry_added}") 
    logger.info(f"Total photometry skipped: {skipped}") 
    logger.info(f"Total inaccessible data: {inaccessbible}")
    logger.info(f"Band counts for g: {ps1g_mag}, r: {ps1r_mag}, i: {ps1i_mag}, z: {ps1z_mag}, y: {ps1y_mag}")

#run ingestion
ingest_PanSTARRS_photometry(data=data)

# write to json file
if save_db:
    db.save_database(directory="data/")
    logger.info("Pan-STARRS Photometry Database saved as SIMPLE.sqlite")
