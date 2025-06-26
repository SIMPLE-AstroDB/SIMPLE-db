import logging
from astrodb_utils import load_astrodb, AstroDBError
from simple import REFERENCE_TABLES
from simple.utils.spectra import ingest_spectrum
from astropy.io import ascii
import sys

# Set up logger
astrodb_utils_logger = logging.getLogger("astrodb_utils")
astrodb_utils_logger.setLevel(logging.INFO)

# set up zhang logger
# Reference: Zhan18.1352
logger = logging.getLogger(
    "astrodb_utils.zhang18"
)
logger.setLevel(logging.INFO)

recreate_db = True 
save_db = False

# Load Database
db = load_astrodb(
    "SIMPLE.sqlite",
    recreatedb=recreate_db,
    reference_tables=REFERENCE_TABLES,
    felis_schema="simple/schema.yaml"
)

path = "scripts/ingests/Zhang18/zhang18_spectra.csv"
sys.path.append(".")

def ingest_zhang_spectra():
    spec_table = ascii.read(
        path,
        format="csv",
        data_start=1,
        header_start=0,
        guess=False,    
        fast_reader=False,
        delimiter=","
    )
    spectra_added = 0
    skipped = 0
    inaccessible = 0

    for row in spec_table:
        sourceName = row["source"]
        file_path = row["access_url"]
        logger.info(f"Processing {sourceName}")

        try:
            ingest_spectrum(
                db,
                source=sourceName,
                access_url=file_path,
                regime=row["regime"],
                telescope=row["telescope"],
                instrument=row["instrument"],
                mode=row["mode"],
                obs_date=row["observation_date"],
                comments=row["comments"],
                reference="Zhan18.1352"
            )
            spectra_added += 1
            logger.info(f"Successfully added spectrum for {sourceName}.")

        except AstroDBError as e:
            if "Spectrum already exists" in str(e):
                logger.warning(f"Skipping {sourceName}: Spectrum already exists.")
                skipped += 1
            else:
                inaccessible += 1
                logger.info(f"Inaccessible data: {inaccessible}")
                print(f"Inaccessible data for {sourceName}: {e}")
        except Exception as e:
            logger.warning(f"Skipping {sourceName} due to invalid URL path: {file_path}")
            skipped += 1
        
    logger.info(f"Total spectra added: {spectra_added}")
    logger.info(f"Total spectra skipped: {skipped}")
    logger.info(f"Total inaccessible spectra: {inaccessible}")

# call ingest function
ingest_zhang_spectra()

# save to SIMPLE database
if save_db: 
    db.save_database(directory="data/")



