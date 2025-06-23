import logging
from astrodb_utils import load_astrodb, AstroDBError
from simple import REFERENCE_TABLES
from simple.utils.spectra import ingest_spectrum
from astrodb_utils.sources import ingest_source, find_source_in_db
from astropy.io import fits
from astropy.time import Time
import os
import csv

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

path = "/Users/guanying/SIMPLE Archieve/SIMPLE-db/scripts/ingests/Zhang18/sty2054_supplemental_files"

def ingest_zhang_spectra():
    for filename in os.listdir(path):
        if not filename.endswith(".fits"):
            continue

        # Get fits files from sty2054 directory
        file_path = os.path.join(path, filename)
        print(f"\nReading: {filename}")

        sourceName = "_".join(filename.split("_")[0:2])
        
        with fits.open(file_path) as hdul:
            header = hdul[0].header

            # Extract values from header
            instrument_ = header.get("INSTRUME", None)
            telescope_ = header.get("TELESCOP", None)
            obs_date_ = header.get("DATE-OBS", None)
            mode_ = header.get("INSMODE", None)
            if(instrument_ == "XSHOOTER"):
                instrument_ = "XShooter"
                telescope_ = "ESO VLT"
                mode_ = "Echelle"

            try:
                obs_date = Time(obs_date_).to_datetime()
            except Exception as e:
                logger.error(f"Error ingesting {filename}: {e}")
                obs_date = None

        spectra_added = 0
        skipped = 0
        inaccessible = 0

        try:
            ingest_spectrum(
                db=db,
                source=sourceName,
                spectrum=file_path,
                reference="Zhan18.1352",
                instrument=instrument_,
                mode=mode_,
                telescope=telescope_,
                obs_date=obs_date,
                regime="optical",
                format="tabular-fits"
            )
            logger.info(f"Successful ingesting {sourceName}")
            spectra_added += 1
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



