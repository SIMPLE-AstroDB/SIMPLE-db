from astropy.io import fits
from astrodb_utils.spectra import ingest_spectrum
from astrodb_utils.instruments import ingest_instrument
from simple import REFERENCE_TABLES
from astrodb_utils import AstroDBError, load_astrodb
import logging
import csv
from datetime import datetime
import pandas as pd

# Set the loggging level of the astrodb_utils logger
astrodb_utils_logger = logging.getLogger("astrodb_utils")
astrodb_utils_logger.setLevel(logging.INFO)

# Set up the logging for this ingest script.
logger = logging.getLogger("astrodb_utils.bones") 
logger.setLevel(logging.INFO)

# Load Database
recreate_db = True
save_db = True

SCHEMA_PATH = "simple/schema.yaml"   
db = load_astrodb(
    "SIMPLE.sqlite",
    recreatedb=recreate_db,  
    reference_tables=REFERENCE_TABLES, 
    felis_schema=SCHEMA_PATH)

# Paths
csv_path = "/Users/guanying/SIMPLE_Archive/SIMPLE-db/scripts/spectra_convert/BONES Archive/BONES_Archive.csv"
data = pd.read_csv(csv_path)

def add_instruments():
    INSTRUMENT=[
        {
            "name": "BCSpec",
            "mode": "spectroscopy",
            "telescope": "LCO duPont",
        },
        {
            "name": "DSpec",
            "mode":"spectroscopy",
            "telescope": "Palomar Hale",
        },
        {
            "name":"MOSFIRE",
            "mode":"Imaging",
            "telescope":"Keck I",
        }
    ]
    for inst in INSTRUMENT:
        try:
            ingest_instrument(
                db,
                instrument=inst["name"],
                mode=inst["mode"],
                telescope=inst["telescope"]
            )
        except AstroDBError as e:
            logger.error(f"Failed to ingest instrument {inst['name']}: {e}")

def ingest_spectra():
    added_files = 0
    failed_files = 0

    for _, row in data.iterrows():
        obs_date = row['DATE-OBS']
        obs_date = datetime.strptime(obs_date, '%Y-%m-%d').replace(microsecond=0)

        try:
            ingest_spectrum(
                db,
                source=row['OBJECT'],
                access_url="",
                regime= row['Regime'],
                instrument=row['INSTRUME'],
                telescope=row['TELESCOP'],
                mode=row['mode'],
                obs_date=obs_date,
                reference=row['Reference'],
            )
            logger.info(f"Ingesting spectrum for {row['OBJECT']}...")
            print(f"Successfully ingest spectrum for {row['OBJECT']}")
            added_files += 1
        except AstroDBError as e:
            logger.error(f"Failed to ingest spectrum for {row['OBJECT']}: {e}")
            failed_files += 1
    print(f"Total spectra added: {added_files}")
    print(f"Total failed spectra: {failed_files}")

add_instruments()
# ingest_spectra()

if save_db:
    db.save_database(directory="data/")
    logger.info("Database saved as SIMPLE.sqlite")