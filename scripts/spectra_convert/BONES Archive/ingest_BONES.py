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
recreate_db = False
save_db = False

SCHEMA_PATH = "simple/schema.yaml"   
db = load_astrodb(
    "SIMPLE.sqlite",
    recreatedb=recreate_db,  
    reference_tables=REFERENCE_TABLES, 
    felis_schema=SCHEMA_PATH)

# Paths
spreadsheet_url =  "https://docs.google.com/spreadsheets/d/e/2PACX-1vS2sqaoYnG8g0d-wTgMjF3lXe40MF63B1wVodiAz2a4W2BCDnBvOBCQave8iiCbjj7-OQWpmqqQdpUA/pub?output=csv"
data = pd.read_csv(spreadsheet_url)

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
    url = "https://bdnyc.s3.us-east-1.amazonaws.com/BONES Archive/" 

    for _, row in data.iterrows():
        
        # skip the non-spectra rows
        if pd.isnull(row['SIMPLE Name']):
            continue

        filename = str(row['Filename'])
        fits_file = filename.replace('.txt', '.fits').replace('.csv', '.fits')
        access_url = url + fits_file

        obs_date = row['DATE-OBS']
        obs_date = datetime.strptime(obs_date + "T" + "000000", '%Y-%m-%dT%H:%M:%S.%f')

        try:
            ingest_spectrum(
                db,
                source=row['SIMPLE Name'],
                access_url=access_url,
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