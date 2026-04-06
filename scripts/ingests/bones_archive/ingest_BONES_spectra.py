import logging
import csv
from datetime import datetime
import pandas as pd
from pathlib import Path
from astropy.io import fits
from astrodb_utils.spectra import ingest_spectrum
from astrodb_utils.instruments import ingest_instrument
from astrodb_utils.publications import ingest_publication
from simple import REFERENCE_TABLES
from astrodb_utils import AstroDBError, load_astrodb

"""
This script ingest new created spectra FITS files into the SIMPLE database
"""

# Set the loggging level of the astrodb_utils logger
astrodb_utils_logger = logging.getLogger("astrodb_utils")
astrodb_utils_logger.setLevel(logging.INFO)

# Set up the logging for this ingest script.
logger = logging.getLogger("astrodb_utils.bones") 
logger.setLevel(logging.INFO)

# Load Database
SAVE_DB = False
RECREATE_DB = True
SCHEMA_PATH = "simple/schema.yaml"
db = load_astrodb(
    "SIMPLE.sqlite",
    recreatedb=RECREATE_DB,
    reference_tables=REFERENCE_TABLES,
    felis_schema=SCHEMA_PATH,
)

# Paths
spreadsheet_url = "https://docs.google.com/spreadsheets/d/e/2PACX-1vRtZ_Sl9hSi-JdIimRxbRLSTTYozlLOStmlzzcoAM7yB-duaMtzSqAIITI2ioMqlSIc6en8eiZDnUGe/pub?gid=0&single=true&output=csv"
data = pd.read_csv(spreadsheet_url)
fits_file = "/Users/guanying/SIMPLE db/SIMPLE-db/scripts/spectra_convert/BONES Archive/Processed BONES/"
SKIP_FILES = [
    "2MASS J12270506-0447207",
    "WISEA J135501.90−825838.9",
    "ULAS J124947.04+095019.8",
    "SDSS J133837.01-022908.4",
    "LHS 377",
    "2MASS J16262034+3925190",
    "2MASS J06453153−6646120",
    "SDSS J010448.46+153501.8",
    "ULAS J020858.62+020657.0",
    "ULAS J021642.96+004005.7",
    "ULAS J024035.36+060629.3",
    "ULAS J130710.22+151103.4",
    "ULAS J135058.86+081506.8",
    "2MASS J14120397+1216100",
    "ULAS J151913.03-000030.0",
    "ULAS J223302.03+062030.8",
    "ULAS J230711.01+014447.1",
]
def add_instruments():
    INSTRUMENT=[
        {
            "name": "BCSpec",
            "mode": "spectroscopy",
            "telescope": "LCO du Pont",
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
        },
        {
            "name":"OSIRIS",
            "mode":"Missing",
            "telescope":"SOAR",
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


def add_publications():
    try:
        ingest_publication(
            db,
            reference="Burg25.79",
            bibcode="2025ApJ...982...79B",
            doi="10.3847/1538-4357/adb39f",
            description="New Cold Subdwarf Discoveries from Backyard Worlds and a Metallicity Classification System for T Subdwarfs"
        )
    except AstroDBError as e:
        logger.error(f"Failed to ingest publication Burg25.79: {e}")

def modify_date(obs_date):
    if not obs_date:
        return None
    
    return datetime.fromisoformat(str(obs_date)) # example: 2004-04-17 04:40:11.761000

def add_access_url(filename):

    filename = filename.replace(".txt", "_SIMPLE.fits")
    filename = filename.replace(".csv", "_SIMPLE.fits")
    filename = filename.replace('+', '%2B')

    access_url = (
        "https://bdnyc.s3.us-east-1.amazonaws.com/bones/"
        + filename
    )

    return access_url

def ingest_spectra():
    added_files = 0
    failed_files = 0

    for _, row in data.iloc[1:83].iterrows():
        
        if row['NAME'] in SKIP_FILES:
            logger.info(f"Skipping {row['Filename']} due to known issues.")
            continue

        print(f"Processing {row['NAME']}...")

        filename = str(row['Filename'])
        access_url = add_access_url(filename)
        original_spectrum = filename # To be updated

        obs_date = modify_date(row['DATE-OBS'])

        try:
            ingest_spectrum(
                db,
                source=row['SIMPLE Name'],
                spectrum=access_url,
                # original_spectrum=original_spectrum,
                regime= row['Regime'],
                instrument=row['INSTRUME'],
                telescope=row['TELESCOP'],
                mode=row['mode'],
                obs_date=obs_date,
                reference=row['Reference'],
            )
            logger.info(f"Ingesting spectrum for {row['NAME']}...")
            print(f"Successfully ingest spectrum for {row['NAME']}")
            added_files += 1
        except AstroDBError as e:
            logger.error(f"Failed to ingest spectrum for {row['NAME']}: {e}")
            failed_files += 1
    print(f"Total spectra added: {added_files}")
    print(f"Total failed spectra: {failed_files}")

add_instruments()
add_publications()
ingest_spectra()

if SAVE_DB:
    db.save_database(directory="data/")
    logger.info("Database saved as SIMPLE.sqlite")