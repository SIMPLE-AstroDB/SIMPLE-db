import logging
import os
from pathlib import Path
from astropy.io import fits
from astrodb_utils import AstroDBError, load_astrodb
from astrodb_utils.spectra import ingest_spectrum
from astrodb_utils.instruments import ingest_instrument
from simple import REFERENCE_TABLES
from specutils import Spectrum
from datetime import datetime


# set up logging 
astrodb_logger = logging.getLogger("astrodb_utils")
astrodb_logger.setLevel(logging.INFO)

# logger for ingest_zhang18
logger = logging.getLogger("ingest_zhang18")
logger.setLevel(logging.INFO)

# Load DB
SAVE_DB = False
RECREATE_DB = True
SCHEMA_PATH = "simple/schema.yaml"
db = load_astrodb(
    "SIMPLE.sqlite",
    recreatedb=RECREATE_DB,
    reference_tables=REFERENCE_TABLES,
    felis_schema=SCHEMA_PATH,
)

# Path
file_path = "scripts/spectra_convert/zhang18/processed_spectra"

# Add new mode for Xshooter smoothed spectra
def add_mode():
    """
        Telescope: ESO VLT (existed)
        Instrument: XShooter (existed)
        mode: Echelle-smoothed (new mode for smoothed spectrum)
    """
    try:
        ingest_instrument(
            db,
            telescope="ESO VLT",
            instrument="Xshooter",
            mode="Echelle-Smoothed",
            raise_error=True
        )
        print("mode added successfully! ")
    except AstroDBError as e:
        astrodb_logger.error(f"Error adding mode: {e}")

def modify_date(obs_date):
    if not obs_date:
        return None
    
    return datetime.fromisoformat(obs_date) # example: 2004-04-17 04:40:11.761000
 

# Ingest spectra --
def ingest_zhang18():
    ingested = 0
    failed = 0
    failed_file = []

    fits_file = list(Path(file_path).glob("*.fits"))

    for file in fits_file:
        filename = file.name
        print(f"Reading {filename}")
        spectrum = Spectrum.read(file)

        with fits.open(file) as hdul:
            header = hdul[0].header
            
        # Get all neccessary info to ingest
        source_name = header.get("OBJECT")
        access_url = filename
        
        # Regime
        if "NIR" in filename:
            regime = "nir"
        else:
            regime = "optical"

        # Mode
        if "SMOOTHED" in filename:
            mode = "Echelle-Smoothed"
        elif "Xshooter" in filename and not "SMOOTHED" in filename:
            mode = "Echelle"
        else:
            mode = "Missing"
        
        # publication
        ref = header.get("VOREF")
        if ref == "10.1093/mnras/stw2438": # I
            reference = "Zhan17.3040"
        elif ref == "10.1093/mnras/stx350": # II
            reference = "Zhan17"
        elif ref == "10.1093/mnras/sty1352": # III
            reference  = "Zhan18.1352"
        elif ref == "10.1093/mnras/sty2054": # IV
            reference = "Zhan18.2054"

        # Date-OBS
        obs_date = modify_date(header.get("DATE-OBS"))

        # iNgest
        try:
            ingest_spectrum(
                db,
                source=source_name,
                spectrum=access_url,
                regime=regime,
                mode=mode,
                telescope=header.get("TELESCOP"),
                instrument=header.get("INSTRUME"),
                obs_date=obs_date,
                reference=reference
            ) 
            logger.info(f"Successfully ingested spectrum")
            ingested += 1
        except AstroDBError as e:
            logger.error(f"Error ingesting spectrum: {e}")
            failed +=1
            failed_file.append(filename)

add_mode()
ingest_zhang18()

if SAVE_DB:
    db.save_db(directory="data/")

