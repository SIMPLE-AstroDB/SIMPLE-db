import logging
import os
import pandas as pd
from pathlib import Path
from astropy.io import fits
from astrodb_utils import AstroDBError, load_astrodb
from astrodb_utils.spectra import ingest_spectrum
from astrodb_utils.instruments import ingest_instrument
from astrodb_utils.sources import find_source_in_db, ingest_source
from astroquery.simbad import Simbad
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

# Add new mode for SDSS and ESO VLT Telescope
def add_mode():
    modes = [
        {
            "telescope": "ESO VLT", # existed
            "instrument": "Xshooter", # existed
            "mode": "Echelle-Smoothed", # new
        },
        {
            "telescope": "Magellan I Baade", # existed
            "instrument": "IMACS", # new
            "mode": "Missing"

        },
        {
            "telescope": "SDSS", # new
            "instrument": "BOSS", # new
            "mode": "Missing"
        }
    ]
    for mode in modes:
        try:
            ingest_instrument(
                db, 
                telescope=mode["telescope"],
                instrument=mode["instrument"],
                mode=mode["mode"]
            )
            print(f"Mode {mode['instrument']} added successfully!")
        except AstroDBError as e:
            astrodb_logger.error(f"Error adding {mode['instrument']} mode: {e}")


def modify_date(obs_date):
    if not obs_date:
        return None
    
    return datetime.fromisoformat(obs_date) # example: 2004-04-17 04:40:11.761000
 
def add_url(filename):
    modified_filename = filename.replace('+', '%2B')
    return "https://bdnyc.s3.us-east-1.amazonaws.com/zhang18/" + modified_filename


# Ingest spectra --
def ingest_zhang18():
    source_ingested = 0
    source_failed = 0
    spectrum_ingested = 0
    spectrum_failed = 0
    failed_file = []

    fits_file = list(Path(file_path).glob("*.fits"))

    for file in fits_file:
        filename = file.name
        print(f"Reading {filename}")

        with fits.open(file) as hdul:
            header = hdul[0].header
            
        # Get all neccessary info to ingest
        source_name = header.get("OBJECT")
        access_url = add_url(filename)
            
        # Date-OBS
        obs_date = modify_date(header.get("DATE-OBS"))

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

        # regime 
        if "NIR" in filename:
            regime = "nir"
        else:
            regime = "optical"
        
        # mode
        if "SMOOTHED" in filename:
            mode = "Echelle-Smoothed"
        elif "Xshooter" in filename and not "SMOOTHED" in filename:
            mode = "Echelle"
        elif "FIRE" in filename:
            mode = "Prism"
        else:
            mode = "Missing"
        
        # instrument
        if "IMAC" in filename:
            instrument = "IMACS"
            telescope = "Magellan I Baade"
        else:
            instrument = header.get("INSTRUME")
            telescope = header.get("TELESCOP") 

        # --- Ingest Source if not exist in DB ---
        result = Simbad.query_object(source_name)
        ra = result[0]["ra"]
        dec = result[0]["dec"]
        source = find_source_in_db(
            db, 
            source_name,
            ra_col_name = "ra",
            dec_col_name = "dec",
            ra = ra,
            dec =dec,
            use_simbad=True
        )
        if source is None:
            logger.info(f"Source {source_name} is not found in SIMPLE database")
            continue

        if not source:
            try:
                ingest_source(
                    db,
                    source_name,
                    reference,
                    ra_col_name="ra",
                    dec_col_name="dec",
                    ra=ra,
                    dec=dec,
                    epoch_col_name="epoch",
                    use_simbad=True
                )
                print(f"Source {source_name} ingested successfully!")
                source_ingested += 1
            except AstroDBError as e:
                logger.error(f"Error ingesting source {source_name}: {e}")
                source_failed += 1
                failed_file.append(filename)
                continue

        # iNgest Spectra
        try:
            print(f"Ingesting spectrum for {source_name} with access URL: {access_url}\n regime: {regime}, mode: {mode}, telescope: {telescope}, instrument: {instrument}, obs_date: {obs_date}, reference: {reference}\n")
            logger.debug(f"Found db_name: {source} for source: {source_name}\n")
            ingest_spectrum(
                db,
                source=source_name,
                spectrum=access_url,
                regime=regime,
                mode=mode,
                telescope=telescope,
                instrument=instrument,
                obs_date=obs_date,
                reference=reference
            ) 
            print("Successfully ingested spectrum\n")
            spectrum_ingested += 1
        except AstroDBError as e:
            logger.error(f"Error ingesting spectrum: {e}")
            spectrum_failed += 1
            failed_file.append(filename)
                

    print(f"\nIngestion complete! \nTotal sources ingested: {source_ingested}")
    print(f"Total sources failed to ingest: {source_failed}")

    print(f"\nTotal spectra ingested: {spectrum_ingested}")
    print(f"Total spectra failed to ingest: {spectrum_failed}")
    print(f"Failed files: {failed_file}")


add_mode()
ingest_zhang18()

if SAVE_DB:
    db.save_db(directory="data/")

