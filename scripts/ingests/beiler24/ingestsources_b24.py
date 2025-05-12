## Ingest Sources for Beiler24 ----
import logging

# SIMPLE & Astrodb Packages
from astrodb_utils import load_astrodb, ingest_instrument, AstroDBError
from astrodb_utils.sources import ingest_source
from astrodb_utils.publications import ingest_publication
from simple import REFERENCE_TABLES
from simple.utils.spectra import ingest_spectrum
import os
import openpyxl
import pandas as pd

# Set the loggging level of the astrodb_utils logger
astrodb_utils_logger = logging.getLogger("astrodb_utils")
astrodb_utils_logger.setLevel(logging.INFO)

# Set up the logging for this ingest script.
logger = logging.getLogger(
    "astrodb_utils.beiler24"
)  # Sets up a child of the "astrodb_utils" logger
logger.setLevel(logging.INFO)  # Set logger to INFO level - less verbose

# Load Database
recreate_db = True
save_db = True

SCHEMA_PATH = "simple/schema.yaml"   
db = load_astrodb(
    "SIMPLE.sqlite",
    recreatedb=recreate_db,  
    reference_tables=REFERENCE_TABLES, 
    felis_schema=SCHEMA_PATH)

# Read the Excel file
# path = "/Users/carolina/Documents/AMNH/SIMPLE/SIMPLE-db/scripts/ingests/beiler24/"
# excel_path = os.path.join(path, "Beiler_SIMPLE_Ingest.xlsx")
excel_path = "scripts/ingests/beiler24/Beiler_SIMPLE_Ingest.xlsx"
data = pd.read_excel(excel_path)


# # Ingest Sources ----
def add_sources():
    # # Ingest Sources ----
    sources_added = 0
    for _, row in data.iterrows():
        try:
            ingest_source(
                db,
                source=row["source"],
                reference="Beil24",
            )
            print(f"Source {row['source']} ingested.")
            sources_added = +1

        except Exception as e:
            print(f"Error ingesting source {row['source']}: {e}")
            continue

    print(f"Total sources added: {sources_added}")# for _, row in data.iterrows():


# Ingest Instruments ----
def add_instruments():
    """Add intrument nirspec mode clear/prism"""
    modes = ["FS-CLEAR/PRISM", "FS-G395H/F290LP"]
    for mode in modes:
        try:
            ingest_instrument(
                db,
                instrument="NIRSpec",
                mode=mode,
                telescope="JWST",
            )
            print(f"Instruments NIRSpec {mode} ingested.")
            
        except Exception as e:
            print(f"Error ingesting instrument NIRSpec {mode}: {e}")
            continue

# Ingest Spectra ----
""" Note: Any source containing '+' has been replaced with '%2B' in the AWS URL"""
def ingest_spectra(data):
    spectra_added = 0
    skipped = 0

    url = "https://bdnyc.s3.us-east-1.amazonaws.com/Beiler24/"

    for _, row in data.iterrows():
        try:
            source_name = row["source"]
            spectrum_file = row["spectrum permalink"]
            if "+" in spectrum_file:
                spectrum_file = spectrum_file.replace("+", "%2B")
            spectrum_url = url + spectrum_file

            ingest_spectrum(
                db,
                source=source_name,
                spectrum=spectrum_url,
                regime=row["regime"],
                telescope=row["telescope"],
                instrument=row["instrument"],
                mode=row["mode"],
                obs_date=row["observation_date"],
                reference="Beil24",
                raise_error=True,
                format="tabular-fits",
            )

            # print(f"Spectrum for {source_name} ingested.")
            spectra_added += 1
            logger.info(f"Spectra ingested: {spectra_added}")
            print("\n------------------")

        except AstroDBError as e:
            skipped += 1
            logger.info(f"Spectra skipped: {skipped}")
            print(f"Error ingesting spectrum for {source_name}: {e}")
            print("\n------------------")
            
    logger.info(f"Total spectra added: {spectra_added}")
    logger.info(f"Total spectra skipped: {skipped}")

# Call Functions ----
ingest_spectra(data=data)

# Save to Database, Writes the JSON Files
if save_db: 
    db.save_database(directory="data/")
