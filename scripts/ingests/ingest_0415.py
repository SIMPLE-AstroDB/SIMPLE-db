from astrodb_scripts import load_astrodb, ingest_instrument, ingest_publication
from schema.schema import *
from scripts.utils.ingest_spectra_utils import ingest_spectrum
from scripts.utils.photometry import ingest_photometry_filter, ingest_photometry
import logging

SAVE_DB = False  # save the data files in addition to modifying the .db file
RECREATE_DB = True  # recreates the .db file from the data files

logger = logging.getLogger("AstroDB")
logger.setLevel(logging.INFO)

db = load_astrodb('SIMPLE.db', recreatedb=RECREATE_DB)

# Spectra Files
# Ingest Spectra
# Ingest Photometry Filters
# Ingest Photometry


# WRITE THE JSON FILES
if SAVE_DB:
    db.save_database(directory='data/')