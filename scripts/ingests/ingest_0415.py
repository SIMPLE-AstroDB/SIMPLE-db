from astrodb_utils import load_astrodb, ingest_instrument, ingest_publication
from simple.schema import *
from simple.schema import REFERENCE_TABLES
from simple.utils.spectra import ingest_spectrum
from astrodb_utils.photometry import ingest_photometry_filter, ingest_photometry
import logging

SAVE_DB = False  # save the data files in addition to modifying the .db file
RECREATE_DB = True  # recreates the .db file from the data files

logger = logging.getLogger("AstroDB")
logger.setLevel(logging.INFO)

db = load_astrodb("SIMPLE.sqlite", recreatedb=True,  reference_tables=REFERENCE_TABLES)

# Spectra Files

# Ingest Spectra

# Ingest Photometry Filters

# Ingest Photometry


# WRITE THE JSON FILES
if SAVE_DB:
    db.save_database(directory='data/')