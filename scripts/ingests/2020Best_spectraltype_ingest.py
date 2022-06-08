import logging

# from astroquery.simbad import conf
from scripts.ingests.ingest_utils import *
from scripts.ingests.utils import *

# conf.server = 'simbad.harvard.edu'
# Simbad.SIMBAD_URL='http://harvard.simbad.edu/simbad/sim-script'
# Simbad.URL = 'harvard.simbad.edu'
SAVE_DB = False  # save the data files in addition to modifying the .db file
RECREATE_DB = True  # recreates the .db file from the data files

logger.setLevel(logging.DEBUG)

db = load_simpledb('SIMPLE.db', recreatedb=RECREATE_DB)

# Read in CSV file as Astropy table
# file = 'Manja19_spectra9.csv'
file = 'UltracoolSheet-Main.csv'
data = Table.read('scripts/ingests/' + file)


# WRITE THE JSON FILES
if SAVE_DB:
    db.save_database(directory='data/')
