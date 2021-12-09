from scripts.ingests.ingest_utils import *
from scripts.ingests.utils import *

SAVE_DB = False  # save the data files in addition to modifying the .db file
RECREATE_DB = True  # recreates the .db file from the data files

logger.setLevel(logging.INFO)

db = load_simpledb('SIMPLE.db', recreatedb=RECREATE_DB)

# Read in CSV file as Astropy table
data = Table.read('scripts/ingests/Manja16_spectra6.csv')

ingest_instrument(db, telescope='ESO VLT', instrument='XShooter', mode='Echelle')
# TODO: fix issue in ingest instrument function that is not adding the mode since it is likely only checkin the name
# mode_add = [{'name': 'Echelle',
#              'instrument': 'XShooter',
#              'telescope': 'ESO VLT'}]
# db.Modes.insert().execute(mode_add)

# Add the sources to the database

ingest_sources(db, data['Source'])
# TODO: the ingest spectra function suspects that the other 21 spectra are duplicates. It is because it does not check the regime when determining a duplicate.

# # Add the spectra to the database
ingest_spectra(db, data['Source'], data['Spectrum'], data['regime'], 'ESO VLT', 'XShooter', 'Echelle',
               data['observation_date'],
               'Manj16', comments=data['spectrum comments'])

# WRITE THE JSON FILES
if SAVE_DB:
    db.save_database(directory='data/')
