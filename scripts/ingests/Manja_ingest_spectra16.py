from scripts.ingests.ingest_utils import *
from scripts.ingests.utils import *

SAVE_DB = False  # save the data files in addition to modifying the .db file
RECREATE_DB = True  # recreates the .db file from the data files

logger.setLevel(logging.INFO)

db = load_simpledb('SIMPLE.db', recreatedb=RECREATE_DB)

# Read in CSV file as Astropy table
data = Table.read('scripts/ingests/Manja16_spectra7.csv')

ingest_instrument(db, telescope='ESO VLT', instrument='XShooter', mode='Echelle')

# Add the sources to the database
# ingest_sources(db, data['Source'])

simp = db.Sources.update()\
         .where(db.Sources.c.source == 'SIMP J013656.5+093347.3')\
         .values(reference='Arti06')
db.engine.execute(simp)

# Add the spectra to the database
ingest_spectra(db, data['Source'], data['Spectrum'], data['regime'], 'ESO VLT', 'XShooter', 'Echelle',
               data['observation_date'],
               'Manj16', comments=data['spectrum comments'])

# WRITE THE JSON FILES
if SAVE_DB:
    db.save_database(directory='data/')
