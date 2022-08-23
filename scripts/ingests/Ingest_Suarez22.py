from scripts.ingests.ingest_utils import *
from scripts.ingests.utils import *


SAVE_DB = True  # save the data files in addition to modifying the .db file
RECREATE_DB = True  # recreates the .db file from the data files

logger.setLevel(logging.DEBUG)

db = load_simpledb('SIMPLE.db', recreatedb=RECREATE_DB)

# Reading in File
file = 'IRS Spectrum - Final Vers  - Sheet1.csv'
data = Table.read('scripts/ingests/' + file)

# Ingesting missing publications
ingest_publication(db, bibcode='2022MNRAS.513.5701S', publication='Suar22')

# Ingesting the sources along with the necessary discovery reference
ingest_sources(db, data['Source'], data['discovery reference'])

# Used new parameter for ingest spectra to add original spectrum
ingest_spectra(db, data['Source'], data['spectrum'], 'mir', 'Spitzer', 'IRS', 'SL',
               data['observation_date'],
               'Suar22', original_spectra=data['original_spectrum'], wavelength_units='um', flux_units='Jy', comments=data['spectrum comments'])

# WRITE THE JSON FILES
if SAVE_DB:
    db.save_database(directory='data/')
