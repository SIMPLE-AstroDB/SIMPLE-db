from scripts.ingests.ingest_utils import *
from scripts.ingests.utils import *


SAVE_DB = False  # save the data files in addition to modifying the .db file
RECREATE_DB = True  # recreates the .db file from the data files

logger.setLevel(logging.DEBUG)

db = load_simpledb('SIMPLE.db', recreatedb=RECREATE_DB)

file = 'IRS Spectrum - Final Vers  - Sheet1.csv'
data = Table.read('scripts/ingests/' + file)

ingest_publication(db, bibcode='2022MNRAS.513.5701S')
ingest_publication(db, bibcode='1952ApJ...115..582L')
ingest_sources(db, data['Source'], data['discovery reference'])

ingest_spectra(db, data['Source'], data['Spectrum'], 'mir', 'Spitzer', 'IRS', 'SL',
               data['observation_date'],
               'Suár22', wavelength_units='um', flux_units='Jy', comments=data['spectrum comments'])
