from scripts.ingests.ingest_utils import *
from scripts.ingests.utils import *


SAVE_DB = False  # save the data files in addition to modifying the .db file
RECREATE_DB = True  # recreates the .db file from the data files

logger.setLevel(logging.DEBUG)

db = load_simpledb('SIMPLE.db', recreatedb=RECREATE_DB)

source = '2MASS J12560183-1257276'

# Add other name
other_name_data = [{'source': source, 'other_name': 'VHS 1256-1257b'}]
db.Names.insert().execute(other_name_data)

# Ingesting missing publications
ingest_publication(db, bibcode='2018ApJ...869...18M')

# Ingest spectral type
ingest_spectral_types(db,
                      source=source,
                      spectral_types=87, # should be L7, not sure what the code is
                      spectral_type_error=1.5,
                      references='Gauz15')

# ingest optical type of L8 pm 2
# other spectral types from Miles 2018?

# Ingest spectra
#ingest_spectra(db, source, data['spectrum'], 'mir', 'Spitzer', 'IRS', 'SL',
#               data['observation_date'],
#               'Suar22', original_spectra=data['original_spectrum'], wavelength_units='um', flux_units='Jy', comments=data['spectrum comments'])

# ingest Gauz15 spectrum
# ingest Miles18 spectrum
# ingest Miles18 version of Gauz15 data

# WRITE THE JSON FILES
if SAVE_DB:
    db.save_database(directory='data/')