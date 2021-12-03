from scripts.ingests.ingest_utils import *
from scripts.ingests.utils import *

SAVE_DB = False  # save the data files in addition to modifying the .db file
RECREATE_DB = True  # recreates the .db file from the data files

logger.setLevel(logging.INFO)

db = load_simpledb('SIMPLE.db', recreatedb=RECREATE_DB)

# Read in CSV file as Astropy table
data = Table.read('scripts/ingests/Manja_spectra5.csv')


# Add Jaya06 to the database
# ingest_publication(db, doi='10.1086/507522')

# Inserting New Mode
def insert_modes():
    telescope_vlt3 = [{'name': 'ESO VLT U3'}]
    instruments_isc = [{'name': 'ISAAC'}]
    swl_mode = [{'name': 'SW LRes',
                 'instrument': 'ISAAC',
                 'telescope': 'ESO VLT U3'}]

    db.Telescopes.insert().execute(telescope_vlt3)
    db.Instruments.insert().execute(instruments_isc)
    db.Modes.insert().execute(swl_mode)

    return


if RECREATE_DB:
    insert_modes()

# Add the sources to the database
ingest_sources(db, data['Source'], data['reference'])

# Add the spectra to the database
ingest_spectra(db, data['Source'], data['Spectrum'], 'nir', 'ESO VLT U3', 'ISAAC', 'SW LRes', data['observation_date'],
               data['reference'], comments=data['comments'])
