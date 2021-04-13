from astrodbkit2.astrodb import create_database
from astrodbkit2.astrodb import Database
from astropy.table import Table
from utils import ingest_parallaxes

connection_string = 'sqlite:///../../SIMPLE.db'  # SQLite
create_database(connection_string)
db = Database(connection_string)
db.load_database('../../data')

# load table
ingest_table = Table.read('Y-dwarf_table.csv', data_start=2)
sources = ingest_table['source']
plx = ingest_table['plx_mas']
plx_unc = ingest_table['plx_err']
plx_ref = ingest_table['astrometry_ref']

ingest_parallaxes(db, sources, plx, plx_unc, plx_ref, verbose=True)

# Save modified JSON files
db.save_db('../../data')
