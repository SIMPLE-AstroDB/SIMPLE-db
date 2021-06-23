import sys
from sqlalchemy.sql.elements import Null
sys.path.append('.')
from astrodbkit2.astrodb import create_database
from astrodbkit2.astrodb import Database
from simple.schema import *
from utils import ingest_photometry
from astropy.table import Table
from pathlib import Path
import os 



DRY_RUN = True #modifies .db file but not the data files
RECREATE_DB = True #recreates the .db file from the data files
VERBOSE = False

verboseprint = print if VERBOSE else lambda *a, **k: None

db_file = 'SIMPLE.db'
db_file_path = Path(db_file)
db_connection_string = 'sqlite:///SIMPLE.db'  # SQLite browser

if RECREATE_DB and db_file_path.exists():
    os.remove(db_file) #removes the current .db file if one already exists

if not db_file_path.exists():
    create_database(db_connection_string) #creates empty database based on the simple schema
    db = Database(db_connection_string) #connects to the empty database
    db.load_database('data/') #loads the data from the data files into the database
else:
    db = Database(db_connection_string) #if database already exists, connects to .db file

#Adding Spizter and IRAC to telescope and instrument tables
new_telescope=[{'name':'Spitzer'}]
db.Telescopes.insert().execute(new_telescope)

new_instrument=[{'name':'IRAC'}]
db.Instruments.insert().execute(new_instrument)
print('done')

# load table
ingest_table = Table.read('scripts/ingests/Y-dwarf_table.csv', data_start=2)

#Defining variables
source = ingest_table['source']
band1 = ['IRAC.I1']*len(source)
band2 = ['IRAC.I2']*len(source)
ucd1 = ['em.IR.3-4um']*len(source)
ucd2 = ['em.IR.4-8um']*len(source)
ch1mag = ingest_table['ch1_mag']
ch1mag_err = ingest_table['ch1_err']
ch2mag = ingest_table['ch2_mag']
ch2mag_err = ingest_table['ch2_err']
telescope = ['Spitzer']*len(source)
instrument = ['IRAC']*len(source)
reference = ingest_table['Spizter_ref']

#ingesting band 1 photometry
ingest_photometry(db, sources = source, bands = band1, ucds = ucd1, magnitudes = ch1mag, magnitude_errors = ch1mag_err, telescope = telescope, instrument = instrument, reference = reference, verbose=True)

#ingesting band 2 photometry 
ingest_photometry(db, sources = source, bands = band2, ucds = ucd2, magnitudes = ch2mag, magnitude_errors = ch2mag_err, telescope = telescope, instrument = instrument, reference = reference, verbose=True)

if not DRY_RUN:
    db.save_db('data') #edits the JSON files if we're not doing a dry run
