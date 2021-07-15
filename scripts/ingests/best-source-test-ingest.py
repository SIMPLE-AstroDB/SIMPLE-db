from scripts.ingests.utils import *
from astrodbkit2.astrodb import create_database
from astrodbkit2.astrodb import Database
from simple.schema import *
from astropy.table import Table
from pathlib import Path
import os

save_db = False #modifies .db file but not the data files
RECREATE_DB = True #recreates the .db file from the data files
VERBOSE = False

def load_db():
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

    return db
db = load_db()

# load table of sources to ingest
ingest_table = Table.read('scripts/ingests/UltracoolSheet-Main.csv', data_start=1, data_end=50)

# use column names of ingest table to populate necessary lists
ingest_names = ingest_table['name']
ingest_ras = ingest_table['ra_j2000_formula'] # decimal degrees
ingest_decs = ingest_table['dec_j2000_formula'] # decimal degrees
n_sources = len(ingest_names)

print(n_sources,"Total Sources")

# find sources not already in the database
missing_sources_indexes, existing_sources_indexes, all_sources = \
    sort_sources(db, ingest_names, ingest_ras, ingest_decs, verbose=False)
missing_sources = ingest_names[missing_sources_indexes]
existing_sources = ingest_names[existing_sources_indexes]

# Add missing sources to the database
missing_ras = ingest_table['ra_j2000_formula'][missing_sources_indexes]
missing_decs = ingest_table['dec_j2000_formula'][missing_sources_indexes]
missing_refs = ingest_table['ref_discovery'][missing_sources_indexes]
missing_eqxs = ['2000'] * n_sources  # all sources are J2000

# TODO References map

ingest_sources(db, missing_sources, missing_ras, missing_decs, missing_refs,
               equinoxes=missing_eqxs, verbose=False, save_db=save_db)

# Add names of new sources to the Names table
add_names(db, missing_sources,verbose=True, save_db=save_db)
