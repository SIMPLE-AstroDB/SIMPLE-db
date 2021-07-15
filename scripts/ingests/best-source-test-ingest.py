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

for i,ref in enumerate(Best['ref_discovery'][missing_sources_index]):
	if search_publication(db, name=ref)==False:
		print("Missing Publication: ", ref)
		if ref=="Kend07a":
			Best['ref_discovery'][missing_sources_index[i]] = "Kend07"
		if ref=="Mace13a":
			Best['ref_discovery'][missing_sources_index[i]] = "Mace13"
		if ref=='Kend03a':
			Best['ref_discovery'][missing_sources_index[i]] = "Kend03"
		if ref=='West08a':
			Best['ref_discovery'][missing_sources_index[i]] = "West08"
		if ref=='Lepi02b':
			Best['ref_discovery'][missing_sources_index[i]] = "Lepi02"
		if ref=='Reid05b':
			Best['ref_discovery'][missing_sources_index[i]] = "Reid05"
		if ref=='Burg08b':
			Best['ref_discovery'][missing_sources_index[i]] = "Burg08c"
		if ref=='Burg08c':
			Best['ref_discovery'][missing_sources_index[i]] = "Burg08d"
		if ref=='Burg08d':
			Best['ref_discovery'][missing_sources_index[i]] = "Burg08b"
		if ref=='Gagn15b':
			Best['ref_discovery'][missing_sources_index[i]] = "Gagn15c"
		if ref=='Gagn15c':
			Best['ref_discovery'][missing_sources_index[i]] = "Gagn15b"
		if ref=='Lodi07a':
			Best['ref_discovery'][missing_sources_index[i]] = "Lodi07b"
		if ref=='Lodi07b':
			Best['ref_discovery'][missing_sources_index[i]] = "Lodi07a"
		if ref=='Reid02c':
			Best['ref_discovery'][missing_sources_index[i]] = "Reid02b"
		if ref=='Reid06a':
			Best['ref_discovery'][missing_sources_index[i]] = "Reid06b"
		if ref=='Reid06b':
			Best['ref_discovery'][missing_sources_index[i]] = "Reid06a"
		if ref=='Scho04b':
			Best['ref_discovery'][missing_sources_index[i]] = "Scho04a"
		if ref=='Scho10a':
			Best['ref_discovery'][missing_sources_index[i]] = "Scho10b"
		if ref=='Tinn93b':
			Best['ref_discovery'][missing_sources_index[i]] = "Tinn93c"
		
#Schm10b = Schm10 and Schm10b in database (reference in there twice), but Best ref name should map to one of them without error

ingest_sources(db, missing_sources, missing_ras, missing_decs, missing_refs,
               equinoxes=missing_eqxs, verbose=False, save_db=save_db)

# Add names of new sources to the Names table
add_names(db, missing_sources,verbose=True, save_db=save_db)
