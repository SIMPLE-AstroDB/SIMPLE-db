from operator import add
from re import search
import sys
from sqlalchemy.sql.elements import Null
sys.path.append('.')
from scripts.ingests.utils import *
from astrodbkit2.astrodb import create_database
from astrodbkit2.astrodb import Database
from simple.schema import *
from astropy.table import Table
from pathlib import Path
import os

save_db = False #modifies .db file but not the data files
RECREATE_DB = False #recreates the .db file from the data files
VERBOSE = False

verboseprint = print if VERBOSE else lambda *a, **k: None

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
ingest_table = Table.read('scripts/ingests/UltracoolSheet-Main.csv', data_start=575)

# use column names of ingest table to populate necessary lists
ingest_names = ingest_table['name']
ingest_ras = ingest_table['ra_j2000_formula'] # decimal degrees
ingest_decs = ingest_table['dec_j2000_formula'] # decimal degrees
n_sources = len(ingest_names)

print(n_sources,"Total Sources")

#names_data = ({'source': 'TWA 27', 'other_name': '2MASSW J1207334-393254'})
#db.Names.insert().execute(names_data)
#add_publication(db, name='Mart99e', bibcode='1999AJ....118.2466M', save_db=True)
db.Names.delete().where(db.Names.c.other_name == 'SDSS J141624.08+134826.7B').execute()
#add_publication(db, name='Gizi00c', bibcode='2000AJ....120.1085G', save_db=True )


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

#print(missing_refs)

for i,ref in enumerate(missing_refs):
	if ref=="Kend07a":
		missing_refs[i] = "Kend07"
	if ref=="Mace13a":
		missing_refs[i] = "Mace13"
	if ref=='Kend03a':
		missing_refs[i] = "Kend03"
	if ref=='West08a':
		missing_refs[i] = "West08"
	if ref=='Lepi02b':
		missing_refs[i] = "Lepi02"
	if ref=='Reid05b':
		missing_refs[i] = "Reid05"
	if ref=='Burg08b':
		missing_refs[i] = "Burg08c"
	if ref=='Burg08c':
		missing_refs[i] = "Burg08d"
	if ref=='Burg08d':
		missing_refs[i] = "Burg08b"
	if ref=='Gagn15b':
		missing_refs[i] = "Gagn15c"
	if ref=='Gagn15c':
		missing_refs[i] = "Gagn15b"
	if ref=='Lodi07a':
		missing_refs[i] = "Lodi07b"
	if ref=='Lodi07b':
		missing_refs[i] = "Lodi07a"
	if ref=='Reid02c':
		missing_refs[i] = "Reid02b"
	if ref=='Reid06a':
		missing_refs[i] = "Reid06b"
	if ref=='Reid06b':
		missing_refs[i] = "Reid06a"
	if ref=='Scho04b':
		missing_refs[i] = "Scho04a"
	if ref=='Scho10a':
		missing_refs[i] = "Scho10b"
	if ref=='Tinn93b':
		missing_refs[i] = "Tinn93c"
	if ref=='Skrz16; Best20a':
		missing_refs[i] = "Skrz16"
	if ref=='Chau03a; Neuh04a':
		missing_refs[i] = "Chau03a"
	if ref=='Schm10b; Kirk10':
		missing_refs[i] = "Kirk10"
	if ref=='Alle12; Deac14b':
		missing_refs[i] = 'Alle12'
	if ref=='Sarr14; Best17a':
		missing_refs[i] = "Sarr14"
	if ref=='Bryj92; Bryj94':
		missing_refs[i] = "Bryj92"
	if ref=='Todo10; Todo14':
		missing_refs[i] = "Todo10"
	if ref=='Pena12; Pena15':
		missing_refs[i] = "Pena12"
	if ref=='Biha09; Pena15':
		missing_refs[i] = "Biha09"
	if ref=='Cruz07; Shko09':
		missing_refs[i] = "Cruz07"
	if ref=='Scho12; Burn13':
		missing_refs[i] = "Scho12"
	if ref== 'Tinn18; Mart18':
		missing_refs[i]= 'Tinn18'
	if ref=='Lowr99; Webb99':
		missing_refs[i] = 'Lowr99'
	if ref=='Shep09a; Kirk10':
		missing_refs[i] = 'Kirk10'
	if ref=='Ruiz01a; Gizi02b':
		missing_refs[i] = 'Gizi02b'
	if ref=='Scho10a; Burn10a':
		missing_refs[i] = 'Scho10a'
	if ref=='Bowl10a; Schm10a':
		missing_refs[i] = 'Bowl10a'
	if ref=='Litt08; Litt13':
		missing_refs[i] = 'Litt08'
	if ref=='Goli04b; Forv04':
		missing_refs[i] = 'Goli04b'
	if ref=='Burn10b; Albe11':
		missing_refs[i] = 'Burn10b'
	if ref=='Scho10b; Murr11':
		missing_refs[i] = 'Murr11'
	if ref=='Gagn15c; Alle16':
		missing_refs[i] = 'Alle16'
	if ref=='Lodi13c; Best17a':
		missing_refs[i] = 'Lodi13c'
	if ref=='Kouw05; Lafr11':
		missing_refs[i] = 'Kouw05'
	if ref=='Krau09a; Alle13b':
		missing_refs[i] = 'Krau09a'
	if ref=='Curr14a; Krau14a':
		missing_refs[i] = 'Curr14a'
	if ref=='Kirk11; Scho11':
		missing_refs[i] = 'Kirk11'
	if ref=='Fahe12; Burg08c':
		missing_refs[i] = 'Fahe12'
	if ref=='Gicl67a; Rodo80':
		missing_refs[i] = 'Gicl67a'
	if ref=='Hawk88a; Kirk11':
		missing_refs[i] = 'Kirk11'
	if ref=='Reid08b; Burg06a':
		missing_refs[i] = 'Reid08b'
	if ref=='Phan01; Lodi05b':
		missing_refs[i] = 'Phan01'
	 
		
#Schm10b = Schm10 and Schm10b in database (reference in there twice), but Best ref name should map to one of them without error

ingest_sources(db, missing_sources, missing_ras, missing_decs, missing_refs,
               equinoxes=missing_eqxs, verbose=False, save_db=save_db)

# Add names of new sources to the Names table
add_names(db, missing_sources,verbose=True, save_db=save_db)
