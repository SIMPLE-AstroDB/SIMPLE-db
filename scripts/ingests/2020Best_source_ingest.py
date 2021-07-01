import sys
from sqlalchemy.sql.elements import Null
sys.path.append('.')
from astrodbkit2.astrodb import create_database
from astrodbkit2.astrodb import Database
from simple.schema import *
from utils import *
from astropy.table import Table
from pathlib import Path
import os

DRY_RUN = False #modifies .db file but not the data files
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

# load table of sources to ingest
Best = Table.read('UltracoolSheet-Main.csv')

# find sources already in database
existing_sources = []
missing_sources = []
db_names = []
for i,name in enumerate(Ydwarfs['source']):
	if len(db.search_object(name,resolve_simbad=True)) != 0:
		existing_sources.append(i)
		db_names.append(db.search_object(name,resolve_simbad=True)[0].source)
	else:
		missing_sources.append(i)
		db_names.append(Ydwarfs['source'][i])

# add missing references
ref_list = Ydwarfs['reference'].tolist()
included_ref = db.query(db.Publications.c.name).filter(db.Publications.c.name.in_(ref_list)).all()
included_ref = [s[0] for s in included_ref]
new_ref = list(set(ref_list)-set(included_ref))
new_ref = [{'name': s} for s in new_ref]

if len(new_ref)>0:
	db.Publications.insert().execute(new_ref)


# add missing objects to Sources table
if len(missing_sources)>0:
	db.add_table_data(Ydwarfs[missing_sources], table='Sources', fmt='astropy')

# add new sources in Names table too
names_data = []
for ms in missing_sources:
	names_data.append({'source': Ydwarfs['source'][ms], 'other_name':Ydwarfs['source'][ms]})
if len(missing_sources)>0:
	db.Names.insert().execute(names_data)

# add other names for existing sources if alternative names not in database yet
other_names_data = []
for es in existing_sources:
	es_names = db.search_object(db_names[es], output_table='Names')
	if Ydwarfs['source'][es] not in [x[1] for x in es_names]:
		other_names_data.append({'source': db_names[es], 'other_name':Ydwarfs['source'][es]})
if len(existing_sources)>0:
	db.Names.insert().execute(other_names_data)

db.save_db('../../data')