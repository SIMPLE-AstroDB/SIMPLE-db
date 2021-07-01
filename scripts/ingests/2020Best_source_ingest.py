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
Best = Table.read('scripts/ingests/UltracoolSheet-Main.csv', data_start=0, data_end=2)

# find sources already in database
existing_sources = []
missing_sources = []
db_names = []
for i,name in enumerate(Best['name']):
	if len(db.search_object(name,resolve_simbad=True)) != 0:
		existing_sources.append(i)
		db_names.append(db.search_object(name,resolve_simbad=True)[0]) #.source was at the end 
	else:
		missing_sources.append(i)
		db_names.append(Best['name'][i])
print(len(existing_sources))
print(len(missing_sources))

# add missing references
for r in Best['ref_discovery'][missing_sources]:
	if search_publication(db, name=r)==False:
   		print(r)
		
		#add_publication(r)

# add missing objects to Sources table
if len(missing_sources)>0:
	for b in Best[missing_sources]:
		if search_publication(db, name=Best['ref_discovery'][b])==True:
			ref=Best['ref_discovery'][b]
		else:
			ref='Missing'
		
	#'source': name[b],
	#use columns O and P for RA and Dec 
		
	#db.add_table_data(Best[missing_sources], table='Sources', fmt='astropy')

# add new sources in Names table too
'''def add_names(): 
	names_data = []
	for ms in missing_sources:
		names_data.append({'source': Best['name'][ms], 'other_name':Best['name'][ms]})
	if len(missing_sources)>0:
		db.Names.insert().execute(names_data)
add_names()

# add other names for existing sources if alternative names not in database yet
other_names_data = []
for es in existing_sources:
	es_names = db.search_object(db_names[es], output_table='Names')
	if Best['name'][es] not in [x[1] for x in es_names]:
		other_names_data.append({'source': db_names[es], 'other_name':Best['name'][es]})
if len(existing_sources)>0:
	db.Names.insert().execute(other_names_data)'''

if save_db == True:
    db.save_db('data') #edits the JSON files if we're not doing a dry run
