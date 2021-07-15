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
Best = Table.read('scripts/ingests/UltracoolSheet-Main.csv', data_start=1, data_end=20)

# find sources already in database
existing_sources_index = []
missing_sources_index = []
db_names = []
for i,name in enumerate(Best['name']):
	namematches = db.search_object(name,resolve_simbad=True)
	print(namematches)
	if len(namematches) == 1:
		existing_sources_index.append(i)
		db_names.append(namematches[0]['source'])  
	elif len(namematches) > 1:
		print("More than one match")
		break
	else:
		missing_sources_index.append(i)
		db_names.append(Best['name'][i])
print("Existing Sources: ", Best['name'][existing_sources_index])
print("Missing Sources: ", Best['name'][missing_sources_index])
print("Db names: ", db_names)

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
		
			#print(Best['ref_discovery'][missing_sources_index[i]])

#Schm10b = Schm10 and Schm10b in database (reference in there twice)

	elif search_publication(db, name=ref)==True:
	#TO DO: Make sure found publication is correct publication 
		pass 
print(Best['ref_discovery'][missing_sources_index])


# add missing objects to Sources table
if len(missing_sources_index)>0:
	for b in Best[missing_sources_index]:
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
