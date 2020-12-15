# Script to ingest Y dwarfs from Kirkpartick+2019
# testing push

from astrodbkit2.astrodb import create_database
from astrodbkit2.astrodb import Database
#from simple.schema import *
from astropy.table import Table

connection_string = 'sqlite:///../../SIMPLE.db'  # SQLite
create_database(connection_string)
db = Database(connection_string)
db.load_database('../../data')


# load table of sources to ingest
Ydwarfs = Table.read('Y-dwarf_table.csv',data_start=2)

# find sources already in database
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

# add new sources in Names table too
names_data = []
for ms in missing_sources:
	names_data.append({'source': Ydwarfs['source'][ms], 'other_name':Ydwarfs['source'][ms]})
if len(missing_sources)>0:
	db.Names.insert().execute(names_data)

# add other names for existing sources if alternative names not in database yet
other_names_data = []
for es in existing_sources:
	es_names = db.search_object(db_names[es], output_table='Names')
	if Ydwarfs['source'][es] not in [x[1] for x in es_names]:
		other_names_data.append({'source': db_names[es], 'other_name':Ydwarfs['source'][es]})
if len(existing_sources)>0:
	db.Names.insert().execute(other_names_data)

git add Y_dwarf_source_ingest.py
db.save_db('../../data')
