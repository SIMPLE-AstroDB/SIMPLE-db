# Script to ingest Y dwarfs from Kirkpartick+2019

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
for i,name in enumerate(Ydwarfs['source']):
	if len(db.search_object(name,resolve_simbad=True)) != 0:
		existing_sources.append(i)
	else:
		missing_sources.append(i)

# add missing references
temp = db.query(db.Publications.c.name).all()
existing_ref = [s[0] for s in temp]

new_ref = []
for ref in Ydwarfs['reference']:
	if ref not in existing_ref:
		new_ref.append({'name':ref})

db.Publications.insert().execute(new_ref)


# add missing objects to Sources table
db.add_table_data(Ydwarfs[missing_sources], table='Sources', fmt='astropy')


db.save_db('../../data')
