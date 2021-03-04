# Script to add Y dwarfs spectral types

from astrodbkit2.astrodb import create_database
from astrodbkit2.astrodb import Database
#from simple.schema import *
from astropy.table import Table
import numpy as np
import re

connection_string = 'sqlite:///../../SIMPLE.db'  # SQLite
create_database(connection_string)
db = Database(connection_string)
db.load_database('../../data')


# load table
Ydwarfs = Table.read('Y-dwarf_table.csv',data_start=2)

# sources names in database Names table
db_names = []
for name in Ydwarfs['source']:
	db_name = db.search_object(name, output_table='Sources')[0].source
	db_names.append(db_name)

regime = ['infrared']*len(Ydwarfs) # all source have IR spectral classifications

# Convert SpT string to code
spectral_type_code = []
for spt in Ydwarfs['SpT']:
	spt_code = np.nan
	# identify main spectral class
	for i,item in enumerate(spt):
		if item=='M':
			spt_code=0
			break
		elif item=='L':
			spt_code=10
			break
		elif item=='T':
			spt_code=20
			break
		elif item=='Y':
			spt_code=30
			break
	# find integer or decimal subclass		
	spt_code+=float(re.findall('\d*\.?\d+',spt[i+1:])[0])
	spectral_type_code.append(spt_code)


# add missing references
ref_list = Ydwarfs['spt_ref'].tolist()
included_ref = db.query(db.Publications.c.name).filter(db.Publications.c.name.in_(ref_list)).all()
included_ref = [s[0] for s in included_ref]
new_ref = list(set(ref_list)-set(included_ref))
new_ref = [{'name': s} for s in new_ref]

if len(new_ref)>0:
	db.Publications.insert().execute(new_ref)

# Make astropy table with all relevant columns and add to SpectralTypes Table
SpT_table = Table([db_names,Ydwarfs['SpT'],spectral_type_code,regime,Ydwarfs['spt_ref']], names=('source','spectral_type_string','spectral_type_code','regime','reference'))
db.add_table_data(SpT_table, table='SpectralTypes', fmt='astropy')

db.save_db('../../data')


