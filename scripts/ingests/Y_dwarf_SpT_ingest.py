# Script to add Y dwarfs spectral types

from astrodbkit2.astrodb import create_database
from astrodbkit2.astrodb import Database
#from simple.schema import *
from astropy.table import Table
import numpy as np
import re
from utils import convert_spt_string_to_code

connection_string = 'sqlite:///../../SIMPLE.db'  # SQLite
create_database(connection_string)
db = Database(connection_string)
db.load_database('../../data')


# load table
ingest_table = Table.read('Y-dwarf_table.csv', data_start = 2)
names = ingest_table['source']
n_sources = len(names)
regime = ['infrared'] * n_sources # all source have IR spectral classifications
spectral_types = ingest_table['SpT']
spt_refs = ingest_table['spt_ref']

# sources names in database Names table
db_names = []
for name in names:
	db_name = db.search_object(name, output_table='Sources')[0].source
	db_names.append(db_name)

# Convert SpT string to code
spectral_type_codes = convert_spt_string_to_code(spectral_types, verbose = True)

# add new references to Publications table
ref_list = spt_refs.tolist()
included_ref = db.query(db.Publications.c.name).filter(db.Publications.c.name.in_(ref_list)).all()
included_ref = [s[0] for s in included_ref]
new_ref = list(set(ref_list) - set(included_ref))
new_ref = [{'name': s} for s in new_ref]

if len(new_ref)>0:
	db.Publications.insert().execute(new_ref)

# Make astropy table with all relevant columns and add to SpectralTypes Table
SpT_table = Table([db_names, spectral_types, spectral_type_codes, regime, spt_refs], names=('source','spectral_type_string','spectral_type_code','regime','reference'))
db.add_table_data(SpT_table, table='SpectralTypes', fmt='astropy')

db.save_db('../../data')


