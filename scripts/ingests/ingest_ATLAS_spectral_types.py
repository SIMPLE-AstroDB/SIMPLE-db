#------------------------------------------------------------------------------------------------

from astrodbkit2.astrodb import create_database
from astrodbkit2.astrodb import Database
from astropy.table import Table
import numpy as np
import re
from utils import convert_spt_string_to_code

connection_string = 'sqlite:///../../SIMPLE.db'  # SQLite
create_database(connection_string)
db = Database(connection_string)
db.load_database('../../data')


# load table of sources to ingest
ingest_table = Table.read("ATLAS_table.vot")
names = ingest_table['Name']
n_sources = len(names)
#regime = ['infrared'] * n_sources # all source have IR spectral classifications
spectral_types_optical = ingest_table['SpType']
spectral_types_nir = ingest_table['SpTSpeX']
#spt_refs = ingest_table['spt_ref']
# need to adjust for only adding SpT for sources lacking SpT



# sources names in database Names table
db_names = []
for name in names:
	print(name)
	db_name = db.search_object(name, output_table='Sources')[0].source
	print(db_name)
	db_names.append(db_name)

# Convert SpT string to code
spectral_type_codes_optical = convert_spt_string_to_code(spectral_types_optical, verbose = True)
spectral_type_codes_nir = convert_spt_string_to_code(spectral_types_nir, verbose = True)