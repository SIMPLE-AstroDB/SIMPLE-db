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
spectral_types = ingest_table['SpType'] # pre-existing spectral types
spectral_types_spex = ingest_table['SpTSpeX'] # new spectral types
#spt_refs = ingest_table['spt_ref']
# need to adjust for only adding SpT for sources lacking SpT

# fetch primary name identifier from database
db_names = []
for name in names:
	db_name = db.search_object(name, output_table='Sources')[0].source
	print(name, db_name)
	db_names.append(db_name)

# Ingest all new spectral type estimates

# find which sources have new spectral types
name_has_spex = []
spex_types_string = []
for i,db_name in enumerate(db_names):
	if spectral_types_spex[i] != "":
		name_has_spex.append(db_name)
		spex_types_string.append(spectral_types_spex[i])

spex_types_codes = convert_spt_string_to_code(spex_types_string, verbose = True)
regime = ['nir']*len(name_has_spex)
spt_ref = ['Manj19']*len(name_has_spex)

SpT_table_spex = Table([name_has_spex, spex_types_string, spex_types_codes, regime, spt_ref], names=('source','spectral_type_string','spectral_type_code','regime','reference'))

# Find out which sources don't have spectral types
# needs_spectral_type = []
# spectral_types_to_add = []
# for i, db_name in enumerate(db_names):
# 	db_spectral_types = db.query(db.SpectralTypes).filter(db.SpectralTypes.c.source == db_name).table()
# 	if db_spectral_types is None or len(db_spectral_types) == 0:
# 		needs_spectral_type.append(db_name)
# 		spectral_types_to_add.append(spectral_types_optical[i])

# Convert SpT string to code
#spectral_type_codes_optical = convert_spt_string_to_code(spectral_types_to_add, verbose = True)
#spectral_type_codes_nir = convert_spt_string_to_code(spectral_types_nir, verbose = True)

# Comment = From ATLAS Table Manajavacas etal. 2019
# reference = UNKNOWN