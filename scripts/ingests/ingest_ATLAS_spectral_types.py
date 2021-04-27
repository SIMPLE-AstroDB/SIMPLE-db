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
ingest_table_df = ingest_table.to_pandas()
names = ingest_table['Name']
n_sources = len(names)
# regime = ['infrared'] * n_sources
spectral_types_unknown = ingest_table['SpType'] # pre-existing spectral types
spectral_types_spex = ingest_table['SpTSpeX'] # new spectral types
# spt_refs = ingest_table['spt_ref']

# fetch primary name identifier from database
db_names = []
for name in names:
	db_name = db.search_object(name, output_table='Sources')[0].source
	print(name, db_name)
	db_names.append(db_name)

# Ingest new spectral type estimates from the SpTSpeX column
db_names_spex = []
spex_types_string = []
for i,db_name in enumerate(db_names):
	if spectral_types_spex[i] != "":
		db_names_spex.append(db_name)
		spex_types_string.append(spectral_types_spex[i])

spex_types_codes = convert_spt_string_to_code(spex_types_string, verbose = True)
regime = ['nir']*len(db_names_spex)
spt_ref = ['Manj19']*len(db_names_spex)
SpT_table_spex = Table([db_names_spex, spex_types_string, spex_types_codes, regime, spt_ref], names=('source','spectral_type_string','spectral_type_code','regime','reference'))
SpT_table_spex_df = SpT_table_spex.to_pandas() # make a Pandas dataframe to explore  with Pycharm

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