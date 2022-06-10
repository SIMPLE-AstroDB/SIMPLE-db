import logging
# from astroquery.simbad import conf
from scripts.ingests.ingest_utils import *
from scripts.ingests.utils import *
# from scripts.ingests.utils_deprecated import sort_sources
from scripts.ingests.utils_deprecated import sort_sources
import re

SAVE_DB = False  # save the data files in addition to modifying the .db file
RECREATE_DB = True  # recreates the .db file from the data files

logger.setLevel(logging.DEBUG)

db = load_simpledb('SIMPLE.db', recreatedb=RECREATE_DB)

# Read in CSV file as Astropy table
# file = 'Manja19_spectra9.csv'
file = 'UltracoolSheet-Main.csv'
data = Table.read('scripts/ingests/' + file, data_start=2)
# Ingest Optical Spectral Types First

names = data['name']
n_sources = len(names)
regime = ['optical'] * n_sources  # all source have IR spectral classifications
spectral_types = data['spt_opt']
spt_refs = data['ref_spt_opt']
# print(names)
# sources names in database Names table

# missing_sources_indexes, existing_sources_indexes, all_sources = sort_sources(db, names)
# missing_sources = names[missing_sources_indexes]
# existing_sources = names[existing_sources_indexes]
# There is one missing source that is not ingested in the database

# db_names = []
# for name in names:
#     db_name = db.search_object(name, output_table='Sources')[0]
#     if db_name.length < 1:
#         continue
#     db.names.append(db_name)
#     # print(db_name)

# Convert SpT string to code
spectral_type_codes = convert_spt_string_to_code(spectral_types)

# add new references to Publications table
# ref_list = spt_refs.tolist()
# included_ref = db.query(db.Publications.c.name).filter(db.Publications.c.name.in_(ref_list)).all()
# included_ref = [s[0] for s in included_ref]
# new_ref = list(set(ref_list) - set(included_ref))
# new_ref = [{'name': s} for s in new_ref]
#
# if len(new_ref) > 0:
#     db.Publications.insert().execute(new_ref)
#
# # Make astropy table with all relevant columns and add to SpectralTypes Table
# SpT_table = Table([db_names, spectral_types, spectral_type_codes, regime, spt_refs],
#                   names=('source', 'spectral_type_string', 'spectral_type_code', 'regime', 'reference'))
# db.add_table_data(SpT_table, table='SpectralTypes', fmt='astropy')

# WRITE THE JSON FILES
if SAVE_DB:
    db.save_database(directory='data/')
