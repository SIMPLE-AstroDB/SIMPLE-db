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
data = Table.read('scripts/ingests/' + file)
# Ingest Optical Spectral Types First
# use the hashtag to comment it out

names = data['name']
n_sources = len(names)
regime = ['optical'] * n_sources
spectral_types = data['spt_opt']
spt_refs = data['ref_spt_opt']


# sources names in database Names table

def ingest_source_pub():
    ingest_sources(db, 'BRI 0021-0214', 'Irwi91')
    ingest_sources(db, 'LHS 1070', 'McCa64c')
    ingest_sources(db, 'WISE J003110.04+574936.3', 'Thom13')
    ingest_sources(db, 'DENIS-P J0041353-562112', 'Phan01')
    ingest_sources(db, 'DENIS J020529.0-115925', 'Delf97')
    ingest_sources(db, 'LP 261-75B', 'Kirk00')
    ingest_sources(db, 'LHS 292', 'Dahn86')



ingest_source_pub()
db_names = []
for name in names:
    db_name = db.search_object(name, output_table='Sources', table_names={'Names': ['other_name']})[0]
    db_names.append(db_name['source'])

# Convert SpT string to code
spectral_type_codes = convert_spt_string_to_code(spectral_types, verbose=True)

ref_list = spt_refs.tolist()
included_ref = db.query(db.Publications.c.publication).filter(db.Publications.c.publication.in_(ref_list)).all()
included_ref = [s[0] for s in included_ref]
new_ref = list(set(ref_list) - set(included_ref))
new_ref = [{'publication': s} for s in new_ref]

if len(new_ref) > 0:
    db.Publications.insert().execute(new_ref)

# Make astropy table with all relevant columns and add to SpectralTypes Table
SpT_table = Table([db_names, spectral_types, spectral_type_codes, regime, spt_refs], names=('source', 'spectral_type_string', 'spectral_type_code', 'regime', 'reference'))


db.add_table_data(SpT_table, table='SpectralTypes', fmt='astropy')

# WRITE THE JSON FILES
if SAVE_DB:
    db.save_database(directory='data/')
