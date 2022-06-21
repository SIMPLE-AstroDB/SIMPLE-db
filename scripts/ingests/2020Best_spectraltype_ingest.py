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

# Optical Spectral Types
filter_opt_data = data[data['spt_opt'] != 'null']
opt_names = filter_opt_data['name']
n_sources = len(opt_names)
opt_regime = ['optical'] * n_sources
spt_refs_opt = filter_opt_data['ref_spt_opt']
spectral_types_opt = filter_opt_data['spt_opt']
# IR Spectral Types
filter_ir_data = data[data['spt_ir'] != 'null']
ir_names = filter_ir_data['name']
n_sources = len(ir_names)
ir_regime = ['infrared'] * n_sources
spt_refs_ir = filter_opt_data['ref_spt_ir']
spectral_types_ir = filter_opt_data['spt_ir']


def ingest_source_pub():
    ingest_sources(db, 'BRI 0021-0214', 'Irwi91')
    ingest_sources(db, 'LHS 1070', 'McCa64c')
    ingest_sources(db, 'WISE J003110.04+574936.3', 'Thom13')
    ingest_sources(db, 'DENIS-P J0041353-562112', 'Phan01')
    ingest_sources(db, 'DENIS J020529.0-115925', 'Delf97')
    ingest_sources(db, 'LP 261-75B', 'Kirk00')
    ingest_sources(db, 'LHS 292', 'Dahn86')


ingest_source_pub()

# Source names in database Names table
db_opt_names = []
db_ir_names = []
for name in opt_names:
    db_opt_name = db.search_object(name, output_table='Sources', table_names={'Names': ['other_name']})[0]
    db_opt_names.append(db_opt_name['source'])

for name in ir_names:
    db_ir_name = db.search_object(name, output_table='Sources', table_names={'Names': ['other_name']})[0]
    db_ir_names.append(db_ir_name['source'])

# # Convert SpT string to code
spectral_type_codes_opt = convert_spt_string_to_code(spectral_types_opt)
spectral_type_codes_ir = convert_spt_string_to_code(spectral_types_ir)

# Adding References for Optical
ref_list_opt = spt_refs_opt.tolist()
included_ref_opt = db.query(db.Publications.c.publication).filter(db.Publications.c.publication.in_(ref_list_opt)).all()
included_ref_opt = [s[0] for s in included_ref_opt]
new_ref_opt = list(set(ref_list_opt) - set(included_ref_opt))
new_ref_opt = [{'publication': s} for s in new_ref_opt]
if len(new_ref_opt) > 0:
    db.Publications.insert().execute(new_ref_opt)

# Adding References for Infrared
ref_list_ir = spt_refs_ir.tolist()
included_ref_ir = db.query(db.Publications.c.publication).filter(db.Publications.c.publication.in_(ref_list_ir)).all()
included_ref_ir = [s[0] for s in included_ref_ir]
new_ref_ir = list(set(ref_list_ir) - set(included_ref_ir))
new_ref_ir = [{'publication': s} for s in new_ref_ir]
if len(new_ref_ir) > 0:
    db.Publications.insert().execute(new_ref_ir)
# # Make astropy table with all relevant columns and add to SpectralTypes Table
SpT_Opt_table = Table([db_opt_names, spectral_types_opt, spectral_type_codes_opt, opt_regime, spt_refs_opt],
                      names=('source', 'spectral_type_string', 'spectral_type_code', 'regime', 'reference'))
SpT_Ir_table = Table([db_ir_names, spectral_types_ir, spectral_type_codes_ir, ir_regime, spt_refs_ir],
                     names=('source', 'spectral_type_string', 'spectral_type_code', 'regime', 'reference'))

db.add_table_data(SpT_Opt_table, table='SpectralTypes', fmt='astropy')
db.add_table_data(SpT_Ir_table, table='SpectralTypes', fmt='astropy')
# # WRITE THE JSON FILES
# if SAVE_DB:
#     db.save_database(directory='data/')
