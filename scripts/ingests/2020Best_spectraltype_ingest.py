import logging
from astropy.table import Table, Column
from scripts.ingests.ingest_utils import ingest_spectral_types, ingest_sources
from scripts.ingests.utils import find_publication, load_simpledb, logger

SAVE_DB = False  # save the data files in addition to modifying the .db file
RECREATE_DB = True  # recreates the .db file from the data files

logger.setLevel(logging.WARNING)
# logger.setLevel(logging.DEBUG)

db = load_simpledb('SIMPLE.db', recreatedb=RECREATE_DB)

# Read in CSV file as Astropy table
file = 'UltracoolSheet-Main.csv'
data = Table.read('scripts/ingests/' + file)

# Optical Spectral Types
filter_opt_data = data[data['spt_opt'] != 'null']
opt_names = filter_opt_data['name']
n_sources_opt = len(opt_names)
opt_regime = ['optical'] * n_sources_opt
spt_refs_opt = filter_opt_data['ref_spt_opt']
spectral_types_opt = filter_opt_data['spt_opt']

# IR Spectral Types
filter_ir_data = data[data['spt_ir'] != 'null']
ir_names = filter_ir_data['name']
n_sources_ir = len(ir_names)
ir_regime = Column(['nir_UCD'] * n_sources_ir)
spt_refs_ir = filter_ir_data['ref_spt_ir']
spectral_types_ir = filter_ir_data['spt_ir']

# Reference information
ref_file = 'UltracoolSheet-References.csv'
ref_data = Table.read('scripts/ingests/' + ref_file)

# Check references against database and ensure they have the correct names
publication_transform = {}
for pub in set(spt_refs_opt.tolist() + spt_refs_ir.tolist() + ['Kuzu11', 'Bowl14', 'Schn15', 'Cush16']):
    # Reject invalid references
    if ';' in pub:
        print(f'Invalid reference name: {pub}; skipping')
        continue
    # Match against reference information
    ind = ref_data['code_ref'] == pub
    if ind.sum() != 1:
        print(f'Reference {pub} not matched against input list; skipping')
        continue
    bibcode = ref_data[ind]['ADSkey_ref'][0]

    check, counts = find_publication(db, bibcode=bibcode)
    if not check:
        print(f'Not matched: {pub} ({bibcode}) not in DB. {counts} matches were returned.')
    else:
        # Check name is consistent between DB and input file
        db_result = db.query(db.Publications.c.publication).filter(db.Publications.c.bibcode == bibcode).astropy()
        db_name = db_result['publication'][0]
        publication_transform[pub] = db_name
        if pub != db_name:
            print(f'{pub} does not match name: {db_name} in database for that bibcode')

# Convert publications to their proper names
# The split(';') forces the first publication in a list to be used
new_ref = list(map(lambda x: publication_transform.get(x.split(';')[0], 'Missing'), spt_refs_opt))
spt_refs_opt = Column(new_ref)
new_ref = list(map(lambda x: publication_transform.get(x.split(';')[0], 'Missing'), spt_refs_ir))
spt_refs_ir = Column(new_ref)

# Ingest missing sources
# ind = data['name'] == '2MASS J15470557-1626303B'  # Just to verify and get RA/Dec
ingest_sources(db, '2MASS J15470557-1626303B', references='Gagn15b', ras=[236.7734], decs=[-16.4419])

# Ingest Optical Spectral Types
ingest_spectral_types(db, opt_names, spectral_types_opt, spt_refs_opt, regimes=opt_regime, spectral_type_error=None)

# Ingest Infrared Spectral Types
# Removing some incorrect spectral types
ind = spectral_types_ir != 'extremely red'
ingest_spectral_types(db, ir_names[ind], spectral_types_ir[ind], spt_refs_ir[ind], regimes=ir_regime[ind],
                      spectral_type_error=None)

# # WRITE THE JSON FILES
# if SAVE_DB:
#     db.save_database(directory='data/')
