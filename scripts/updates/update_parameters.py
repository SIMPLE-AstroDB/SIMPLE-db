"""
Script to update the Parameters table
"""

import logging
from astropy.table import Table
from scripts import REFERENCE_TABLES
from scripts.ingests.utils import logger, load_simpledb

SAVE_DB = True  # save the data files in addition to modifying the .db file
RECREATE_DB = True  # recreates the .db file from the data files

logger.setLevel(logging.INFO)

db = load_simpledb('SIMPLE.db', recreatedb=RECREATE_DB)

# mass, radius, log_g, T_eff, metallicity, C-to-O ratio
parameter_data = [
    {'parameter': 'mass', 'description': 'Mass'},
    {'parameter': 'radius', 'description': 'Radius'},
    {'parameter': 'log g', 'description': 'Log surface gravity'},
    {'parameter': 'T eff', 'description': 'Effective temperature in K'},
    {'parameter': 'metallicity', 'description': 'Metallicity'},
    {'parameter': 'C/O ratio', 'description': 'C/O ratio'},
]

existing_data = db.query(db.Parameters).table()

# Loop over the parameters, inserting those missing
for p in parameter_data:
    if len(existing_data) > 0 and p['parameter'] in existing_data['parameter']:
        print(f'Already in database, skipping {p}')
        continue

    db.Parameters.insert([p]).execute()

# Save the database
if SAVE_DB:
    db.save_database(directory='data/')
