import logging

import pandas as pd
from scripts.ingests.ingest_utils import ingest_parallaxes
from scripts.ingests.utils import *

SAVE_DB = False  # save the data files in addition to modifying the .db file
RECREATE_DB = True  # recreates the .db file from the data files

#alternate between info and debug
logger.setLevel(logging.INFO)

db = load_simpledb('SIMPLE.db', recreatedb=RECREATE_DB)


df = pd.read_csv('scripts/ingests/UltracoolSheet-Main.csv', usecols=['name','plx_UKIRT', 'plxerr_UKIRT','ref_plx_UKIRT']).dropna()
df.reset_index(inplace=True, drop=True)
print(df)
# if we wanted to use an astropy tables method instead,
# we could use this to create a new table without masked plx_UKIRT values
# see https://community.openastronomy.org/t/extract-unmasked-elements-from-astropy-table/145/6?u=kelle
# data = Table.read('scripts/ingests/UltracoolSheet-Main.csv')
# UKIRT_plx_data = data[~data['plx_UKIRT'].mask]

#Use ingest_parallax function
ingest_parallaxes(db, df.name, df.plx_UKIRT, df.plxerr_UKIRT, df.ref_plx_UKIRT)

# WRITE THE JSON FILEs
if SAVE_DB:
    db.save_database(directory='data/')