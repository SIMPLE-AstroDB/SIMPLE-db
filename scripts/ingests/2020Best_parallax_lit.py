import pandas as pd
from scripts.ingests.ingest_utils import ingest_parallaxes
from scripts.ingests.utils import *

SAVE_DB = False  # save the data files in addition to modifying the .db file
RECREATE_DB = True  # recreates the .db file from the data files

#alternate between info and debug
logger.setLevel(logging.DEBUG)

db = load_simpledb('SIMPLE.db', recreatedb=RECREATE_DB)

# Use Pandas to read in non-null rows.
df = pd.read_csv('scripts/ingests/Best2020-Truncated.csv', usecols=['name','plx_lit', 'plxerr_lit','ref_plx_lit']).dropna()
df.reset_index(inplace=True, drop=True)

print(df)


db.Publications.update().where(db.Publications.c.publication == 'Dupu12a').\
    values(publication='Dupu12').execute()
# Fix some references
'''for i, ref in enumerate(df.ref_plx_lit):
    if ref == 'Burg08b':
        df.ref_plx_lit[i] = "Burg08c"
    if ref == 'Burg08c':
        df.ref_plx_lit[i] = "Burg08d"
    if ref == 'Burg08d':
        df.ref_plx_lit[i] = "Burg08b"
    if ref == 'Gagn15b':
        df.ref_plx_lit[i] = "Gagn15c"
    if ref == 'Gagn15c':
        df.ref_plx_lit[i] = "Gagn15b"
    if ref == 'Lodi07a':
        df.ref_plx_lit[i] = "Lodi07b"
    if ref == 'Lodi07b':
        df.ref_plx_lit[i] = "Lodi07a"
    if ref == 'Reid02c':
        df.ref_plx_lit[i] = "Reid02b"
    if ref == 'Reid06a':
        df.ref_plx_lit[i] = "Reid06b"
    if ref == 'Reid06b':
        df.ref_plx_lit[i] = "Reid06a"
    if ref == 'Scho04b':
        df.ref_plx_lit[i] = "Scho04a"
    if ref == 'Scho10a':
        df.ref_plx_lit[i] = "Scho10b"
    if ref == 'Tinn93b':
        df.ref_plx_lit[i] = "Tinn93c"
    if ref == 'Lieb79f':
        df.ref_plx_lit[i] = "Lieb79"
    if ref == 'Prob83c':
        df.ref_plx_lit[i] = "Prob83"
    if ref == 'Jame08a':
        df.ref_plx_lit[i] = 'Jame08'
    if ref == 'Lepi05a':
        df.ref_plx_lit[i] = 'Lepi05'
    if ref == 'Lodi05b':
        df.ref_plx_lit[i] = 'Lodi05'
    if ref == 'Tinn95c':
        df.ref_plx_lit[i] = 'Tinn95'
    if ref == 'Roes10b':
        df.ref_plx_lit[i] = 'Roes10'
    if ref == 'Hog_00a':
        df.ref_plx_lit[i] = 'Hog_00'
    if ref == 'Ditt14a':
        df.ref_plx_lit[i] = 'Ditt14'
    if ref == 'Schn16b':
        df.ref_plx_lit[i] = 'Schn16'
    if ref == 'Tinn03b':
        df.ref_plx_lit[i] = 'Tinn03'
    if ref == 'Phan08a':
        df.ref_plx_lit[i] = 'Phan08'
    if ref == 'Gizi15a':
        df.ref_plx_lit[i] = 'Gizi15'
        '''

#print(df)

# if we wanted to use an astropy tables method instead,
# we could use this to create a new table without masked plx_UKIRT values
# see https://community.openastronomy.org/t/extract-unmasked-elements-from-astropy-table/145/6?u=kelle
# data = Table.read('scripts/ingests/UltracoolSheet-Main.csv')
# UKIRT_plx_data = data[~data['plx_UKIRT'].mask]

#Use ingest_parallax function
#ingest_parallaxes(db, df.name, df.plx_lit, df.plxerr_lit, df.ref_plx_lit)

# WRITE THE JSON FILEs
#if SAVE_DB:
   # db.save_database(directory='data/')

