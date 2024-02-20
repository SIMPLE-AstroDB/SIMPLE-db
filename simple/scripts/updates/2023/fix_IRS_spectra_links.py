from scripts.ingests.utils import load_simpledb
from astropy.table import Table


SAVE_DB = True  # save the data files in addition to modifying the .db file
RECREATE_DB = True  # recreates the .db file from the data files

# LOAD THE DATABASE
db = load_simpledb('SIMPLE.db', recreatedb=RECREATE_DB)


#  link to live google sheet
link = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vQG5cGkI2aHPHD4b6ZZPTU4jjQMirU_z-yhl5ElI3p6nCIufIL64crC-yFalF58OauWHxmYvEKR_isY/pub?gid=0&single=true&output=csv'
columns = ['source', 'original_spectrum', 'fixed_spectrum']
spectra_link_table = Table.read(link, format='ascii', data_start=2, names=columns, guess=False, fast_reader=False, delimiter=',')

for row in spectra_link_table:
    # t = db.query(db.Spectra).filter(db.Spectra.c.original_spectrum == row['original_spectrum']).astropy()
    # print(t['spectrum'])
    with db.engine.connect() as conn:
        conn.execute(db.Spectra.update().where(db.Spectra.c.original_spectrum == row['original_spectrum']).values(spectrum=row['fixed_spectrum']))
        conn.commit()

# WRITE THE JSON FILES
if SAVE_DB:
    db.save_database(directory='data/')