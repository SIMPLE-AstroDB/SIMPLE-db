from astropy.io import ascii
from utils import *
from ingest_utils import *

SAVE_DB = False  # save the data files in addition to modifying the .db file
RECREATE_DB = True  # recreates the .db file from the data files
db = load_simpledb('SIMPLE.db', recreatedb=RECREATE_DB)

logger.setLevel(logging.INFO)


legg21_table = ascii.read('scripts/ingests/apjac0cfet10_mrt.txt', format='mrt')

legg21_table.write('scripts/ingests/legg21.csv', overwrite=True, format='ascii.csv')

#  Check if all sources are in the database
legg21_sources = legg21_table['Survey', 'RA', 'Decl.', 'DisRef']
for source in legg21_sources:
    source_string = f"{source['Survey']} {source['RA']}{source['Decl.']}"
    print(source_string)
    # convert ra and dec to decimal string
    # convert reference names
    ingest_sources(db, source_string, references=source['DisRef'])