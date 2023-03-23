from astrodbkit2.astrodb import create_database
from astrodbkit2.astrodb import Database
from astropy.table import Table

SAVE_DB = True  # save the data files in addition to modifying the .db file
RECREATE_DB = True  # recreates the .db file from the data files
VERBOSE = False
# LOAD THE DATABASE
db = load_simpledb('SIMPLE.db', recreatedb=RECREATE_DB)

filip_table = Table.read("Fili15_table9.csv", data_start=1)
