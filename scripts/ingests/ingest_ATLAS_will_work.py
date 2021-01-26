# Script to ingest Cloud Atlas Manjavacas+2019
# Using Clémence Fontanive's previous code as guide

#------------------------------------------------------------------------------------------------

from astrodbkit2.astrodb import create_database
from astrodbkit2.astrodb import Database
#from simple.schema import *
from astropy.table import Table
connection_string = 'sqlite:///../../SIMPLE.db'  # SQLite
create_database(connection_string)
db = Database(connection_string)
db.load_database('../../data')
import numpy as np
# load table of sources to ingest

input_file = ("ATLAS_table.vot")
ATLAS=Table.read(input_file)
