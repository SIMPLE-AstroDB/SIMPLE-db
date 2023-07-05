# script ingest first data into companion tables

from scripts.ingests.ingest_utils import *
from scripts.ingests.utils import *
from astropy.table import Table
import astropy.units as u
from astropy.coordinates import Angle

SAVE_DB = False  # save the data files in addition to modifying the .db file
RECREATE_DB = True  # recreates the .db file from the data files
VERBOSE = False
# LOAD THE DATABASE
db = load_simpledb('SIMPLE.db', recreatedb=RECREATE_DB)

