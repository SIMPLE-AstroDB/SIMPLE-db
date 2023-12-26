from scripts.ingests.ingest_utils import *
from scripts.ingests.utils import *
from astropy.table import Table
import astropy.units as u
from astropy.coordinates import Angle
   

SAVE_DB = False  # True: save the data files(json) in addition to modifying the .db file
RECREATE_DB = True  # recreates the .db file from the data files
# LOAD THE DATABASE
db = load_simpledb('SIMPLE.db', recreatedb=RECREATE_DB)

# SPECTRAL TYPES
ingest_spectral_types(db, 
                          sources = " ", 
                          spectral_types= " ", 
                          references = " ", 
                          regimes = " ", 
                          spectral_type_error=None,
                          comments=None)