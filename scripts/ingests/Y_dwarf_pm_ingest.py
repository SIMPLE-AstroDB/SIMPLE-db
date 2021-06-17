#Adapted from Clemence Fontanive's Y-dwarf-astrometry-ingest.py code

#imports from y-dwarf-astrometry-ingest.py, utils.py, ingest_ATLAS_spectral_types.py
import sys
sys.path.append('.')
from astrodbkit2.astrodb import create_database
from astrodbkit2.astrodb import Database
from simple.schema import *
from utils import ingest_pm
from astropy.table import Table
import numpy as np
from astropy.coordinates import SkyCoord
import astropy.units as u
from astroquery.simbad import Simbad
import warnings
warnings.filterwarnings("ignore", module='astroquery.simbad')
import re
import os
from pathlib import Path


#From ingest_ATLAS_spectral_types.py file
DRY_RUN = False #modifies .db file but not the data files
RECREATE_DB = True #recreates the .db file from the data files
VERBOSE = False

verboseprint = print if VERBOSE else lambda *a, **k: None

db_file = 'SIMPLE.db'
db_file_path = Path(db_file)
db_connection_string = 'sqlite:///SIMPLE.db'  # SQLite browser

if RECREATE_DB and db_file_path.exists():
    os.remove(db_file) #removes the current .db file if one already exists

if not db_file_path.exists():
    create_database(db_connection_string) #creates empty database based on the simple schema
    db = Database(db_connection_string) #connects to the empty database
    db.load_database('data/') #loads the data from the data files into the database
else:
    db = Database(db_connection_string) #if database already exists, connects to .db file


# load table
ingest_table = Table.read('scripts/ingests/Y-dwarf_table.csv', data_start=2)

sources = ingest_table['source']
muRA = ingest_table['muRA_masyr']
muRA_err = ingest_table['muRA_err']
muDEC = ingest_table['muDEC_masyr']
muDEC_err = ingest_table['muDEC_err']
pm_reference = ['Kirk19',]*len(muRA)

#ingest_pm defintion in utils.py, adapted from ingest_parallaxes definition
ingest_pm(db, sources, muRA, muRA_err, muDEC, muDEC_err, pm_reference, verbose=True)


if not DRY_RUN:
    db.save_db('data') #edits the JSON files if we're not doing a dry run
