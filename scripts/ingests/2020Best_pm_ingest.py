import sys
sys.path.append('.')
from astrodbkit2.astrodb import create_database
from astrodbkit2.astrodb import Database
from simple.schema import *
from astropy.table import Table
import numpy as np
from scripts.ingests.utils import ingest_proper_motions
from astropy.coordinates import SkyCoord
import astropy.units as u
from astroquery.simbad import Simbad
import warnings
warnings.filterwarnings("ignore", module='astroquery.simbad')
import re
import os
from pathlib import Path


SAVE_DB = False  # save the data files in addition to modifying the .db file
RECREATE_DB = True  # recreates the .db file from the data files
VERBOSE = False

verboseprint = print if VERBOSE else lambda *a, **k: None

def load_db():
    # Utility function to load the database

    db_file = 'SIMPLE.db'
    db_file_path = Path(db_file)
    db_connection_string = 'sqlite:///SIMPLE.db'  # SQLite browser

    if RECREATE_DB and db_file_path.exists():
        os.remove(db_file)  # removes the current .db file if one already exists

    if not db_file_path.exists():
        create_database(db_connection_string)  # creates empty database based on the simple schema
        db = Database(db_connection_string)  # connects to the empty database
        db.load_database('data/')  # loads the data from the data files into the database
    else:
        db = Database(db_connection_string)  # if database already exists, connects to .db file

    return db

db = load_db()

# load table
ingest_table = Table.read('scripts/ingests/UltracoolSheet-Main.csv', data_start=1)

#Defining variables 
sources = ingest_table['name']
ra_lit = ingest_table['pmra_lit']
ra_lit_err = ingest_table['pmraerr_lit']
dec_lit = ingest_table['pmdec_lit']
dec_lit_err = ingest_table['pmdecerr_lit']
ref_pm_lit = ingest_table['ref_pm_lit']
ra_UKIRT = ingest_table['pmra_UKIRT']
ra_UKIRT_err = ingest_table['pmraerr_UKIRT']
dec_UKIRT = ingest_table['pmdec_UKIRT']
dec_UKIRT_err = ingest_table['pmdecerr_UKIRT']
#ref_pm_UKIRT = ingest_table[] No table for UKIRT ref 


#Ingesting lit pm into db
ingest_proper_motions(db, sources, ra_lit, ra_lit_err, dec_lit, dec_lit_err, ref_pm_lit, save_db=False, verbose=False)

#Ingesting UKIRT pm into db
ingest_proper_motions(db, sources, ra_UKIRT, ra_UKIRT_err, dec_UKIRT, dec_UKIRT_err, save_db=False, verbose=False )

