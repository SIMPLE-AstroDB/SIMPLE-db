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
import pandas as pd


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
#ra_UKIRT = ingest_table['pmra_UKIRT']
#ra_UKIRT_err = ingest_table['pmraerr_UKIRT']
#dec_UKIRT = ingest_table['pmdec_UKIRT']
#dec_UKIRT_err = ingest_table['pmdecerr_UKIRT']
#ref_pm_UKIRT = ingest_table['ref_plx_UKIRT']

#ingest_table_df = pd.DataFrame({'sources': sources, 'pm_ra' : ra_UKIRT, 'pm_ra_err' : ra_UKIRT_err, 'pm_dec' : dec_UKIRT, 'pm_dec_err' : dec_UKIRT_err, 'pm_ref' : ref_pm_UKIRT})



df = pd.read_csv('scripts/ingests/UltracoolSheet-Main.csv', usecols=['name' ,'pmra_lit', 'pmraerr_lit', 'pmdec_lit', 'pmdecerr_lit', 'ref_pm_lit']) .dropna()
df.reset_index(inplace=True, drop=True)

for i, ref in enumerate(df.ref_pm_lit):
    if ref == 'Dupu12':
        df.ref_pm_lit[i] = 'Dupu12a'
    if ref == 'VanL07':
        df.ref_pm_lit[i] = 'vanL07'
    if ref == 'Kend07a':
        df.ref_pm_lit[i] = 'Kend07'
    if ref == 'Lepi02b':
        df.ref_pm_lit[i] = 'Lepi02'
    if ref == "Mace13a":
        df.ref_pm_lit[i] = "Mace13"
    if ref == 'Kend03a':
        df.ref_pm_lit[i] = "Kend03"
    if ref == 'West08a':
        df.ref_pm_lit[i] = "West08"
    if ref == 'Reid05b':
        df.ref_pm_lit[i] = "Reid05"
    if ref == 'Burg08b':
        df.ref_pm_lit[i] = "Burg08c"
    if ref == 'Burg08c':
        df.ref_pm_lit[i] = "Burg08d"
    if ref == 'Burg08d':
        df.ref_pm_lit[i] = "Burg08b"
    if ref == 'Gagn15b':
        df.ref_pm_lit[i] = "Gagn15c"
    if ref == 'Gagn15c':
        df.ref_pm_lit[i] = "Gagn15b"
    if ref == 'Lodi07a':
        df.ref_pm_lit[i] = "Lodi07b"
    if ref == 'Lodi07b':
        df.ref_pm_lit[i] = "Lodi07a"
    if ref == 'Reid02c':
        df.ref_pm_lit[i] = "Reid02b"
    if ref == 'Reid06a':
        df.ref_pm_lit[i] = "Reid06b"
    if ref == 'Reid06b':
        df.ref_pm_lit[i] = "Reid06a"
    if ref == 'Scho04b':
        df.ref_pm_lit[i] = "Scho04a"
    if ref == 'Scho10a':
        df.ref_pm_lit[i] = "Scho10b"
    if ref == 'Tinn93b':
        df.ref_pm_lit[i] = "Tinn93c"
    if ref == 'Lieb79f':
        df.ref_pm_lit[i] = "Lieb79"
    if ref == 'Prob83c':
        df.ref_pm_lit[i] = "Prob83"
    if ref == 'Jame08a':
        df.ref_pm_lit[i] = 'Jame08'
print(df)





#Ingesting lit pm into db
#ingest_proper_motions(db, sources, ra_lit, ra_lit_err, dec_lit, dec_lit_err, ref_pm_lit, save_db=False, verbose=False)

#Ingesting UKIRT pm into db
ingest_proper_motions(db, df.name, df.pmra_lit, df.pmraerr_lit, df.pmdec_lit, df.pmdecerr_lit, df.ref_pm_lit, save_db=False, verbose=False )

