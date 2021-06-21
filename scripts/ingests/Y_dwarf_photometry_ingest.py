import sys
sys.path.append('.')
from astrodbkit2.astrodb import create_database
from astrodbkit2.astrodb import Database
from simple.schema import *
#from utils import ingest_photometry
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

#def ingest_photometry adapted from ingest_pm and ingest_parallaxes from utils 
#NOTE: Have to eventually add back in epoch, ucd, and instrument below
def ingest_photometry(db, sources, band, ch1mag, ch1mag_err, ch2mag, ch2mag_err, telescope, reference, verbose=False):
    verboseprint = print if verbose else lambda *a, **k: None

    n_added = 0

    for i, source in enumerate(sources):
        db_name = db.search_object(source, output_table='Sources')[0]['source']
        # Search for existing proper motion data and determine if this is the best
        adopted = None
        source_photometry_data = db.query(db.Photometry).filter(db.Photometry.c.source == db_name).table()
        if source_photometry_data is None or len(source_photometry_data) == 0:
            adopted = True
        else:
            print("OTHER PHOTOMETRY MOTION EXISTS: ",source_photometry_data)

        # TODO: Work out logic for updating/setting adopted. Be it's own function.

        # TODO: Make function which validates refs

        # Construct data to be added
        photometry_data = [{'source': db_name,
                          'band': band[i],
                          #'ucd' : ucd[i],
                          'ch1mag' : ch1mag[i],
                          'ch1mag_err' : ch1mag_err[i],
                          'ch2mag': ch2mag[i],
                          'ch2mag_err': ch2mag_err[i],
                          'telescope': telescope[i],
                          #'instrument': instrument[i],
                          #'epoch': epoch[i], 
                          'adopted': adopted,
                          'reference': reference[i]}]
        verboseprint('Photometry data: ',photometry_data)

        # Consider making this optional or a key to only view the output but not do the operation.
        db.Photometry.insert().execute(photometry_data)
        n_added += 1

    print("Added to database: ", n_added)





#From ingest_ATLAS_spectral_types.py file
DRY_RUN = True #modifies .db file but not the data files
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

source = ingest_table['source']
band = ingest_table['J_MKO']
#Don't know what ucd is or if it is needed
#ucd = ingest_table['ucd']
ch1mag = ingest_table['ch1_mag']
ch1mag_err = ingest_table['ch1_err']
ch2mag = ingest_table['ch2_mag']
ch2mag_err = ingest_table['ch2_err']
telescope = ['Spizter',]*len(band)
#Have to look up what instrument is in table
#instrument = ingest_table['instrument']
#Leaving epoch blank for now since we don't know if the epoch table in the csv files corresponds to pm epoch or photometry
#epoch = ingest_table['epoch']
reference = ingest_table['Spizter_ref']

#ingest_photometry defintion in utils.py, adapted from ingest_parallaxes definition
#NOTE: have to eventually add back in ucd, instrument, and epoch
ingest_photometry(db, source, band, ch1mag, ch1mag_err, ch2mag, ch2mag_err, telescope, reference, verbose=True)


if not DRY_RUN:
    db.save_db('data') #edits the JSON files if we're not doing a dry run