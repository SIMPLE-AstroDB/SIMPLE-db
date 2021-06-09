#Adapted from Clemence Fontanive's Y-dwarf-astrometry-ingest.py code

#imports from y-dwarf-astrometry-ingest.py, utils.py, ingest_ATLAS_spectral_types.py
from astrodbkit2.astrodb import create_database
from astrodbkit2.astrodb import Database
from astropy.table import Table
from astrodbkit2.schema_example import *
import numpy as np
from astropy.coordinates import SkyCoord
import astropy.units as u
from astroquery.simbad import Simbad
import warnings
warnings.filterwarnings("ignore", module='astroquery.simbad')
import re
import os
from utils import convert_spt_string_to_code
from pathlib import Path
from astrodbkit2.schema_example import *

'''from astrodbkit2.astrodb import create_database
from astrodbkit2.astrodb import Database
import sys
sys.path.append('.')
from simple.schema import *
from astropy.table import Table
import numpy as np
import re
import os
from utils import convert_spt_string_to_code
from pathlib import Path'''

#ingest_pm definition adapted from utils.py ingest_parallaxes definition
def ingest_pm(db, sources, muRA, muRA_err, muDEC, muDEC_err, verbose=False, norun=False):
    """

    TODO: do stuff about adopted in cases of multiple measurements.

    :param db:
    :param sources:
    :param plx:
    :param plx_unc:
    :param plx_ref:
    :param verbose:
    :param norun:
    :return:
    """
    verboseprint = print if verbose else lambda *a, **k: None

    n_added = 0

    for i, source in enumerate(sources):
        db_name = db.search_object(source, output_table='Sources')[0]

        # Search for existing proper motion data and determine if this is the best
        adopted = None
        source_pm_data = db.query(db.Pm).filter(db.Pm.c.source == db_name).table()
        if source_pm_data is None or len(source_pm_data) == 0:
            adopted = True
        else:
            print("OTHER PROPER MOTION EXISTS")
            print(source_pm_data)

        # TODO: Work out logic for updating/setting adopted. Be it's own function.

        # TODO: Make function which validates refs

        # Construct data to be added
        pm_data = [{'source': db_name,
                          'Right Ascension': muRA[i],
                          'Right Ascension Error' : muRA_err[i],
                          'Declination': muDEC[i],
                          'Declination Error': muDEC_err[i],
                          'adopted': adopted}]
        verboseprint(pm_data)

        # Consider making this optional or a key to only view the output but not do the operation.
        if not norun:
            db.Pm.insert().execute(pm_data)
            n_added += 1

    print("Added to database: ", n_added)
#end of ingest_pm def from utils.py


DRY_RUN = True
RECREATE_DB = True
VERBOSE = False

verboseprint = print if VERBOSE else lambda *a, **k: None

db_file = 'SIMPLE.db'
db_file_path = Path(db_file)
db_connection_string = 'sqlite:///SIMPLE.db'  # SQLite

if RECREATE_DB and db_file_path.exists():
    os.remove(db_file)

if not db_file_path.exists():
    create_database(db_connection_string)
    db = Database(db_connection_string)
    #db.load_database('data')

# load table
ingest_table = Table.read('scripts/ingests/Y-dwarf_table.csv', data_start=2)
sources = ingest_table['source']
muRA = ingest_table['muRA_masyr']
muRA_err = ingest_table['muRA_err']
muDEC = ingest_table['muDEC_masyr']
muDEC_err = ingest_table['muDEC_err']

ingest_pm(db, sources, muRA, muRA_err, muDEC, muDEC_err, verbose=True)


if not DRY_RUN:
    db.save_db('data')
