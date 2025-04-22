from astrodb_utils import load_astrodb
from astrodb_utils import ingest_publication
import sys
sys.path.append(".")
from simple.schema import *
from simple.schema import REFERENCE_TABLES
from astropy.io import ascii
from scripts.utils.ingest_spectra_utils import ingest_spectrum

SAVE_DB = True  # save the data files in addition to modifying the .db file
RECREATE_DB = True  # recreates the .db file from the data files
# LOAD THE DATABASE
db = load_astrodb("SIMPLE.sqlite", recreatedb=RECREATE_DB, reference_tables=REFERENCE_TABLES)
#ingest_publication(db, doi='10.3847/1538-3881/ad324e' )
# Load Google sheet

link = 'Spectra.csv'
#Link to the CSV file used: https://docs.google.com/spreadsheets/d/1JFa8F4Ngzp3qAW8NOBurkz4bMKo9zXYeF6N1vMtqDZs/edit?usp=sharing

byw_table = ascii.read(
    link, #change this to the path for the csv file
    format="csv",
    data_start=1,
    header_start=0,
    guess=False,
    fast_reader=False,
    delimiter=",",
)

for row in byw_table:  # skip the header row - [1:10]runs only first 10 rows
    
    ingest_spectrum(
        db,
        source=row["Source"],
        spectrum=row["Spectrum"],
        regime=row["Regime"],
        telescope=row["Telescope"],
        instrument=row["Instrument"],
        reference=row["ref"]
        )
    

    # WRITE THE JSON FILES
if SAVE_DB:
    db.save_database(directory="data/")
